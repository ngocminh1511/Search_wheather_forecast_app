import io
import os
import sys
import struct
import time
import math
import logging
import numpy as np
from PIL import Image
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
from typing import Dict, Any, Optional

_WIND_FIELD_ZSTD_CCTX = None  # lazy-initialized per process in _get_zstd_cctx()

# Windows holds an exclusive file handle on memory-mapped .npy files inside
# pool workers (workers are reused), which prevents the post-stage cleanup
# from deleting the canvas (WinError 32). Skip mmap on Windows so files can
# be removed when the cut/publish stages finish.
_USE_MMAP = not sys.platform.startswith("win")


def _np_load(path):
    """Load a .npy file. mmap on POSIX (zero-copy), full read on Windows."""
    if _USE_MMAP:
        return np.load(path, mmap_mode="r")
    return np.load(path)

from ..config import get_settings
from ..services.resource_guard import get_resource_metrics
from .metatile import Metatile, iter_metatiles, TILE_SIZE
from .chunk_format import ChunkWriter
from .colormap import apply_colormap

log = logging.getLogger(__name__)

# Stepped INTEGER colormaps must use NEAREST upscaling to prevent cross-type blending.
_NEAREST_CMAP_TYPES: frozenset[str] = frozenset({
    "precip_debug_ptype",
    "advanced_precipitation_base",
    "precip_base",
    "rain_advanced",
})

_CFG = get_settings()
_PNG_SAVE_KWARGS = {
    "format": "PNG",
    "optimize": _CFG.TILE_PNG_OPTIMIZE,
    "compress_level": _CFG.TILE_PNG_COMPRESS_LEVEL,
}
_BANDED_MAP_TYPES = {
    "precip_debug_ptype",
    "advanced_precipitation_base",
    "precip_base",
    "rain_advanced",
    "rain_basic",
    "snow_depth",
}


def _delta2d(arr: np.ndarray) -> np.ndarray:
    """2D prediction residual: horizontal diff then vertical diff of horizontal diff."""
    d = arr.astype(np.int16)
    dx = np.empty_like(d)
    dx[:, 0] = d[:, 0]            # first column raw (anchor)
    dx[:, 1:] = np.diff(d, axis=1)
    out = np.empty_like(dx)
    out[0, :] = dx[0, :]          # first row raw (anchor)
    out[1:, :] = np.diff(dx, axis=0)
    return np.clip(out, -128, 127).astype(np.int8)


def _get_zstd_cctx():
    global _WIND_FIELD_ZSTD_CCTX
    if _WIND_FIELD_ZSTD_CCTX is None:
        try:
            import zstandard as _zstd_mod
            _WIND_FIELD_ZSTD_CCTX = _zstd_mod.ZstdCompressor(level=19, threads=1)
        except ImportError:
            raise RuntimeError(
                "zstandard package not installed. Run: pip install zstandard"
            )
    return _WIND_FIELD_ZSTD_CCTX


def _wind_field_wfld(u_tile: np.ndarray, v_tile: np.ndarray) -> bytes:
    """Encode one wind_field tile: 8-byte WFLD header + Zstd(interleaved 2D-delta U,V)."""
    h, w = u_tile.shape
    raw = np.stack([_delta2d(u_tile), _delta2d(v_tile)], axis=-1).tobytes()
    return struct.pack('<4sHH', b'WFLD', w, h) + _get_zstd_cctx().compress(raw)


def _resolve_worker_count(workers: int) -> int:
    requested = workers or _CFG.TILE_PROCESS_WORKERS or _CFG.TILE_WORKERS or 1
    cpu_count = max(1, (os.cpu_count() or 1))
    max_allowed = min(requested, cpu_count)
    min_allowed = min(_CFG.TILE_MIN_PROCESS_WORKERS, max_allowed)
    safe_workers = max(min_allowed, max_allowed)

    if _CFG.TILE_ADAPTIVE_THROTTLE:
        metrics = get_resource_metrics()
        cpu = metrics.get("cpu_percent", 0.0)
        ram = metrics.get("ram_percent", 0.0)
        iowait = metrics.get("iowait_percent", 0.0)

        # If server is already under pressure, start with fewer workers.
        over_cpu = _CFG.MAX_CPU_PERCENT > 0 and cpu >= _CFG.MAX_CPU_PERCENT
        over_ram = _CFG.MAX_RAM_PERCENT > 0 and ram >= _CFG.MAX_RAM_PERCENT
        over_iowait = _CFG.MAX_IOWAIT_PERCENT > 0 and iowait >= _CFG.MAX_IOWAIT_PERCENT
        if over_cpu or over_ram or over_iowait:
            safe_workers = max(1, min_allowed)
            log.warning(
                "Adaptive throttle: cpu=%.1f%% ram=%.1f%% iowait=%.1f%% -> workers=%d",
                cpu, ram, iowait, safe_workers,
            )
        elif max_allowed >= 4:
            # Keep headroom under moderate load to avoid server stalls.
            near_cpu = _CFG.MAX_CPU_PERCENT > 0 and cpu >= (_CFG.MAX_CPU_PERCENT - 10.0)
            near_ram = _CFG.MAX_RAM_PERCENT > 0 and ram >= (_CFG.MAX_RAM_PERCENT - 10.0)
            near_iowait = _CFG.MAX_IOWAIT_PERCENT > 0 and iowait >= (_CFG.MAX_IOWAIT_PERCENT - 10.0)
            if near_cpu or near_ram or near_iowait:
                safe_workers = max(min_allowed, max_allowed - 1)
                log.info(
                    "Adaptive throttle (soft): cpu=%.1f%% ram=%.1f%% iowait=%.1f%% -> workers=%d",
                    cpu, ram, iowait, safe_workers,
                )

    return max(1, min(safe_workers, cpu_count))


def _aggregate_stats(res: Dict[str, Any], summary: Dict[str, Any], chunk_increment: int = 1) -> None:
    for k in ["total", "saved", "empty_skipped", "errors", "bytes", 
              "metatiles_total", "metatiles_empty_skipped", "tiles_empty_skipped_inside_nonempty",
              "bytes_before_compress", "bytes_after_compress"]:
        summary[k] = summary.get(k, 0) + res.get(k, 0)
        
    for k in ["metatile_extract_time_s", "colorize_time_s", "encode_time_s", "chunk_write_time_s"]:
        summary[k] = summary.get(k, 0.0) + res.get(k, 0.0)
        
    if res.get("chunk_path") or res.get("saved", 0) > 0:
        summary["chunks_written"] = summary.get("chunks_written", 0) + chunk_increment

def _drain_futures(futures, summary: Dict[str, Any], chunk_increment: int = 1) -> None:
    done = [f for f in futures if f.done()]
    for fut in done:
        futures.remove(fut)
        try:
            _aggregate_stats(fut.result(), summary, chunk_increment)
        except Exception as exc:
            log.error(f"Worker crashed: {exc}")

def _wait_and_drain_one(futures, summary: Dict[str, Any], chunk_increment: int = 1) -> None:
    if not futures:
        return
    done, _ = wait(set(futures), return_when=FIRST_COMPLETED)
    for fut in done:
        if fut in futures:
            futures.remove(fut)
        try:
            _aggregate_stats(fut.result(), summary, chunk_increment)
        except Exception as exc:
            log.error(f"Worker crashed: {exc}")


def _should_skip_chunk(output_dir: Path, z: int, cx: int, cy: int, *products: str) -> bool:
    if not _CFG.TILE_SKIP_EXISTING_CHUNKS:
        return False
    if not products:
        return (output_dir / str(z) / f"{cx}_{cy}.chunk").exists()
    return all(
        (output_dir / product / str(z) / f"{cx}_{cy}.chunk").exists()
        for product in products
    )


def _quantize_if_needed(img: Image.Image) -> Image.Image:
    try:
        return img.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    except Exception:
        return img


def _encode_tile_image(img: Image.Image, map_type: str) -> bytes:
    fmt = _CFG.TILE_FORMAT_DEFAULT
    if _CFG.TILE_USE_PNG8_FOR_BANDED and map_type in _BANDED_MAP_TYPES:
        fmt = "png8"
    buf = io.BytesIO()
    if fmt == "webp":
        rgba = img.convert("RGBA")
        rgba.save(
            buf,
            format="WEBP",
            quality=_CFG.TILE_WEBP_QUALITY,
            method=4,
            lossless=False,
        )
        return buf.getvalue()
    if fmt == "png8":
        img = _quantize_if_needed(img.convert("RGBA"))
    img.save(buf, **_PNG_SAVE_KWARGS)
    return buf.getvalue()

def process_wind_metatile_worker(
    npy_u_path: str,
    npy_v_path: str,
    npy_speed_path: str,
    px_per_meter: float,
    z: int,
    cx: int,
    cy: int,
    output_dir: str,
    base_only: bool = False,
    field_only: bool = False,
) -> Dict[str, Any]:
    """Worker for wind: generates wind_base (PNG) and wind_field (WFLD: 2D-delta + Zstd u/v) chunks."""
    start_t = time.perf_counter()
    stats = {
        "total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "bytes": 0,
        "metatile_extract_time_s": 0.0, "colorize_time_s": 0.0,
        "encode_time_s": 0.0, "chunk_write_time_s": 0.0,
        "bytes_before_compress": 0, "bytes_after_compress": 0,
        "metatiles_total": 1, "metatiles_empty_skipped": 0,
        "tiles_empty_skipped_inside_nonempty": 0,
    }
    metatile = Metatile(z, cx, cy)

    try:
        merc_u = _np_load(npy_u_path)
        merc_v = _np_load(npy_v_path)
        merc_speed = _np_load(npy_speed_path)
        canvas_size = merc_u.shape[0]
        x0, y0, x1, y1 = metatile.get_master_canvas_slice(canvas_size, px_per_meter)
        
        if x1 <= x0 or y1 <= y0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["total"] += metatile.tiles_x * metatile.tiles_y
            stats["metatiles_empty_skipped"] = 1
            stats["metatile_extract_time_s"] = time.perf_counter() - start_t
            return stats
            
        mt_u = merc_u[y0:y1, x0:x1]
        mt_v = merc_v[y0:y1, x0:x1]
        mt_speed = merc_speed[y0:y1, x0:x1]
        
        if mt_u.size == 0 or (~np.isfinite(mt_u)).all():
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["total"] += metatile.tiles_x * metatile.tiles_y
            stats["metatiles_empty_skipped"] = 1
            stats["metatile_extract_time_s"] = time.perf_counter() - start_t
            return stats
            
        target_w = metatile.pixel_width
        target_h = metatile.pixel_height
        h_s, w_s = mt_u.shape
        
        def _upsample(arr):
            nan_mask = ~np.isfinite(arr)
            if nan_mask.all():
                return np.full((target_h, target_w), np.nan, dtype=np.float32)
            filled = np.where(nan_mask, 0.0, arr).astype(np.float32)
            img = Image.fromarray(filled, mode="F").resize((target_w, target_h), Image.Resampling.BILINEAR)
            up_arr = np.array(img, dtype=np.float32)
            valid_img = Image.fromarray((~nan_mask).astype(np.float32), mode="F").resize((target_w, target_h), Image.Resampling.BILINEAR)
            up_arr[np.array(valid_img) < 0.01] = np.nan
            return up_arr
            
        if h_s != target_h or w_s != target_w:
            up_u = _upsample(mt_u)
            up_v = _upsample(mt_v)
            up_speed = _upsample(mt_speed)
        else:
            up_u, up_v, up_speed = mt_u, mt_v, mt_speed
            
        t_colorize_start = time.perf_counter()
        # Colorize wind speed
        rgba_speed = apply_colormap(up_speed, "wind_surface")
        
        # Quantize U/V to int8. Scale is roughly -100 to 100 m/s mapped to -127 to +127.
        # nan is mapped to -128 as nodata.
        nan_mask_uv = np.isnan(up_u) | np.isnan(up_v)
        u_scaled = np.clip(np.nan_to_num(up_u, nan=0.0) * (127.0 / 100.0), -127, 127).astype(np.int8)
        v_scaled = np.clip(np.nan_to_num(up_v, nan=0.0) * (127.0 / 100.0), -127, 127).astype(np.int8)
        u_scaled[nan_mask_uv] = -128
        v_scaled[nan_mask_uv] = -128
        stats["colorize_time_s"] += time.perf_counter() - t_colorize_start
        stats["metatile_extract_time_s"] = t_colorize_start - start_t
        
        base_chunk_path = Path(output_dir) / "wind_base" / str(z) / f"{cx}_{cy}.chunk"
        field_chunk_path = Path(output_dir) / "wind_field" / str(z) / f"{cx}_{cy}.chunk"
        
        writer_base = ChunkWriter(base_chunk_path, z, cx, cy)
        writer_field = ChunkWriter(field_chunk_path, z, cx, cy)
        
        for local_y in range(metatile.tiles_y):
            for local_x in range(metatile.tiles_x):
                stats["total"] += 1
                
                px_y0 = local_y * TILE_SIZE
                px_y1 = px_y0 + TILE_SIZE
                px_x0 = local_x * TILE_SIZE
                px_x1 = px_x0 + TILE_SIZE
                
                # Check empty
                tile_rgba = rgba_speed[px_y0:px_y1, px_x0:px_x1]
                if np.all(tile_rgba[..., 3] == 0):
                    stats["empty_skipped"] += 1
                    stats["tiles_empty_skipped_inside_nonempty"] += 1
                    continue
                    
                t_enc_start = time.perf_counter()
                png_bytes = b""
                field_wfld_bytes = b""
                if not field_only:
                    # 1. Base PNG
                    img_speed = Image.fromarray(tile_rgba, mode="RGBA")
                    png_bytes = _encode_tile_image(img_speed, "wind_surface")

                if not base_only:
                    # 2. Field WFLD: 8-byte header + Zstd(2D-delta U ∥ 2D-delta V)
                    tile_u_t = u_scaled[px_y0:px_y1, px_x0:px_x1]
                    tile_v_t = v_scaled[px_y0:px_y1, px_x0:px_x1]
                    stats["bytes_before_compress"] += tile_u_t.size + tile_v_t.size

                    field_wfld_bytes = _wind_field_wfld(tile_u_t, tile_v_t)
                    stats["bytes_after_compress"] += len(field_wfld_bytes)
                
                stats["encode_time_s"] += time.perf_counter() - t_enc_start
                
                t_write_start = time.perf_counter()
                if not field_only:
                    writer_base.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, png_bytes)
                    stats["bytes"] += len(png_bytes)
                if not base_only:
                    writer_field.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, field_wfld_bytes)
                    stats["bytes"] += len(field_wfld_bytes)
                
                stats["chunk_write_time_s"] += time.perf_counter() - t_write_start
                stats["saved"] += 1
                
        if stats["saved"] > 0:
            t_write_start = time.perf_counter()
            if not field_only:
                writer_base.write()
            if not base_only:
                writer_field.write()
            stats["chunk_write_time_s"] += time.perf_counter() - t_write_start
            
    except Exception as exc:
        log.error(f"Wind metatile error z={z} cx={cx} cy={cy}: {exc}", exc_info=True)
        stats["errors"] = metatile.tiles_x * metatile.tiles_y
        
    stats["duration_s"] = time.perf_counter() - start_t
    return stats

def process_all_wind_metatiles(
    npy_u_path: str,
    npy_v_path: str,
    npy_speed_path: str,
    px_per_meter: float,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 5,
    workers: int = 8,
    base_only: bool = False,
    field_only: bool = False,
) -> Dict[str, Any]:
    summary = {
        "total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "chunks_written": 0, "bytes": 0, 
        "duration_s": 0.0,
        "metatiles_total": 0, "metatiles_empty_skipped": 0, "tiles_empty_skipped_inside_nonempty": 0,
        "bytes_before_compress": 0, "bytes_after_compress": 0,
        "metatile_extract_time_s": 0.0, "colorize_time_s": 0.0,
        "encode_time_s": 0.0, "chunk_write_time_s": 0.0,
    }
    start_t = time.perf_counter()
    
    (output_dir / "wind_base").mkdir(parents=True, exist_ok=True)
    (output_dir / "wind_field").mkdir(parents=True, exist_ok=True)
    
    metatiles = list(iter_metatiles(zoom_min, zoom_max))
    safe_workers = _resolve_worker_count(workers)
    max_inflight = max(1, safe_workers * _CFG.TILE_MAX_INFLIGHT_MULTIPLIER)
    log.info(
        "Wind metatiles: workers=%d inflight_limit=%d",
        safe_workers, max_inflight,
    )
    with ProcessPoolExecutor(max_workers=safe_workers) as pool:
        futures = []
        for mt in metatiles:
            if _should_skip_chunk(output_dir, mt.z, mt.cx, mt.cy, "wind_base", "wind_field"):
                summary["chunks_written"] += 2
                continue
            if len(futures) >= max_inflight:
                _wait_and_drain_one(futures, summary, chunk_increment=2)
            fut = pool.submit(
                process_wind_metatile_worker,
                npy_u_path, npy_v_path, npy_speed_path, px_per_meter,
                mt.z, mt.cx, mt.cy, str(output_dir), base_only, field_only
            )
            futures.append(fut)
        for fut in as_completed(futures):
            try:
                _aggregate_stats(fut.result(), summary, 2)
            except Exception as e:
                log.error(f"Wind worker crashed: {e}")
                
    summary["duration_s"] = time.perf_counter() - start_t
    return summary


# ---------------------------------------------------------------------------
# Temporal wind worker — all frames for one metatile encoded together
# ---------------------------------------------------------------------------

def process_wind_temporal_metatile_worker(
    npy_u_paths: list,
    npy_v_paths: list,
    frame_nums: list,
    px_per_meter: float,
    z: int,
    cx: int,
    cy: int,
    output_dir: str,
) -> Dict[str, Any]:
    """Worker: load all frame canvases for one metatile, encode per-tile temporal sequence.

    Each (z, x, y) tile gets its own directory of .wf / .wfd files.
    Memory: n_frames × metatile_pixels × 2 channels × 1 byte  (≈600 MB at z=8 for 73 frames).
    """
    from .wind_temporal import generate_wind_tiles_for_run

    start_t = time.perf_counter()
    stats = {
        "tiles_total": 0, "tiles_saved": 0, "tiles_empty_skipped": 0,
        "errors": 0, "bytes": 0, "duration_s": 0.0,
    }
    metatile = Metatile(z, cx, cy)

    try:
        # mmap all canvases — OS only loads accessed pages
        merc_u_all = [_np_load(p) for p in npy_u_paths]
        merc_v_all = [_np_load(p) for p in npy_v_paths]
        canvas_size = merc_u_all[0].shape[0]
        x0, y0, x1, y1 = metatile.get_master_canvas_slice(canvas_size, px_per_meter)

        if x1 <= x0 or y1 <= y0:
            stats["tiles_empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["tiles_total"] += metatile.tiles_x * metatile.tiles_y
            return stats

        target_w = metatile.pixel_width
        target_h = metatile.pixel_height

        def _upsample_uv(raw_u, raw_v):
            """Upsample float32 metatile region to (target_h, target_w), return int8 U/V."""
            nan_mask = ~np.isfinite(raw_u) | ~np.isfinite(raw_v)
            if nan_mask.all():
                fill = np.full((target_h, target_w), -128, dtype=np.int8)
                return fill, fill.copy()
            h_s, w_s = raw_u.shape
            if h_s == target_h and w_s == target_w:
                up_u, up_v = raw_u, raw_v
            else:
                def _resize(arr, fill_val):
                    filled = np.where(nan_mask, fill_val, arr).astype(np.float32)
                    return np.array(
                        Image.fromarray(filled, mode="F").resize(
                            (target_w, target_h), Image.Resampling.BILINEAR
                        ),
                        dtype=np.float32,
                    )
                up_u = _resize(raw_u, 0.0)
                up_v = _resize(raw_v, 0.0)
                valid_up = np.array(
                    Image.fromarray((~nan_mask).astype(np.float32), mode="F").resize(
                        (target_w, target_h), Image.Resampling.BILINEAR
                    )
                ) >= 0.01
                up_u[~valid_up] = np.nan
                up_v[~valid_up] = np.nan

            nan_uv = np.isnan(up_u) | np.isnan(up_v)
            u_s = np.clip(np.nan_to_num(up_u, nan=0.0) * (127.0 / 100.0), -127, 127).astype(np.int8)
            v_s = np.clip(np.nan_to_num(up_v, nan=0.0) * (127.0 / 100.0), -127, 127).astype(np.int8)
            u_s[nan_uv] = -128
            v_s[nan_uv] = -128
            return u_s, v_s

        # Precompute all frames' int8 metatile regions once
        frame_u_mt: list = []
        frame_v_mt: list = []
        for merc_u, merc_v in zip(merc_u_all, merc_v_all):
            mt_u = merc_u[y0:y1, x0:x1]
            mt_v = merc_v[y0:y1, x0:x1]
            u_s, v_s = _upsample_uv(mt_u, mt_v)
            frame_u_mt.append(u_s)
            frame_v_mt.append(v_s)

        out_path = Path(output_dir)
        n_frames = len(frame_nums)

        for local_y in range(metatile.tiles_y):
            for local_x in range(metatile.tiles_x):
                stats["tiles_total"] += 1
                px_y0 = local_y * TILE_SIZE
                px_y1 = px_y0 + TILE_SIZE
                px_x0 = local_x * TILE_SIZE
                px_x1 = px_x0 + TILE_SIZE

                tile_x = metatile.x_min + local_x
                tile_y = metatile.y_min + local_y

                frames_uv = [
                    (frame_u_mt[i][px_y0:px_y1, px_x0:px_x1].copy(),
                     frame_v_mt[i][px_y0:px_y1, px_x0:px_x1].copy())
                    for i in range(n_frames)
                ]

                # Skip if all frames are all-NaN for this tile
                if all((u == -128).all() for u, _ in frames_uv):
                    stats["tiles_empty_skipped"] += 1
                    continue

                b = generate_wind_tiles_for_run(frames_uv, out_path, z, tile_x, tile_y)
                stats["bytes"] += b
                stats["tiles_saved"] += 1

    except Exception as exc:
        log.error("Wind temporal worker z=%d cx=%d cy=%d: %s", z, cx, cy, exc, exc_info=True)
        stats["errors"] += 1

    stats["duration_s"] = round(time.perf_counter() - start_t, 3)
    return stats


def process_all_wind_temporal_metatiles(
    npy_u_paths: list,
    npy_v_paths: list,
    frame_nums: list,
    px_per_meter: float,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 5,
    workers: int = 4,
) -> Dict[str, Any]:
    """Orchestrate temporal wind encoding across all metatiles for a complete run.

    All frame canvases must already exist in staging before calling this.
    workers default = 4 (lower than tile workers due to higher per-worker memory).
    """
    summary: Dict[str, Any] = {
        "tiles_total": 0, "tiles_saved": 0, "tiles_empty_skipped": 0,
        "errors": 0, "bytes": 0, "chunks_written": 0, "duration_s": 0.0,
    }
    start_t = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    metatiles = list(iter_metatiles(zoom_min, zoom_max))
    safe_workers = max(1, min(workers, _resolve_worker_count(workers)))
    max_inflight = max(1, safe_workers * _CFG.TILE_MAX_INFLIGHT_MULTIPLIER)
    log.info(
        "Wind temporal metatiles: frames=%d workers=%d inflight=%d metatiles=%d",
        len(frame_nums), safe_workers, max_inflight, len(metatiles),
    )

    with ProcessPoolExecutor(max_workers=safe_workers) as pool:
        futures = []
        for mt in metatiles:
            if len(futures) >= max_inflight:
                _wait_and_drain_one(futures, summary, chunk_increment=0)
            fut = pool.submit(
                process_wind_temporal_metatile_worker,
                npy_u_paths, npy_v_paths, frame_nums, px_per_meter,
                mt.z, mt.cx, mt.cy, str(output_dir),
            )
            futures.append(fut)
        for fut in as_completed(futures):
            try:
                res = fut.result()
                for k in ("tiles_total", "tiles_saved", "tiles_empty_skipped", "errors", "bytes"):
                    summary[k] = summary.get(k, 0) + res.get(k, 0)
            except Exception as e:
                log.error("Wind temporal worker crashed: %s", e)

    summary["duration_s"] = round(time.perf_counter() - start_t, 3)
    return summary


def process_adv_precip_metatile_worker(
    npy_paths: Dict[str, str],
    px_per_meter: float,
    z: int,
    cx: int,
    cy: int,
    output_dir: str
) -> Dict[str, Any]:
    """Worker for advanced precipitation using SSAA and Late Classification."""
    from .precip_classifier import classify_on_mercator
    
    start_t = time.perf_counter()
    stats = {
        "total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "bytes": 0, "chunk_path": None,
        "metatile_extract_time_s": 0.0, "colorize_time_s": 0.0,
        "encode_time_s": 0.0, "chunk_write_time_s": 0.0,
        "bytes_before_compress": 0, "bytes_after_compress": 0,
        "metatiles_total": 1, "metatiles_empty_skipped": 0,
        "tiles_empty_skipped_inside_nonempty": 0,
    }
    metatile = Metatile(z, cx, cy)

    try:
        mercs = {k: _np_load(v) for k, v in npy_paths.items()}
        canvas_size = mercs["prate"].shape[0]
        x0, y0, x1, y1 = metatile.get_master_canvas_slice(canvas_size, px_per_meter)
        
        if x1 <= x0 or y1 <= y0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["total"] += metatile.tiles_x * metatile.tiles_y
            stats["metatiles_empty_skipped"] = 1
            stats["metatile_extract_time_s"] = time.perf_counter() - start_t
            return stats
            
        slices = {k: m[y0:y1, x0:x1] for k, m in mercs.items()}
        slice_prate = slices["prate"]
        
        # Skip completely dry or all NaNs
        if slice_prate.size == 0 or np.all((slice_prate == 0) | ~np.isfinite(slice_prate)):
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["total"] += metatile.tiles_x * metatile.tiles_y
            stats["metatiles_empty_skipped"] = 1
            stats["metatile_extract_time_s"] = time.perf_counter() - start_t
            return stats
            
        # Oversample size (2x for SSAA)
        ssaa_w = metatile.pixel_width * 2
        ssaa_h = metatile.pixel_height * 2
        
        def _upsample(arr):
            nan_mask = ~np.isfinite(arr)
            if nan_mask.all():
                return np.full((ssaa_h, ssaa_w), np.nan, dtype=np.float32)
            filled = np.where(nan_mask, 0.0, arr).astype(np.float32)
            img = Image.fromarray(filled, mode="F").resize((ssaa_w, ssaa_h), Image.Resampling.BILINEAR)
            return np.array(img, dtype=np.float32)
            
        up = {k: _upsample(v) for k, v in slices.items()}
        
        t_colorize_start = time.perf_counter()
        # Late Classification on the high-res array
        idx_ssaa = classify_on_mercator(
            up["prate"], up["crain"], up["csnow"], up["cicep"], up["cfrzr"], up["cpofp"], 
            min_patch_pixels=10, verbose=False
        )
        
        # Colorize
        rgba_ssaa = apply_colormap(idx_ssaa, "precip_base")
        img_ssaa = Image.fromarray(rgba_ssaa, mode="RGBA")
        
        # Downsample with LANCZOS to feather alpha at boundaries
        img_mt = img_ssaa.resize((metatile.pixel_width, metatile.pixel_height), Image.Resampling.LANCZOS)
        rgba_mt = np.array(img_mt)
        
        stats["colorize_time_s"] += time.perf_counter() - t_colorize_start
        stats["metatile_extract_time_s"] = t_colorize_start - start_t
        
        chunk_path = Path(output_dir) / str(z) / f"{cx}_{cy}.chunk"
        writer = ChunkWriter(chunk_path, z, cx, cy)
        
        for local_y in range(metatile.tiles_y):
            for local_x in range(metatile.tiles_x):
                stats["total"] += 1
                
                px_y0 = local_y * TILE_SIZE
                px_y1 = px_y0 + TILE_SIZE
                px_x0 = local_x * TILE_SIZE
                px_x1 = px_x0 + TILE_SIZE
                
                tile_rgba = rgba_mt[px_y0:px_y1, px_x0:px_x1]
                
                if np.all(tile_rgba[..., 3] == 0):
                    stats["empty_skipped"] += 1
                    stats["tiles_empty_skipped_inside_nonempty"] += 1
                    continue
                    
                t_enc_start = time.perf_counter()
                img = Image.fromarray(tile_rgba, mode="RGBA")
                png_bytes = _encode_tile_image(img, "precip_base")
                stats["encode_time_s"] += time.perf_counter() - t_enc_start
                
                t_write_start = time.perf_counter()
                writer.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, png_bytes)
                stats["chunk_write_time_s"] += time.perf_counter() - t_write_start
                
                stats["saved"] += 1
                stats["bytes"] += len(png_bytes)
                
        if stats["saved"] > 0:
            t_write_start = time.perf_counter()
            writer.write()
            stats["chunk_write_time_s"] += time.perf_counter() - t_write_start
            stats["chunk_path"] = str(chunk_path)
            
    except Exception as exc:
        log.error(f"Adv precip metatile error z={z} cx={cx} cy={cy}: {exc}", exc_info=True)
        stats["errors"] = metatile.tiles_x * metatile.tiles_y
        
    stats["duration_s"] = time.perf_counter() - start_t
    return stats

def process_all_adv_precip_metatiles(
    npy_paths: Dict[str, str],
    px_per_meter: float,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 5,
    workers: int = 8,
) -> Dict[str, Any]:
    summary = {
        "total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "chunks_written": 0, "bytes": 0,
        "duration_s": 0.0,
        "metatiles_total": 0, "metatiles_empty_skipped": 0, "tiles_empty_skipped_inside_nonempty": 0,
        "bytes_before_compress": 0, "bytes_after_compress": 0,
        "metatile_extract_time_s": 0.0, "colorize_time_s": 0.0,
        "encode_time_s": 0.0, "chunk_write_time_s": 0.0,
    }
    start_t = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    metatiles = list(iter_metatiles(zoom_min, zoom_max))
    safe_workers = _resolve_worker_count(workers)
    max_inflight = max(1, safe_workers * _CFG.TILE_MAX_INFLIGHT_MULTIPLIER)
    log.info(
        "Adv precip metatiles: workers=%d inflight_limit=%d",
        safe_workers, max_inflight,
    )
    with ProcessPoolExecutor(max_workers=safe_workers) as pool:
        futures = []
        for mt in metatiles:
            if _should_skip_chunk(output_dir, mt.z, mt.cx, mt.cy):
                summary["chunks_written"] += 1
                continue
            if len(futures) >= max_inflight:
                _wait_and_drain_one(futures, summary, chunk_increment=1)
            fut = pool.submit(
                process_adv_precip_metatile_worker,
                npy_paths, px_per_meter, mt.z, mt.cx, mt.cy, str(output_dir)
            )
            futures.append(fut)
        for fut in as_completed(futures):
            try:
                _aggregate_stats(fut.result(), summary, 1)
            except Exception as e:
                log.error(f"Adv precip worker crashed: {e}")

    summary["duration_s"] = time.perf_counter() - start_t
    return summary


# ---------------------------------------------------------------------------
# Pre-classified precipitation worker (classify ONCE on full canvas)
# ---------------------------------------------------------------------------

def process_precip_classified_metatile_worker(
    combined_npy_path: str,
    px_per_meter: float,
    z: int,
    cx: int,
    cy: int,
    output_dir: str,
) -> Dict[str, Any]:
    """Worker for pre-classified precipitation: loads combined_index canvas once,
    slices metatile region, upsamples with NEAREST, colorizes, writes chunk.

    This avoids calling classify_on_mercator() per metatile (the ~1272s bottleneck).
    The combined_index array is NaN=dry, 1..6=rain, 7..12=mixed, 13..18=snow.
    """
    start_t = time.perf_counter()
    stats = {
        "total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "bytes": 0, "chunk_path": None,
        "metatile_extract_time_s": 0.0, "colorize_time_s": 0.0,
        "encode_time_s": 0.0, "chunk_write_time_s": 0.0,
        "bytes_before_compress": 0, "bytes_after_compress": 0,
        "metatiles_total": 1, "metatiles_empty_skipped": 0,
        "tiles_empty_skipped_inside_nonempty": 0,
    }
    metatile = Metatile(z, cx, cy)

    try:
        combined = _np_load(combined_npy_path)
        canvas_size = combined.shape[0]
        x0, y0, x1, y1 = metatile.get_master_canvas_slice(canvas_size, px_per_meter)

        if x1 <= x0 or y1 <= y0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["total"] += metatile.tiles_x * metatile.tiles_y
            stats["metatiles_empty_skipped"] = 1
            stats["metatile_extract_time_s"] = time.perf_counter() - start_t
            return stats

        mt_combined = combined[y0:y1, x0:x1]

        # Skip entirely dry metatiles (all NaN)
        if mt_combined.size == 0 or not np.isfinite(mt_combined).any():
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["total"] += metatile.tiles_x * metatile.tiles_y
            stats["metatiles_empty_skipped"] = 1
            stats["metatile_extract_time_s"] = time.perf_counter() - start_t
            return stats

        target_w = metatile.pixel_width
        target_h = metatile.pixel_height
        h_s, w_s = mt_combined.shape

        # NEAREST upsample preserves discrete integer class indices (no blending)
        if h_s != target_h or w_s != target_w:
            # Replace NaN with 0.0 for PIL resize (we restore via alpha)
            filled = np.where(np.isfinite(mt_combined), mt_combined, 0.0).astype(np.float32)
            mt_up = np.array(
                Image.fromarray(filled, mode="F").resize((target_w, target_h), Image.Resampling.NEAREST),
                dtype=np.float32,
            )
            valid_up = np.array(
                Image.fromarray(np.isfinite(mt_combined).astype(np.float32), mode="F").resize(
                    (target_w, target_h), Image.Resampling.NEAREST
                ),
                dtype=np.float32,
            )
            mt_up[valid_up < 0.5] = np.nan
        else:
            mt_up = np.array(mt_combined, dtype=np.float32)

        t_colorize_start = time.perf_counter()
        rgba_mt = apply_colormap(mt_up, "precip_base")
        stats["colorize_time_s"] += time.perf_counter() - t_colorize_start
        stats["metatile_extract_time_s"] = t_colorize_start - start_t

        chunk_path = Path(output_dir) / str(z) / f"{cx}_{cy}.chunk"
        writer = ChunkWriter(chunk_path, z, cx, cy)

        for local_y in range(metatile.tiles_y):
            for local_x in range(metatile.tiles_x):
                stats["total"] += 1

                px_y0 = local_y * TILE_SIZE
                px_y1 = px_y0 + TILE_SIZE
                px_x0 = local_x * TILE_SIZE
                px_x1 = px_x0 + TILE_SIZE

                tile_rgba = rgba_mt[px_y0:px_y1, px_x0:px_x1]

                if np.all(tile_rgba[..., 3] == 0):
                    stats["empty_skipped"] += 1
                    stats["tiles_empty_skipped_inside_nonempty"] += 1
                    continue

                t_enc_start = time.perf_counter()
                img = Image.fromarray(tile_rgba, mode="RGBA")
                png_bytes = _encode_tile_image(img, "precip_base")
                stats["encode_time_s"] += time.perf_counter() - t_enc_start

                t_write_start = time.perf_counter()
                writer.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, png_bytes)
                stats["chunk_write_time_s"] += time.perf_counter() - t_write_start

                stats["saved"] += 1
                stats["bytes"] += len(png_bytes)

        if stats["saved"] > 0:
            t_write_start = time.perf_counter()
            writer.write()
            stats["chunk_write_time_s"] += time.perf_counter() - t_write_start
            stats["chunk_path"] = str(chunk_path)

    except Exception as exc:
        log.error(f"Classified precip metatile error z={z} cx={cx} cy={cy}: {exc}", exc_info=True)
        stats["errors"] = metatile.tiles_x * metatile.tiles_y

    stats["duration_s"] = time.perf_counter() - start_t
    return stats


def process_all_precip_classified_metatiles(
    combined_npy_path: str,
    px_per_meter: float,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 8,
    workers: int = 8,
) -> Dict[str, Any]:
    """Orchestrate classified-precip metatile workers.

    Each worker slices the pre-computed combined_index canvas and colorizes,
    eliminating the per-metatile classify_on_mercator() call (~1272s → ~0s).
    """
    summary = {
        "total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "chunks_written": 0, "bytes": 0,
        "duration_s": 0.0,
        "metatiles_total": 0, "metatiles_empty_skipped": 0, "tiles_empty_skipped_inside_nonempty": 0,
        "bytes_before_compress": 0, "bytes_after_compress": 0,
        "metatile_extract_time_s": 0.0, "colorize_time_s": 0.0,
        "encode_time_s": 0.0, "chunk_write_time_s": 0.0,
    }
    start_t = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)

    if not os.path.exists(combined_npy_path):
        log.error("process_all_precip_classified_metatiles: combined_npy not found: %s", combined_npy_path)
        return summary

    metatiles = list(iter_metatiles(zoom_min, zoom_max))
    safe_workers = _resolve_worker_count(workers)
    max_inflight = max(1, safe_workers * _CFG.TILE_MAX_INFLIGHT_MULTIPLIER)
    log.info(
        "Classified precip metatiles: workers=%d inflight_limit=%d total_metatiles=%d",
        safe_workers, max_inflight, len(metatiles),
    )
    with ProcessPoolExecutor(max_workers=safe_workers) as pool:
        futures = []
        for mt in metatiles:
            if _should_skip_chunk(output_dir, mt.z, mt.cx, mt.cy):
                summary["chunks_written"] += 1
                continue
            if len(futures) >= max_inflight:
                _wait_and_drain_one(futures, summary, chunk_increment=1)
            fut = pool.submit(
                process_precip_classified_metatile_worker,
                combined_npy_path, px_per_meter, mt.z, mt.cx, mt.cy, str(output_dir),
            )
            futures.append(fut)
        for fut in as_completed(futures):
            try:
                _aggregate_stats(fut.result(), summary, 1)
            except Exception as e:
                log.error(f"Classified precip worker crashed: {e}")

    summary["duration_s"] = time.perf_counter() - start_t
    return summary

def process_metatile_worker(
    npy_path: str,
    px_per_meter: float,
    z: int,
    cx: int,
    cy: int,
    cmap_type: str,
    cmap_product: Optional[str],
    output_dir: str
) -> Dict[str, Any]:
    """
    Worker function to process a single metatile.
    Loads the mmap canvas, slices it, downsamples/upsamples, colorizes,
    splits into 256x256 tiles, and writes a .chunk file.
    Returns statistics.
    """
    start_t = time.perf_counter()
    stats = {
        "total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "chunk_path": None, "bytes": 0,
        "metatile_extract_time_s": 0.0, "colorize_time_s": 0.0,
        "encode_time_s": 0.0, "chunk_write_time_s": 0.0,
        "bytes_before_compress": 0, "bytes_after_compress": 0,
        "metatiles_total": 1, "metatiles_empty_skipped": 0,
        "tiles_empty_skipped_inside_nonempty": 0,
    }
    metatile = Metatile(z, cx, cy)

    try:
        # Validate the npy canvas file exists before any worker tries to open it.
        # This gives a clear error instead of a cryptic FileNotFoundError deep in mmap.
        if not os.path.exists(npy_path):
            raise FileNotFoundError(
                f"Canvas .npy not found (deleted too early or never written): {npy_path}"
            )
        merc = _np_load(npy_path)
        canvas_size = merc.shape[0]

        # Slicing from master canvas
        x0, y0, x1, y1 = metatile.get_master_canvas_slice(canvas_size, px_per_meter)
        
        if x1 <= x0 or y1 <= y0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["total"] += metatile.tiles_x * metatile.tiles_y
            stats["metatiles_empty_skipped"] = 1
            stats["metatile_extract_time_s"] = time.perf_counter() - start_t
            return stats
            
        mt_scalar = merc[y0:y1, x0:x1]
        
        if mt_scalar.size == 0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            stats["total"] += metatile.tiles_x * metatile.tiles_y
            stats["metatiles_empty_skipped"] = 1
            stats["metatile_extract_time_s"] = time.perf_counter() - start_t
            return stats
            
        nan_mask_mt = ~np.isfinite(mt_scalar)
        
        # Tier 1 empty skip: all NaNs or all dry (if banded)
        if cmap_type in _BANDED_MAP_TYPES:
            if np.all((mt_scalar == 0) | nan_mask_mt):
                stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
                stats["total"] += metatile.tiles_x * metatile.tiles_y
                stats["metatiles_empty_skipped"] = 1
                stats["metatile_extract_time_s"] = time.perf_counter() - start_t
                return stats
        else:
            if nan_mask_mt.all():
                stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
                stats["total"] += metatile.tiles_x * metatile.tiles_y
                stats["metatiles_empty_skipped"] = 1
                stats["metatile_extract_time_s"] = time.perf_counter() - start_t
                return stats
            
        # Resampling logic (Scalar-first)
        # We need to resize mt_scalar to (metatile.pixel_width, metatile.pixel_height)
        target_w = metatile.pixel_width
        target_h = metatile.pixel_height
        
        h_s, w_s = mt_scalar.shape
        if h_s != target_h or w_s != target_w:
            pil_resample = Image.Resampling.NEAREST if cmap_type in _NEAREST_CMAP_TYPES else Image.Resampling.LANCZOS
            valid_resample = Image.Resampling.NEAREST if cmap_type in _NEAREST_CMAP_TYPES else Image.Resampling.BILINEAR
            
            if pil_resample == Image.Resampling.NEAREST:
                fill_val = 0.0
            else:
                fill_val = float(np.nanmean(mt_scalar)) if not nan_mask_mt.all() else 0.0
                
            filled = np.where(nan_mask_mt, fill_val, mt_scalar).astype(np.float32)
            
            scalar_img = Image.fromarray(filled, mode="F")
            scalar_img = scalar_img.resize((target_w, target_h), pil_resample)
            
            valid_img = Image.fromarray((~nan_mask_mt).astype(np.float32), mode="F")
            valid_img = valid_img.resize((target_w, target_h), valid_resample)
            
            mt_scalar_up = np.array(scalar_img, dtype=np.float32)
            mt_scalar_up[np.array(valid_img) < 0.01] = np.nan
        else:
            mt_scalar_up = mt_scalar
            
        t_colorize_start = time.perf_counter()
        # Apply Colormap
        rgba_mt = apply_colormap(mt_scalar_up, cmap_type, product=cmap_product)
        stats["colorize_time_s"] += time.perf_counter() - t_colorize_start
        stats["metatile_extract_time_s"] = t_colorize_start - start_t
        
        # Split into individual 256x256 tiles and save to ChunkWriter
        chunk_path = Path(output_dir) / str(z) / f"{cx}_{cy}.chunk"
        writer = ChunkWriter(chunk_path, z, cx, cy)
        
        for local_y in range(metatile.tiles_y):
            for local_x in range(metatile.tiles_x):
                stats["total"] += 1
                
                # Slice 256x256 from the large RGBA array
                px_y0 = local_y * TILE_SIZE
                px_y1 = px_y0 + TILE_SIZE
                px_x0 = local_x * TILE_SIZE
                px_x1 = px_x0 + TILE_SIZE
                
                tile_rgba = rgba_mt[px_y0:px_y1, px_x0:px_x1]
                
                # Tier 2 empty skip: fully transparent
                if np.all(tile_rgba[..., 3] == 0):
                    stats["empty_skipped"] += 1
                    stats["tiles_empty_skipped_inside_nonempty"] += 1
                    continue
                    
                t_enc_start = time.perf_counter()
                img = Image.fromarray(tile_rgba, mode="RGBA")
                
                # Quantize for banded layers to save size
                if cmap_type in _NEAREST_CMAP_TYPES or cmap_type in {"rain_basic", "snow_depth", "tmp_2m", "temperature_feels_like"}:
                    img = img.convert("RGBA")
                png_bytes = _encode_tile_image(img, cmap_type)
                stats["encode_time_s"] += time.perf_counter() - t_enc_start
                
                t_write_start = time.perf_counter()
                writer.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, png_bytes)
                stats["chunk_write_time_s"] += time.perf_counter() - t_write_start
                
                stats["saved"] += 1
                stats["bytes"] += len(png_bytes)
                
        # Write chunk if there are any saved tiles
        if stats["saved"] > 0:
            t_write_start = time.perf_counter()
            writer.write()
            stats["chunk_write_time_s"] += time.perf_counter() - t_write_start
            stats["chunk_path"] = str(chunk_path)
            
    except Exception as exc:
        log.error(f"Metatile error z={z} cx={cx} cy={cy}: {exc}", exc_info=True)
        stats["errors"] = metatile.tiles_x * metatile.tiles_y
        
    stats["duration_s"] = time.perf_counter() - start_t
    return stats

def process_all_metatiles(
    npy_path: str,
    px_per_meter: float,
    cmap_type: str,
    cmap_product: Optional[str],
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 5,
    workers: int = 8,
) -> Dict[str, Any]:
    """
    Submits all metatiles across all zooms to a ProcessPoolExecutor.
    """
    summary = {
        "total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "chunks_written": 0, "bytes": 0, 
        "duration_s": 0.0,
        "metatiles_total": 0, "metatiles_empty_skipped": 0, "tiles_empty_skipped_inside_nonempty": 0,
        "bytes_before_compress": 0, "bytes_after_compress": 0,
        "metatile_extract_time_s": 0.0, "colorize_time_s": 0.0,
        "encode_time_s": 0.0, "chunk_write_time_s": 0.0,
    }
    
    start_t = time.perf_counter()
    output_dir.mkdir(parents=True, exist_ok=True)
    
    metatiles = list(iter_metatiles(zoom_min, zoom_max))
    safe_workers = _resolve_worker_count(workers)
    max_inflight = max(1, safe_workers * _CFG.TILE_MAX_INFLIGHT_MULTIPLIER)
    log.info(
        "Processing %d metatiles using workers=%d inflight_limit=%d",
        len(metatiles), safe_workers, max_inflight,
    )
    with ProcessPoolExecutor(max_workers=safe_workers) as pool:
        # Validate npy file once before submitting any workers
        if not os.path.exists(npy_path):
            log.error(
                "process_all_metatiles: npy_path does not exist, aborting: %s", npy_path
            )
            summary["errors"] = len(metatiles)
            return summary
        futures = []
        for mt in metatiles:
            if _should_skip_chunk(output_dir, mt.z, mt.cx, mt.cy):
                summary["chunks_written"] += 1
                continue
            if len(futures) >= max_inflight:
                _wait_and_drain_one(futures, summary, chunk_increment=1)
            fut = pool.submit(
                process_metatile_worker,
                npy_path, px_per_meter, mt.z, mt.cx, mt.cy,
                cmap_type, cmap_product, str(output_dir)
            )
            futures.append(fut)
        for fut in as_completed(futures):
            try:
                _aggregate_stats(fut.result(), summary, 1)
            except Exception as e:
                log.error(f"Worker crashed: {e}")
                
    summary["duration_s"] = time.perf_counter() - start_t
    return summary

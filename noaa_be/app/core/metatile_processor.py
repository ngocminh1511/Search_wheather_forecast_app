import io
import os
import time
import math
import logging
import numpy as np
from PIL import Image
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_COMPLETED
from typing import Dict, Any, Optional

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


def _drain_futures(futures, summary: Dict[str, Any], chunk_increment: int = 1) -> None:
    done = [f for f in futures if f.done()]
    for fut in done:
        futures.remove(fut)
        try:
            res = fut.result()
            summary["total"] += res.get("total", 0)
            summary["saved"] += res.get("saved", 0)
            summary["empty_skipped"] += res.get("empty_skipped", 0)
            summary["errors"] += res.get("errors", 0)
            summary["bytes"] += res.get("bytes", 0)
            if res.get("chunk_path") or res.get("saved", 0) > 0:
                summary["chunks_written"] += chunk_increment
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
            res = fut.result()
            summary["total"] += res.get("total", 0)
            summary["saved"] += res.get("saved", 0)
            summary["empty_skipped"] += res.get("empty_skipped", 0)
            summary["errors"] += res.get("errors", 0)
            summary["bytes"] += res.get("bytes", 0)
            if res.get("chunk_path") or res.get("saved", 0) > 0:
                summary["chunks_written"] += chunk_increment
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
    output_dir: str
) -> Dict[str, Any]:
    """Worker for wind: generates wind_base (PNG) and wind_field (binary u/v) chunks."""
    start_t = time.perf_counter()
    stats = {"total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "bytes": 0}
    
    try:
        merc_u = np.load(npy_u_path, mmap_mode="r")
        merc_v = np.load(npy_v_path, mmap_mode="r")
        merc_speed = np.load(npy_speed_path, mmap_mode="r")
        canvas_size = merc_u.shape[0]
        
        metatile = Metatile(z, cx, cy)
        x0, y0, x1, y1 = metatile.get_master_canvas_slice(canvas_size, px_per_meter)
        
        if x1 <= x0 or y1 <= y0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            return stats
            
        mt_u = merc_u[y0:y1, x0:x1]
        mt_v = merc_v[y0:y1, x0:x1]
        mt_speed = merc_speed[y0:y1, x0:x1]
        
        if mt_u.size == 0 or (~np.isfinite(mt_u)).all():
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
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
            
        # Colorize wind speed
        rgba_speed = apply_colormap(up_speed, "wind_surface")
        
        # Quantize U/V to int8. Scale is roughly -100 to 100 m/s mapped to -127 to +127.
        # nan is mapped to -128 as nodata.
        nan_mask_uv = np.isnan(up_u) | np.isnan(up_v)
        u_scaled = np.clip(np.nan_to_num(up_u, nan=0.0) * (127.0 / 100.0), -127, 127).astype(np.int8)
        v_scaled = np.clip(np.nan_to_num(up_v, nan=0.0) * (127.0 / 100.0), -127, 127).astype(np.int8)
        u_scaled[nan_mask_uv] = -128
        v_scaled[nan_mask_uv] = -128
        
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
                    continue
                    
                # 1. Base PNG
                img_speed = Image.fromarray(tile_rgba, mode="RGBA")
                png_bytes = _encode_tile_image(img_speed, "wind_surface")
                writer_base.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, png_bytes)
                
                # 2. Field binary (U, V arrays interleaved)
                # Flatten the 256x256 into a single 1D array of 65536 * 2 bytes
                tile_u = u_scaled[px_y0:px_y1, px_x0:px_x1].flatten()
                tile_v = v_scaled[px_y0:px_y1, px_x0:px_x1].flatten()
                # Interleave U and V: u0, v0, u1, v1...
                uv_bytes = np.empty((tile_u.size * 2,), dtype=np.int8)
                uv_bytes[0::2] = tile_u
                uv_bytes[1::2] = tile_v
                
                # We can compress the binary payload here using zlib to save size
                import zlib
                compressed_uv = zlib.compress(
                    uv_bytes.tobytes(),
                    level=_CFG.WIND_FIELD_COMPRESS_LEVEL,
                )
                
                writer_field.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, compressed_uv)
                
                stats["saved"] += 1
                stats["bytes"] += len(png_bytes) + len(compressed_uv)
                
        if stats["saved"] > 0:
            writer_base.write()
            writer_field.write()
            
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
) -> Dict[str, Any]:
    summary = {"total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "chunks_written": 0, "bytes": 0, "duration_s": 0.0}
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
                mt.z, mt.cx, mt.cy, str(output_dir)
            )
            futures.append(fut)
        for fut in as_completed(futures):
            try:
                res = fut.result()
                summary["total"] += res["total"]
                summary["saved"] += res["saved"]
                summary["empty_skipped"] += res["empty_skipped"]
                summary["errors"] += res["errors"]
                summary["bytes"] += res["bytes"]
                if res["saved"] > 0:
                    summary["chunks_written"] += 2
            except Exception as e:
                log.error(f"Wind worker crashed: {e}")
                
    summary["duration_s"] = time.perf_counter() - start_t
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
    stats = {"total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "bytes": 0, "chunk_path": None}
    
    try:
        mercs = {k: np.load(v, mmap_mode="r") for k, v in npy_paths.items()}
        canvas_size = mercs["prate"].shape[0]
        
        metatile = Metatile(z, cx, cy)
        x0, y0, x1, y1 = metatile.get_master_canvas_slice(canvas_size, px_per_meter)
        
        if x1 <= x0 or y1 <= y0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            return stats
            
        slices = {k: m[y0:y1, x0:x1] for k, m in mercs.items()}
        slice_prate = slices["prate"]
        
        if slice_prate.size == 0 or (~np.isfinite(slice_prate)).all():
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
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
                    continue
                    
                img = Image.fromarray(tile_rgba, mode="RGBA")
                png_bytes = _encode_tile_image(img, "precip_base")
                
                writer.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, png_bytes)
                stats["saved"] += 1
                stats["bytes"] += len(png_bytes)
                
        if stats["saved"] > 0:
            writer.write()
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
    summary = {"total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "chunks_written": 0, "bytes": 0, "duration_s": 0.0}
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
                res = fut.result()
                summary["total"] += res["total"]
                summary["saved"] += res["saved"]
                summary["empty_skipped"] += res["empty_skipped"]
                summary["errors"] += res["errors"]
                summary["bytes"] += res["bytes"]
                if res.get("chunk_path"):
                    summary["chunks_written"] += 1
            except Exception as e:
                log.error(f"Adv precip worker crashed: {e}")
                
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
    stats = {"total": 0, "saved": 0, "empty_skipped": 0, "errors": 0, "chunk_path": None, "bytes": 0}
    
    try:
        merc = np.load(npy_path, mmap_mode="r")
        canvas_size = merc.shape[0]
        
        metatile = Metatile(z, cx, cy)
        
        # Slicing from master canvas
        x0, y0, x1, y1 = metatile.get_master_canvas_slice(canvas_size, px_per_meter)
        
        if x1 <= x0 or y1 <= y0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            return stats
            
        mt_scalar = merc[y0:y1, x0:x1]
        
        if mt_scalar.size == 0:
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
            return stats
            
        # Tier 1 empty skip: all NaNs
        nan_mask_mt = ~np.isfinite(mt_scalar)
        if nan_mask_mt.all():
            stats["empty_skipped"] += metatile.tiles_x * metatile.tiles_y
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
            
        # Apply Colormap
        rgba_mt = apply_colormap(mt_scalar_up, cmap_type, product=cmap_product)
        
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
                    continue
                    
                img = Image.fromarray(tile_rgba, mode="RGBA")
                
                # Quantize for banded layers to save size
                if cmap_type in _NEAREST_CMAP_TYPES or cmap_type in {"rain_basic", "snow_depth", "tmp_2m", "temperature_feels_like"}:
                    img = img.convert("RGBA")
                png_bytes = _encode_tile_image(img, cmap_type)
                
                writer.add_tile(metatile.x_min + local_x, metatile.y_min + local_y, png_bytes)
                stats["saved"] += 1
                stats["bytes"] += len(png_bytes)
                
        # Write chunk if there are any saved tiles
        if stats["saved"] > 0:
            writer.write()
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
        "total": 0,
        "saved": 0,
        "empty_skipped": 0,
        "errors": 0,
        "chunks_written": 0,
        "bytes": 0,
        "duration_s": 0.0
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
                res = fut.result()
                summary["total"] += res["total"]
                summary["saved"] += res["saved"]
                summary["empty_skipped"] += res["empty_skipped"]
                summary["errors"] += res["errors"]
                summary["bytes"] += res["bytes"]
                if res.get("chunk_path"):
                    summary["chunks_written"] += 1
            except Exception as e:
                log.error(f"Worker crashed: {e}")
                
    summary["duration_s"] = time.perf_counter() - start_t
    return summary

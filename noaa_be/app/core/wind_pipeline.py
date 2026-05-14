from __future__ import annotations

import logging
import time
from pathlib import Path

import numpy as np
from rasterio.warp import Resampling

from ..config import get_settings
from .grib_reader import read_multi_fields
from ..core.tile_cutter import _warp_scalar_to_mercator, TILE_SIZE
from ..core.metatile_processor import process_all_wind_metatiles

_log = logging.getLogger(__name__)


def generate_wind_frame(
    run_id: str,
    fff: int,
    data_dir: Path,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int | None = None,
    workers: int | None = None,
    skip_existing: bool = True,
    base_only: bool = False,
    field_only: bool = False,
) -> dict:
    """
    Generate wind tiles (wind_base PNG + wind_field WFLD) for one (run_id, fff) frame.
    """
    cfg = get_settings()
    z_max = zoom_max if zoom_max is not None else cfg.TILE_ZOOM_EAGER_MAX
    z_max = min(z_max, cfg.TILE_PER_MAP_ZOOM.get("wind_surface", z_max))
    w = workers if workers is not None else cfg.TILE_PROCESS_WORKERS

    grib_path = (
        data_dir / "wind_surface" / run_id /
        "wind_10m" / f"f{fff:03d}.grib2"
    )

    if not grib_path.exists():
        _log.debug("wind_pipeline: GRIB missing %s", grib_path)
        return {"skipped": True, "reason": f"GRIB missing: {grib_path}"}
    if grib_path.stat().st_size == 0:
        _log.debug("wind_pipeline: empty GRIB %s", grib_path.name)
        return {"skipped": True, "reason": f"GRIB empty: {grib_path.name}"}

    t0 = time.perf_counter()
    timings = {
        "raw_read_time_s": 0.0,
        "warp_time_s": 0.0,
        "tile_cut_time_s": 0.0,
    }

    # 1. Read UGRD, VGRD
    t_read_start = time.perf_counter()
    try:
        fields = read_multi_fields(grib_path, ["10u", "10v"])
    except Exception as exc:
        _log.error("wind_pipeline: read failed %s: %s", grib_path.name, exc)
        return {"skipped": True, "reason": f"read error: {exc}"}

    if "10u" not in fields or "10v" not in fields:
        return {"skipped": True, "reason": "Missing 10u or 10v in GRIB"}

    u_field = fields["10u"]
    v_field = fields["10v"]

    _log.info(
        "wind_pipeline %s/f%03d: u_max=%.2f v_max=%.2f",
        run_id, fff,
        float(np.nanmax(np.abs(u_field.values))),
        float(np.nanmax(np.abs(v_field.values))),
    )
    timings["raw_read_time_s"] = time.perf_counter() - t_read_start

    # 2. Warp to Mercator
    t_warp_start = time.perf_counter()
    canvas_size = min((2 ** z_max) * TILE_SIZE, 8192)
    _log.info("wind_pipeline: warping u/v to %dx%d Mercator canvas ...",
              canvas_size, canvas_size)

    merc_u, px_m = _warp_scalar_to_mercator(
        u_field.values, u_field.lat, u_field.lon, canvas_size,
        resampling=Resampling.bilinear,
    )
    merc_v, _ = _warp_scalar_to_mercator(
        v_field.values, v_field.lat, v_field.lon, canvas_size,
        resampling=Resampling.bilinear,
    )

    # 3. Compute speed
    merc_speed = np.sqrt(merc_u**2 + merc_v**2)
    timings["warp_time_s"] = time.perf_counter() - t_warp_start

    # Save to npy for IPC
    staging_dir = cfg.STAGING_DIR / "wind_surface" / run_id / "canvases"
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    npy_u = staging_dir / f"wind_u_{fff:03d}.npy"
    npy_v = staging_dir / f"wind_v_{fff:03d}.npy"
    npy_speed = staging_dir / f"wind_speed_{fff:03d}.npy"
    
    np.save(str(npy_u), merc_u)
    np.save(str(npy_v), merc_v)
    np.save(str(npy_speed), merc_speed)

    # 4. Cut and save using metatile processor
    _log.info("wind_pipeline: processing wind metatiles (base PNG + field BIN) ...")
    t_cut_start = time.perf_counter()
    try:
        summary = process_all_wind_metatiles(
            npy_u_path=str(npy_u),
            npy_v_path=str(npy_v),
            npy_speed_path=str(npy_speed),
            px_per_meter=px_m,
            output_dir=output_dir / f"{fff:03d}",
            zoom_min=zoom_min,
            zoom_max=z_max,
            workers=w,
            base_only=base_only,
            field_only=field_only,
        )
        
        # Write manifests
        import json
        for prod in ["wind_base", "wind_field"]:
            manifest = {
                "ready": True,
                "product": prod,
                "total": summary["total"],
                "saved": summary["saved"],
                "empty_skipped": summary["empty_skipped"],
                "errors": summary["errors"],
                "chunks_written": summary["chunks_written"] // 2, # Halved because it counts both
                "total_size_bytes": summary["bytes"] // 2, # Approximation
                "format": "chunk",
                "tile_format": cfg.TILE_FORMAT_DEFAULT if prod == "wind_base" else "wfld",
                "tile_ext": ("webp" if cfg.TILE_FORMAT_DEFAULT == "webp" else "png") if prod == "wind_base" else "bin",
                "timestamp": time.time()
            }
            (output_dir / f"{fff:03d}" / prod / "manifest.json").write_text(json.dumps(manifest, indent=2))

        elapsed = time.perf_counter() - t0
        _log.info(
            "wind_pipeline %s/f%03d DONE: chunks_written=%d skipped=%d err=%d | %.3fs",
            run_id, fff,
            summary.get("chunks_written", 0),
            summary.get("empty_skipped", 0),
            summary.get("errors", 0),
            elapsed,
        )
    finally:
        # Do NOT delete npy files here \u2014 workers have finished (pool context exited),
        # but mmap handles may still be open on macOS. Let staging cleanup handle removal.
        pass

    timings["tile_cut_time_s"] = time.perf_counter() - t_cut_start
    return {
        "saved": summary.get("saved", 0),
        "empty_skipped": summary.get("empty_skipped", 0),
        "errors": summary.get("errors", 0),
        "chunks_written": summary.get("chunks_written", 0),
        "duration_s": round(elapsed, 3),
        "base": summary, # for benchmark to extract detailed stats
        "timings": timings,
    }


def generate_wind_temporal_tiles(
    run_id: str,
    fffs: list[int],
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int | None = None,
    workers: int = 4,
) -> dict:
    """Encode all wind_field frames for *run_id* using keyframe + temporal-delta format.

    Prerequisite: all frame canvases must exist in STAGING_DIR/wind_surface/{run_id}/canvases/
    (written by generate_wind_frame for each fff beforehand).

    Output path: output_dir/{z}/{x}/{y}/f{NNN}.wf[d]

    Call after all hot-zone frames have been warped, e.g.:
        for fff in hot_fffs:
            generate_wind_frame(run_id, fff, ..., base_only=True)   # warp + wind_base only
        generate_wind_temporal_tiles(run_id, hot_fffs, output_dir)  # temporal wind_field
    """
    from .metatile_processor import process_all_wind_temporal_metatiles

    cfg = get_settings()
    z_max = zoom_max if zoom_max is not None else cfg.TILE_ZOOM_EAGER_MAX
    z_max = min(z_max, cfg.TILE_PER_MAP_ZOOM.get("wind_surface", z_max))

    staging_dir = cfg.STAGING_DIR / "wind_surface" / run_id / "canvases"

    npy_u_paths: list[str] = []
    npy_v_paths: list[str] = []
    valid_fffs: list[int] = []

    for fff in fffs:
        npy_u = staging_dir / f"wind_u_{fff:03d}.npy"
        npy_v = staging_dir / f"wind_v_{fff:03d}.npy"
        if npy_u.exists() and npy_v.exists():
            npy_u_paths.append(str(npy_u))
            npy_v_paths.append(str(npy_v))
            valid_fffs.append(fff)
        else:
            _log.warning("wind_temporal: canvas missing for f%03d, skipping", fff)

    if not valid_fffs:
        return {"skipped": True, "reason": "no canvases found"}

    # Derive px_per_meter from first canvas size (all frames share same canvas size)
    first_canvas = np.load(npy_u_paths[0], mmap_mode="r")
    canvas_size = first_canvas.shape[0]
    MERC_HALF = 20037508.3427892
    px_per_meter = canvas_size / (2.0 * MERC_HALF)

    _log.info(
        "wind_temporal %s: encoding %d frames z%d–z%d workers=%d",
        run_id, len(valid_fffs), zoom_min, z_max, workers,
    )
    t0 = time.perf_counter()
    summary = process_all_wind_temporal_metatiles(
        npy_u_paths=npy_u_paths,
        npy_v_paths=npy_v_paths,
        frame_nums=valid_fffs,
        px_per_meter=px_per_meter,
        output_dir=output_dir,
        zoom_min=zoom_min,
        zoom_max=z_max,
        workers=workers,
    )
    elapsed = time.perf_counter() - t0
    _log.info(
        "wind_temporal %s DONE: tiles_saved=%d bytes=%d errors=%d %.1fs",
        run_id, summary["tiles_saved"], summary["bytes"], summary["errors"], elapsed,
    )
    return {**summary, "frames_encoded": len(valid_fffs), "duration_s": round(elapsed, 3)}

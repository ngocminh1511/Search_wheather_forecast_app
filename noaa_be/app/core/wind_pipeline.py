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
) -> dict:
    """
    Generate wind tiles (wind_base PNG + wind_field BIN) for one (run_id, fff) frame.
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

    # 1. Read UGRD, VGRD
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

    # 2. Warp to Mercator
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
    try:
        summary = process_all_wind_metatiles(
            npy_u_path=str(npy_u),
            npy_v_path=str(npy_v),
            npy_speed_path=str(npy_speed),
            px_per_meter=px_m,
            output_dir=output_dir / f"{fff:03d}",
            zoom_min=zoom_min,
            zoom_max=z_max,
            workers=w
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
                "tile_format": cfg.TILE_FORMAT_DEFAULT if prod == "wind_base" else "bin",
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
        try:
            npy_u.unlink(missing_ok=True)
            npy_v.unlink(missing_ok=True)
            npy_speed.unlink(missing_ok=True)
        except Exception:
            pass

    return {
        "saved": summary.get("saved", 0),
        "empty_skipped": summary.get("empty_skipped", 0),
        "errors": summary.get("errors", 0),
        "chunks_written": summary.get("chunks_written", 0),
        "duration_s": round(elapsed, 3),
    }

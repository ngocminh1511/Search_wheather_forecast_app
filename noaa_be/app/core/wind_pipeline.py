from __future__ import annotations

import logging
import time
from pathlib import Path

import numpy as np
from rasterio.warp import Resampling

from ..config import get_settings
from .grib_reader import read_multi_fields
from ..core.tile_cutter import _warp_scalar_to_mercator, cut_and_save_wind, TILE_SIZE

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
    w = workers if workers is not None else cfg.TILE_WORKERS

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

    # 4. Cut and save
    _log.info("wind_pipeline: cutting wind tiles (base PNG + field BIN) ...")
    base_summary = cut_and_save_wind(
        merc_u=merc_u,
        merc_v=merc_v,
        merc_speed=merc_speed,
        px_per_meter=px_m,
        run_id=run_id,
        fff=fff,
        output_dir=output_dir,
        zoom_min=zoom_min,
        zoom_max=z_max,
        workers=w,
        skip_existing=skip_existing,
    )

    elapsed = time.perf_counter() - t0
    _log.info(
        "wind_pipeline %s/f%03d DONE: saved=%d skipped=%d err=%d | %.3fs",
        run_id, fff,
        base_summary.get("saved", 0),
        base_summary.get("skipped", 0),
        base_summary.get("errors", 0),
        elapsed,
    )
    return {
        "saved": base_summary.get("saved", 0) * 2, # x2 because it saves both PNG and BIN
        "skipped": base_summary.get("skipped", 0) * 2,
        "errors": base_summary.get("errors", 0) * 2,
        "base": base_summary,
        "duration_s": round(elapsed, 3),
    }

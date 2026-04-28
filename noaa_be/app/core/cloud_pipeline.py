from __future__ import annotations

"""
cloud_pipeline.py — orchestrate cloud GRIB2 → cloud_total scalar → PNG tiles.

Single entry point: generate_cloud_frame()
  Wraps cloud_reader + cloud_builder + tile_cutter into one callable that is
  API-compatible with tile_generator.generate_frame().

Tile output path:
  output_dir / cloud_total / <run_id> / f<fff> / tcdc_entire_atmosphere / {z}/{x}/{y}.png

This keeps the same path convention used by every other map type so that the
tile router, availability service, and Flutter app require zero changes.
"""

import logging
import time
from pathlib import Path

import numpy as np

from ..config import get_settings
from .cloud_builder import build_total_cloud_cover
from .cloud_reader import read_cloud_fields
from .tile_cutter import _warp_scalar_to_mercator, TILE_SIZE
from .metatile_processor import process_all_metatiles
from rasterio.warp import Resampling

_log = logging.getLogger(__name__)

# Product name used for tile sub-path — matches tile_generator convention
_CLOUD_PRODUCT = "tcdc_entire_atmosphere"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_cloud_frame(
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
    Generate cloud_total PNG tiles for one (run_id, fff) frame.

    Steps:
      1. Read GRIB — detect total vs layered source automatically
      2. Build cloud_total scalar (0–100 %, float32)
      3. Cut tiles via tile_cutter.cut_and_save using map_type="cloud_total"

    Returns a summary dict compatible with tile_generator.generate_run():
      { saved, skipped, errors, duration_s, cloud_source, nan_pct }
    Returns { skipped: True, reason: ... } if no GRIB data is available.
    """
    cfg = get_settings()
    z_max = zoom_max if zoom_max is not None else cfg.TILE_ZOOM_EAGER_MAX
    z_max = min(z_max, cfg.TILE_PER_MAP_ZOOM.get("cloud_total", z_max))
    w = workers if workers is not None else cfg.TILE_PROCESS_WORKERS

    start_ts = time.perf_counter()

    # ── 1. Read GRIB cloud fields ────────────────────────────────────────
    fields = read_cloud_fields(run_id=run_id, fff=fff, data_dir=data_dir)

    if fields.source == "none":
        reason = (
            f"No cloud GRIB available for run={run_id} fff={fff:03d} "
            f"(checked cloud_total and cloud_layered in {data_dir})"
        )
        _log.warning("cloud_pipeline: %s", reason)
        return {"skipped": True, "reason": reason}

    _log.info(
        "cloud_pipeline: run=%s fff=%03d source=%s — building scalar",
        run_id, fff, fields.source,
    )

    # ── 2. Build cloud_total scalar ──────────────────────────────────────
    result = build_total_cloud_cover(fields)

    _log.info(
        "cloud_pipeline: scalar ready  source=%s  "
        "min=%.1f max=%.1f nan_pct=%.1f%%  shape=%s",
        result.source,
        float(np.nanmin(result.values)),
        float(np.nanmax(result.values)),
        result.nan_pct,
        result.values.shape,
    )

    # ── 3. Warp and Cut tiles ────────────────────────────────────────────
    canvas_size = min((2 ** z_max) * TILE_SIZE, 8192)
    merc, px_m = _warp_scalar_to_mercator(
        result.values, result.lat, result.lon, canvas_size,
        resampling=Resampling.bilinear,
    )
    
    staging_dir = cfg.STAGING_DIR / "cloud_total" / run_id / "canvases"
    staging_dir.mkdir(parents=True, exist_ok=True)
    npy_path = staging_dir / f"cloud_{fff:03d}.npy"
    np.save(str(npy_path), merc)
    
    try:
        summary = process_all_metatiles(
            npy_path=str(npy_path),
            px_per_meter=px_m,
            cmap_type="cloud_total",
            cmap_product=None,
            output_dir=output_dir / f"{fff:03d}" / _CLOUD_PRODUCT,
            zoom_min=zoom_min,
            zoom_max=z_max,
            workers=w
        )
    
        # Write manifest
        import json
        manifest = {
            "ready": True,
            "product": _CLOUD_PRODUCT,
            "total": summary["total"],
            "saved": summary["saved"],
            "empty_skipped": summary["empty_skipped"],
            "errors": summary["errors"],
            "chunks_written": summary["chunks_written"],
            "total_size_bytes": summary["bytes"],
            "format": "chunk",
            "tile_format": cfg.TILE_FORMAT_DEFAULT,
            "tile_ext": "webp" if cfg.TILE_FORMAT_DEFAULT == "webp" else "png",
            "timestamp": time.time()
        }
        (output_dir / f"{fff:03d}" / _CLOUD_PRODUCT / "manifest.json").write_text(json.dumps(manifest, indent=2))

        elapsed = time.perf_counter() - start_ts
        summary["duration_s"] = round(elapsed, 3)
        summary["cloud_source"] = result.source
        summary["nan_pct"] = round(result.nan_pct, 2)

        _log.info(
            "cloud_pipeline: done  run=%s fff=%03d source=%s  "
            "saved=%d skipped=%d errors=%d  %.3fs",
            run_id, fff, result.source,
            summary.get("saved", 0),
            summary.get("skipped", 0),
            summary.get("errors", 0),
            elapsed,
        )
    finally:
        try:
            npy_path.unlink(missing_ok=True)
        except Exception:
            pass

    return summary

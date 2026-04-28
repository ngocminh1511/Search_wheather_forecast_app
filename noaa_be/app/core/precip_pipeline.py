from __future__ import annotations

"""
precip_pipeline.py — orchestrate rain_advanced GRIB2 -> advanced_precipitation_base tiles.

Pipeline (matches rain_basic philosophy, 3 zones + 3 palettes):

  Step 1 : Read PRATE, CRAIN, CSNOW from GRIB2.
  Step 2 : Convert PRATE kg/m2/s -> mm/h (done in precip_reader).
  Step 3 : Warp each field separately to a square Mercator canvas:
             - PRATE  : Resampling.bilinear  (smooth continuous intensity)
             - CRAIN  : Resampling.nearest   (preserve binary 0/1 category)
             - CSNOW  : Resampling.nearest   (preserve binary 0/1 category)
  Step 4 : classify_on_mercator() on the Mercator canvases:
             - Apply visual_threshold = 0.10 mm/h
             - Classify: Rain(CRAIN=1,CSNOW=0) / Mixed(CRAIN=1,CSNOW=1) / Snow(CSNOW=1,CRAIN=0)
             - Cleanup: binary_opening + remove small patches
             - Output : combined_index float32 (NaN=dry, 1-18 levels)
  Step 5 : cut_and_save_from_canvas() with cmap_type='precip_base':
             - NEAREST tile upscale (crisp discrete colors, no blending)
             - dry pixels (NaN) -> alpha=0
  Step 6 : (Optional) cut_and_save_fx_mask() for particle FX tiles (unchanged).
  Step 7 : save_debug_pngs().

Output path (unchanged from FE perspective):
  tiles/rain_advanced/{run_id}/{fff:03d}/precip_base/{z}/{x}/{y}.png
"""

import json
import logging
import time
from pathlib import Path

import numpy as np

from ..config import get_settings
from .precip_reader import read_precip_fields
from .precip_classifier import classify_on_mercator
from ..core.tile_cutter import (
    _warp_scalar_to_mercator,
    TILE_SIZE,
)
from ..core.metatile_processor import process_all_adv_precip_metatiles
from .colormap import get_precip_metadata_json
from rasterio.warp import Resampling

_log = logging.getLogger(__name__)


def generate_precip_frame(
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
    Generate static PNG tiles for one (run_id, fff) frame of rain_advanced.

    Base tile pipeline (advanced_precipitation_base):
      - 3 fields warped separately to Mercator (bilinear PRATE, nearest CRAIN/CSNOW)
      - Classification on Mercator canvas -> discrete combined_index 1..18
      - NEAREST tile upscale -> crisp discrete bands
      - Palette: Rain(green->red), Mixed(pink), Snow(cyan->navy) mapped via slots
    """
    cfg = get_settings()
    z_max = zoom_max if zoom_max is not None else cfg.TILE_ZOOM_EAGER_MAX
    z_max = min(z_max, cfg.TILE_PER_MAP_ZOOM.get("rain_advanced", z_max))
    w = workers if workers is not None else cfg.TILE_PROCESS_WORKERS

    grib_path = (
        data_dir / "rain_advanced" / run_id /
        "rain_adv_surface" / f"f{fff:03d}.grib2"
    )

    if not grib_path.exists():
        _log.debug("precip_pipeline: GRIB missing %s", grib_path)
        return {"skipped": True, "reason": f"GRIB missing: {grib_path}"}
    if grib_path.stat().st_size == 0:
        _log.debug("precip_pipeline: empty GRIB %s", grib_path.name)
        return {"skipped": True, "reason": f"GRIB empty: {grib_path.name}"}

    t0 = time.perf_counter()

    # ── Step 1 & 2: Read fields (PRATE already in mm/h from reader) ───────────
    try:
        fields = read_precip_fields(grib_path)
    except Exception as exc:
        _log.error("precip_pipeline: read failed %s: %s", grib_path.name, exc)
        return {"skipped": True, "reason": f"read error: {exc}"}

    prate_mmh = fields.prate   # float32 H×W mm/h
    crain_raw  = fields.crain  # float32 H×W 0/1
    csnow_raw  = fields.csnow  # float32 H×W 0/1

    _log.info(
        "precip_pipeline %s/f%03d: prate_max=%.3f mm/h  "
        "crain_active=%.1f%%  csnow_active=%.1f%%",
        run_id, fff,
        float(np.nanmax(prate_mmh)),
        100.0 * (crain_raw >= 0.5).sum() / crain_raw.size,
        100.0 * (csnow_raw >= 0.5).sum() / csnow_raw.size,
    )

    # ── Step 3: Warp each field to Mercator canvas ─────────────────────────────
    # Canvas size matches zoom_max so tile cutting has pixel-level accuracy.
    canvas_size = min((2 ** z_max) * TILE_SIZE, 8192)

    _log.info("precip_pipeline: warping 6 fields to %dx%d Mercator canvas ...",
              canvas_size, canvas_size)

    merc_prate, px_m = _warp_scalar_to_mercator(
        prate_mmh, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,   # smooth intensity
    )
    merc_crain, _ = _warp_scalar_to_mercator(
        crain_raw, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,   # soft transition for boundaries
    )
    merc_csnow, _ = _warp_scalar_to_mercator(
        csnow_raw, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,   # soft transition for boundaries
    )
    merc_cicep, _ = _warp_scalar_to_mercator(
        fields.cicep, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,
    )
    merc_cfrzr, _ = _warp_scalar_to_mercator(
        fields.cfrzr, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,
    )
    merc_cpofp, _ = _warp_scalar_to_mercator(
        fields.cpofp, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,
    )

    combined_index_merc = None
    if cfg.WRITE_DEBUG_PNGS:
        # Only build this full-frame debug artifact when explicitly requested.
        min_patch = max(20, canvas_size // 200)
        combined_index_merc = classify_on_mercator(
            merc_prate, merc_crain, merc_csnow, merc_cicep, merc_cfrzr, merc_cpofp,
            min_patch_pixels=min_patch,
        )

    # Save to npy for IPC
    staging_dir = cfg.STAGING_DIR / "rain_advanced" / run_id / "canvases"
    staging_dir.mkdir(parents=True, exist_ok=True)
    
    npy_paths = {}
    for name, arr in [("prate", merc_prate), ("crain", merc_crain), ("csnow", merc_csnow), 
                      ("cicep", merc_cicep), ("cfrzr", merc_cfrzr), ("cpofp", merc_cpofp)]:
        path = staging_dir / f"precip_{name}_{fff:03d}.npy"
        np.save(str(path), arr)
        npy_paths[name] = str(path)

    # ── Step 5: Cut base tiles (SSAA) using metatile processor ───────────────
    _log.info("precip_pipeline: processing adv precip metatiles (precip_base) ...")
    try:
        base_summary = process_all_adv_precip_metatiles(
            npy_paths=npy_paths,
            px_per_meter=px_m,
            output_dir=output_dir / f"{fff:03d}" / "precip_base",
            zoom_min=zoom_min,
            zoom_max=z_max,
            workers=w
        )
        
        # Write manifest
        import json
        manifest = {
            "ready": True,
            "product": "precip_base",
            "total": base_summary["total"],
            "saved": base_summary["saved"],
            "empty_skipped": base_summary["empty_skipped"],
            "errors": base_summary["errors"],
            "chunks_written": base_summary["chunks_written"],
            "total_size_bytes": base_summary["bytes"],
            "format": "chunk",
            "tile_format": cfg.TILE_FORMAT_DEFAULT,
            "tile_ext": "webp" if cfg.TILE_FORMAT_DEFAULT == "webp" else "png",
            "timestamp": time.time()
        }
        (output_dir / f"{fff:03d}" / "precip_base" / "manifest.json").write_text(json.dumps(manifest, indent=2))

        # ── Step 6: Generate Metadata JSON ─────────────────────────────────────────
        meta_dir = output_dir / f"{fff:03d}" / "precip_base_meta"
        meta_dir.mkdir(parents=True, exist_ok=True)
        meta_path = meta_dir / "meta.json"
        
        meta_data = get_precip_metadata_json()
        meta_path.write_text(json.dumps(meta_data, indent=2))
        _log.info("precip_pipeline: wrote metadata to %s", meta_path)



        # ── Step 7: Debug PNGs ────────────────────────────────────────────────────
        if cfg.WRITE_DEBUG_PNGS and combined_index_merc is not None:
            try:
                _save_debug_pngs_adv(
                    combined_index_merc=combined_index_merc,
                    merc_prate=merc_prate,
                    merc_crain=merc_crain,
                    merc_csnow=merc_csnow,
                    merc_cicep=merc_cicep,
                    merc_cfrzr=merc_cfrzr,
                    output_dir=output_dir,
                    fff=fff,
                )
            except Exception as dbg_exc:
                _log.warning("precip_pipeline: debug PNGs failed (non-fatal): %s", dbg_exc)

        elapsed = time.perf_counter() - t0
        _log.info(
            "precip_pipeline %s/f%03d DONE: base(saved=%d skipped=%d err=%d) | %.3fs",
            run_id, fff,
            base_summary.get("saved", 0),
            base_summary.get("skipped", 0),
            base_summary.get("errors", 0),
            elapsed,
        )
    finally:
        for path in npy_paths.values():
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass

    return {
        "base":     base_summary,
        "duration_s": round(elapsed, 3),
    }


# ---------------------------------------------------------------------------
# Debug helpers
# ---------------------------------------------------------------------------

def _save_debug_pngs_adv(
    combined_index_merc: np.ndarray,
    merc_prate: np.ndarray,
    merc_crain: np.ndarray,
    merc_csnow: np.ndarray,
    merc_cicep: np.ndarray,
    merc_cfrzr: np.ndarray,
    output_dir: Path,
    fff: int,
) -> None:
    """
    Save flat equirectangular debug PNGs for advanced precipitation.
    Outputs to {output_dir}/{fff:03d}/debug/:
      ptype_debug.png          — zone map: rain=blue, mixed=purple, snow=white, dry=black
      level_debug.png          — intensity level 1..6 per active pixel (grayscale)
      combined_index_debug.png — full RGBA render (same as tile colormap)
      *_prate_debug.png        — grayscale intensity per zone
    """
    from PIL import Image
    from .colormap import apply_colormap

    debug_dir = output_dir / f"{fff:03d}" / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)

    ci = combined_index_merc  # float32, NaN=dry, 1-18 levels
    valid = np.isfinite(ci) & (ci >= 1)
    
    mask_rain = valid & (ci >= 1) & (ci <= 6)
    mask_mixed = valid & (ci >= 7) & (ci <= 12)
    mask_snow = valid & (ci >= 13) & (ci <= 18)

    # ── A. ptype_debug: zone map (rain=1, mixed=2, snow=3, dry=0) ─────────────
    ptype_float = np.full(ci.shape, np.nan, dtype=np.float32)
    ptype_float[mask_rain]  = 1.0   # rain
    ptype_float[mask_mixed] = 2.0   # mixed
    ptype_float[mask_snow]  = 3.0   # snow
    ptype_arr = np.nan_to_num(ptype_float, nan=0.0)
    ptype_rgba = apply_colormap(ptype_arr.astype(np.float32), "precip_debug_ptype")
    Image.fromarray(ptype_rgba, "RGBA").save(debug_dir / "ptype_debug.png")

    n_rain  = int(mask_rain.sum())
    n_mixed = int(mask_mixed.sum())
    n_snow  = int(mask_snow.sum())
    n_dry   = int(ci.size - n_rain - n_mixed - n_snow)
    _log.info(
        "debug ptype_debug.png: rain=%d mixed=%d snow=%d dry=%d",
        n_rain, n_mixed, n_snow, n_dry,
    )

    # ── B. level_debug: intensity level 1..6 (grayscale) ─────────────────────
    level_float = np.full(ci.shape, 0.0, dtype=np.float32)
    level_float[valid] = ((ci[valid].astype(int) - 1) % 6 + 1) / 6.0
    level_uint8 = (level_float * 255).astype(np.uint8)
    Image.fromarray(level_uint8, "L").save(debug_dir / "level_debug.png")

    # ── C. combined_index_debug: full RGBA render (same as tile colormap) ────
    ci_rgba = apply_colormap(
        np.nan_to_num(ci, nan=0.0).astype(np.float32),
        "precip_base",
    )
    # Force alpha=220+ on non-dry pixels so debug image is always visible
    ci_rgba[valid, 3] = np.maximum(ci_rgba[valid, 3], 220)
    Image.fromarray(ci_rgba, "RGBA").save(debug_dir / "combined_index_debug.png")

    # ── D. Per-zone prate heatmaps (grayscale, 50 mm/h → 255) ─────────────────
    _MAX_MMPH = 50.0
    rain_arr  = np.where(mask_rain,  merc_prate, 0.0)
    mixed_arr = np.where(mask_mixed, merc_prate, 0.0)
    snow_arr  = np.where(mask_snow,  merc_prate, 0.0)
    for name, arr in [("rain", rain_arr), ("mixed", mixed_arr), ("snow", snow_arr)]:
        ch = np.clip(arr / _MAX_MMPH, 0.0, 1.0)
        Image.fromarray((ch * 255).astype(np.uint8), "L").save(
            debug_dir / f"{name}_prate_debug.png"
        )

    _log.info("debug PNGs saved -> %s", debug_dir)

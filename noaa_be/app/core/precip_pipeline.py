from __future__ import annotations

"""
precip_pipeline.py — orchestrate rain_advanced GRIB2 -> precip_base tiles.

Pipeline (classify ONCE on full Mercator canvas):

  Step 1 : Read PRATE, CRAIN, CSNOW, CICEP, CFRZR, CPOFP from GRIB2.
  Step 2 : Convert PRATE kg/m2/s -> mm/h (done in precip_reader).
  Step 3 : Warp each field to a square Mercator canvas (bilinear for all).
  Step 4 : classify_on_mercator() ONCE on the full Mercator canvas:
             - Apply visual_threshold = 0.10 mm/h
             - Classify: Rain/Mixed/Snow, binary_opening + remove small patches
             - Output : combined_index float32 (NaN=dry, 1-18 levels)
             - Save as combined_index.npy (IPC to tile workers)
  Step 5 : process_all_precip_classified_metatiles() — workers slice combined_index,
             upsample with NEAREST, colorize. No per-worker classification.
             Speedup: old ~1272s colorize → new ~40-60s (eliminate classify×1368 calls).

Output paths:
  tiles/rain_advanced/{run_id}/{fff:03d}/precip_base/{z}/{x}/{y}.webp
  tiles/rain_advanced/{run_id}/{hours:03d}_{minutes:02d}/precip_base/...  (interp frames)

Interpolation (15-min frames for "3h forward from NOW", sliding):
  generate_precip_interp_frames() reads f006..f015 (10 anchor frames) and produces
  27 sub-frames (006_15 to 014_45 at 15-min intervals) covering all "now" ∈ [f006,f012].
"""

import json
import logging
import time
from pathlib import Path

import numpy as np

from ..config import get_settings
from .precip_reader import read_precip_fields, PrecipFields
from .precip_classifier import classify_on_mercator
from ..core.tile_cutter import (
    _warp_scalar_to_mercator,
    TILE_SIZE,
)
from ..core.metatile_processor import process_all_precip_classified_metatiles
from .colormap import get_precip_metadata_json
from .interpolator import generate_rain_advanced_interp_frames
from rasterio.warp import Resampling

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _warp_precip_fields(
    fields: PrecipFields,
    canvas_size: int,
) -> tuple[dict[str, np.ndarray], float]:
    """Warp all 6 precipitation fields to a square Mercator canvas."""
    merc_prate, px_m = _warp_scalar_to_mercator(
        fields.prate, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,
    )
    merc_crain, _ = _warp_scalar_to_mercator(
        fields.crain, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,
    )
    merc_csnow, _ = _warp_scalar_to_mercator(
        fields.csnow, fields.lat, fields.lon, canvas_size,
        resampling=Resampling.bilinear,
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
    return {
        "prate": merc_prate,
        "crain": merc_crain,
        "csnow": merc_csnow,
        "cicep": merc_cicep,
        "cfrzr": merc_cfrzr,
        "cpofp": merc_cpofp,
    }, px_m


def _classify_and_tile(
    fields: PrecipFields,
    canvas_size: int,
    staging_dir: Path,
    frame_key: str,
    output_dir: Path,
    zoom_min: int,
    zoom_max: int,
    workers: int,
) -> dict:
    """Classify ONCE on full canvas, then tile with classified workers.

    Returns summary dict from process_all_precip_classified_metatiles.
    """
    cfg = get_settings()
    min_patch = max(20, canvas_size // 200)

    t_warp = time.perf_counter()
    mercs, px_m = _warp_precip_fields(fields, canvas_size)
    warp_time = time.perf_counter() - t_warp

    # The core bottleneck fix: classify ONCE on the full canvas
    t_classify = time.perf_counter()
    combined_index = classify_on_mercator(
        mercs["prate"], mercs["crain"], mercs["csnow"],
        mercs["cicep"], mercs["cfrzr"], mercs["cpofp"],
        min_patch_pixels=min_patch,
        verbose=True,
    )
    classify_time = time.perf_counter() - t_classify

    _log.info(
        "precip_pipeline frame %s: warp=%.2fs  classify=%.2fs  "
        "canvas=%dx%d  prate_max=%.3f mm/h",
        frame_key, warp_time, classify_time,
        canvas_size, canvas_size,
        float(np.nanmax(mercs["prate"])),
    )

    staging_dir.mkdir(parents=True, exist_ok=True)
    npy_combined = staging_dir / f"precip_combined_{frame_key}.npy"
    np.save(str(npy_combined), combined_index)

    if cfg.WRITE_DEBUG_PNGS:
        try:
            _save_debug_pngs_adv(
                combined_index_merc=combined_index,
                merc_prate=mercs["prate"],
                merc_crain=mercs["crain"],
                merc_csnow=mercs["csnow"],
                merc_cicep=mercs["cicep"],
                merc_cfrzr=mercs["cfrzr"],
                output_dir=output_dir.parent,
                frame_key=frame_key,
            )
        except Exception as dbg_exc:
            _log.warning("precip_pipeline: debug PNGs failed (non-fatal): %s", dbg_exc)

    precip_base_dir = output_dir / "precip_base"
    base_summary = process_all_precip_classified_metatiles(
        combined_npy_path=str(npy_combined),
        px_per_meter=px_m,
        output_dir=precip_base_dir,
        zoom_min=zoom_min,
        zoom_max=zoom_max,
        workers=workers,
    )
    base_summary["warp_time_s"] = round(warp_time, 3)
    base_summary["classify_time_s"] = round(classify_time, 3)
    return base_summary


def _write_manifest(output_dir: Path, summary: dict) -> None:
    cfg = get_settings()
    manifest = {
        "ready": True,
        "product": "precip_base",
        "total": summary.get("total", 0),
        "saved": summary.get("saved", 0),
        "empty_skipped": summary.get("empty_skipped", 0),
        "errors": summary.get("errors", 0),
        "chunks_written": summary.get("chunks_written", 0),
        "total_size_bytes": summary.get("bytes", 0),
        "format": "chunk",
        "tile_format": cfg.TILE_FORMAT_DEFAULT,
        "tile_ext": "webp" if cfg.TILE_FORMAT_DEFAULT == "webp" else "png",
        "timestamp": time.time(),
    }
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))


def _write_precip_meta(output_dir: Path) -> None:
    meta_dir = output_dir / "precip_base_meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    meta_data = get_precip_metadata_json()
    (meta_dir / "meta.json").write_text(json.dumps(meta_data, indent=2))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_precip_frame(
    run_id: str,
    fff: int,
    data_dir: Path,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int | None = None,
    workers: int | None = None,
    skip_existing: bool = True,
    base_only: bool = False,
) -> dict:
    """Generate precip_base tiles for one (run_id, fff) frame of rain_advanced.

    Uses classify-once-on-full-canvas:
      1. Warp 6 fields to Mercator
      2. Classify ONCE → combined_index.npy  (~5-15s)
      3. Workers slice+colorize only         (~40-60s)

    vs. old per-metatile classify: ~1272s classify + ~320s rest ≈ 1600s/frame
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

    try:
        fields = read_precip_fields(grib_path)
    except Exception as exc:
        _log.error("precip_pipeline: read failed %s: %s", grib_path.name, exc)
        return {"skipped": True, "reason": f"read error: {exc}"}

    _log.info(
        "precip_pipeline %s/f%03d: prate_max=%.3f mm/h  "
        "crain_active=%.1f%%  csnow_active=%.1f%%",
        run_id, fff,
        float(np.nanmax(fields.prate)),
        100.0 * (fields.crain >= 0.5).sum() / fields.crain.size,
        100.0 * (fields.csnow >= 0.5).sum() / fields.csnow.size,
    )

    canvas_size = min((2 ** z_max) * TILE_SIZE, 8192)
    staging_dir = cfg.STAGING_DIR / "rain_advanced" / run_id / "canvases"
    frame_output = output_dir / f"{fff:03d}"

    base_summary = _classify_and_tile(
        fields=fields,
        canvas_size=canvas_size,
        staging_dir=staging_dir,
        frame_key=f"{fff:03d}",
        output_dir=frame_output,
        zoom_min=zoom_min,
        zoom_max=z_max,
        workers=w,
    )

    _write_manifest(frame_output / "precip_base", base_summary)
    _write_precip_meta(frame_output)

    elapsed = time.perf_counter() - t0
    _log.info(
        "precip_pipeline %s/f%03d DONE: saved=%d skipped=%d err=%d | %.3fs",
        run_id, fff,
        base_summary.get("saved", 0),
        base_summary.get("empty_skipped", 0),
        base_summary.get("errors", 0),
        elapsed,
    )

    return {
        "base": base_summary,
        "duration_s": round(elapsed, 3),
        "timings": {
            "warp_time_s": base_summary.get("warp_time_s", 0.0),
            "classify_time_s": base_summary.get("classify_time_s", 0.0),
            "tile_cut_time_s": base_summary.get("duration_s", 0.0),
        },
    }


def generate_precip_interp_frames(
    run_id: str,
    data_dir: Path,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int | None = None,
    workers: int | None = None,
    anchor_start: int = 6,
    anchor_end: int = 15,
) -> dict:
    """Generate 15-min interpolated rain_advanced frames for "3h forward from NOW".

    Per user spec: at any "now" ∈ [f_live=f006, f_next_live=f012], user must see
    11-frame 15-min interp covering [now, now+3h]. Pipeline stores superset = 27
    sub-frames covering [f006:15, f014:45]; frontend picks 11 per "now" view.

    Reads anchor frames f{anchor_start}..f{anchor_end} (default f006..f015 = 10 frames)
    and produces 27 interp sub-frames at 15-min intervals.

    Tile output: output_dir/{hours:03d}_{minutes:02d}/precip_base/
                 (e.g. 006_15, 006_30, 006_45, 007_15, ..., 014_45)
    """
    cfg = get_settings()
    z_max = zoom_max if zoom_max is not None else cfg.TILE_ZOOM_EAGER_MAX
    z_max = min(z_max, cfg.TILE_PER_MAP_ZOOM.get("rain_advanced", z_max))
    w = workers if workers is not None else cfg.TILE_PROCESS_WORKERS

    grib_dir = data_dir / "rain_advanced" / run_id / "rain_adv_surface"
    staging_dir = cfg.STAGING_DIR / "rain_advanced" / run_id / "canvases"
    canvas_size = min((2 ** z_max) * TILE_SIZE, 8192)

    t0 = time.perf_counter()
    source_fields: dict[int, PrecipFields] = {}

    for fff in range(anchor_start, anchor_end + 1):
        grib_path = grib_dir / f"f{fff:03d}.grib2"
        if not grib_path.exists() or grib_path.stat().st_size == 0:
            _log.warning(
                "precip_pipeline interp: source GRIB missing for f%03d, skipping interpolation", fff
            )
            return {"skipped": True, "reason": f"Source GRIB missing: f{fff:03d}"}
        try:
            source_fields[fff] = read_precip_fields(grib_path)
        except Exception as exc:
            _log.error("precip_pipeline interp: read failed f%03d: %s", fff, exc)
            return {"skipped": True, "reason": f"read error f{fff:03d}: {exc}"}

    interp_frames = generate_rain_advanced_interp_frames(
        sources=source_fields,
        anchor_start=anchor_start,
        anchor_end=anchor_end,
    )

    totals: dict = {
        "frames_generated": 0,
        "total_tiles_saved": 0,
        "total_errors": 0,
        "duration_s": 0.0,
        "frames": [],
    }

    for frame in interp_frames:
        frame_t0 = time.perf_counter()
        frame_output = output_dir / frame.path_name

        base_summary = _classify_and_tile(
            fields=frame.fields,
            canvas_size=canvas_size,
            staging_dir=staging_dir,
            frame_key=frame.path_name,
            output_dir=frame_output,
            zoom_min=zoom_min,
            zoom_max=z_max,
            workers=w,
        )

        _write_manifest(frame_output / "precip_base", base_summary)
        _write_precip_meta(frame_output)

        frame_elapsed = time.perf_counter() - frame_t0
        _log.info(
            "precip_pipeline interp %s/%s DONE: saved=%d | %.3fs",
            run_id, frame.path_name,
            base_summary.get("saved", 0),
            frame_elapsed,
        )

        totals["frames_generated"] += 1
        totals["total_tiles_saved"] += base_summary.get("saved", 0)
        totals["total_errors"] += base_summary.get("errors", 0)
        totals["frames"].append({
            "frame": frame.path_name,
            "saved": base_summary.get("saved", 0),
            "duration_s": round(frame_elapsed, 3),
        })

    totals["duration_s"] = round(time.perf_counter() - t0, 3)
    _log.info(
        "precip_pipeline interp %s: %d frames in %.3fs",
        run_id, totals["frames_generated"], totals["duration_s"],
    )
    return totals


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
    frame_key: str,
) -> None:
    """Save flat equirectangular debug PNGs for advanced precipitation."""
    from PIL import Image
    from .colormap import apply_colormap

    debug_dir = output_dir / frame_key / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)

    ci = combined_index_merc
    valid      = np.isfinite(ci) & (ci >= 1)
    mask_rain  = valid & (ci >= 1)  & (ci <= 6)
    mask_mixed = valid & (ci >= 7)  & (ci <= 12)
    mask_snow  = valid & (ci >= 13) & (ci <= 18)

    ptype_float = np.full(ci.shape, np.nan, dtype=np.float32)
    ptype_float[mask_rain]  = 1.0
    ptype_float[mask_mixed] = 2.0
    ptype_float[mask_snow]  = 3.0
    ptype_rgba = apply_colormap(np.nan_to_num(ptype_float, nan=0.0), "precip_debug_ptype")
    Image.fromarray(ptype_rgba, "RGBA").save(debug_dir / "ptype_debug.png")

    level_float = np.full(ci.shape, 0.0, dtype=np.float32)
    level_float[valid] = ((ci[valid].astype(int) - 1) % 6 + 1) / 6.0
    Image.fromarray((level_float * 255).astype(np.uint8), "L").save(debug_dir / "level_debug.png")

    ci_rgba = apply_colormap(np.nan_to_num(ci, nan=0.0).astype(np.float32), "precip_base")
    ci_rgba[valid, 3] = np.maximum(ci_rgba[valid, 3], 220)
    Image.fromarray(ci_rgba, "RGBA").save(debug_dir / "combined_index_debug.png")

    _MAX_MMPH = 50.0
    for name, mask in [("rain", mask_rain), ("mixed", mask_mixed), ("snow", mask_snow)]:
        arr = np.where(mask, merc_prate, 0.0)
        ch = np.clip(arr / _MAX_MMPH, 0.0, 1.0)
        Image.fromarray((ch * 255).astype(np.uint8), "L").save(debug_dir / f"{name}_prate_debug.png")

    _log.info("debug PNGs saved -> %s", debug_dir)

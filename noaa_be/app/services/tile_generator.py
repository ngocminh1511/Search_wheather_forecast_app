from __future__ import annotations

"""
tile_generator.py — orchestrate GRIB2 → RGBA → PNG tiles for one (map_type, run_id, fff).

GRIB2 path convention (noaa_be internal):
  data/<map_type>/<run_id>/<product_name>/f<fff:03d>.grib2
  e.g. data/rain_basic/20260406_00z/apcp_surface/f000.grib2

Strategy:
  - Eager: z=0..TILE_ZOOM_EAGER_MAX saved to staging dir then atomic-swapped to live
  - Lazy:  z=TILE_ZOOM_EAGER_MAX+1..10 generated on demand and cached
  - cloud_layered: three products (low_cloud, mid_cloud, high_cloud) per frame
"""

import logging
import shutil
from pathlib import Path

import numpy as np

from ..config import get_settings
from ..core.colormap import apply_colormap
from ..core.grib_reader import GribField, read_first_field, read_multi_fields
from ..core.map_specs import MAP_SPECS
from ..core.tile_cutter import cut_and_save, get_lazy_tile

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Map-type → tile product mapping
# (product name == MAP_SPECS Product.name == sub-folder under run_id/)
# ---------------------------------------------------------------------------

# Products we generate PNG tiles for (wind_animation is JSON-only)
# Each entry maps:  map_type → [product_name, ...]
# product_name also doubles as the GRIB file sub-folder name.
_MAP_PRODUCTS: dict[str, list[str]] = {
    "temperature_feels_like": ["tmp_2m"],
    "rain_basic":             ["apcp_surface"],
    "rain_advanced":          ["rain_adv_surface"],
    "cloud_total":            ["tcdc_entire_atmosphere"],
    "cloud_layered":          ["low_cloud", "mid_cloud", "high_cloud"],
    "snow_depth":             ["snod_surface"],
}

# GRIB short names expected inside each product's GRIB2 file
_PRODUCT_SHORT_NAMES: dict[str, list[str]] = {
    "tmp_2m":                 ["2t", "t2m", "t"],
    "apcp_surface":           ["tp", "apcp"],
    "rain_adv_surface":       ["prate", "crain", "csnow"],
    "tcdc_entire_atmosphere": ["tcc", "tcdc"],
    "low_cloud":              ["lcc", "lcdc"],
    "mid_cloud":              ["mcc", "mcdc"],
    "high_cloud":             ["hcc", "hcdc"],
    "snod_surface":           ["sde", "snod"],
}

# colormap product key → one of _PRODUCT_SHORT_NAMES
# (the first short_name found in the file is used for RGBA)
_COLORMAP_PRODUCT: dict[str, str] = {
    "tmp_2m":                 "temperature_feels_like",
    "apcp_surface":           "rain_basic",
    "rain_adv_surface":       "rain_advanced",
    "tcdc_entire_atmosphere": "cloud_total",
    "low_cloud":              "cloud_layered",
    "mid_cloud":              "cloud_layered",
    "high_cloud":             "cloud_layered",
    "snod_surface":           "snow_depth",
}

_COLORMAP_PRODUCT_ARG: dict[str, str | None] = {
    "tmp_2m":                 None,
    "apcp_surface":           None,
    "rain_adv_surface":       None,
    "tcdc_entire_atmosphere": None,
    "low_cloud":              "low_cloud",
    "mid_cloud":              "mid_cloud",
    "high_cloud":             "high_cloud",
    "snod_surface":           None,
}


# Extra filter keys needed for products whose GRIB file has multiple stepTypes
_PRODUCT_EXTRA_FILTERS: dict[str, dict] = {
    "rain_adv_surface": {"stepType": "instant"},
}


# ---------------------------------------------------------------------------
# GRIB loading helpers
# ---------------------------------------------------------------------------

def _load_primary_field(file_path: Path, product_name: str) -> GribField:
    """Load the first available GRIB short name for this product."""
    candidates = _PRODUCT_SHORT_NAMES.get(product_name, [])
    extra = _PRODUCT_EXTRA_FILTERS.get(product_name, {})
    last_exc: Exception | None = None
    for sn in candidates:
        try:
            return read_first_field(file_path, filter_by_keys={"shortName": sn, **extra})
        except Exception as exc:
            last_exc = exc
    # fallback: open without filter
    try:
        return read_first_field(file_path)
    except Exception:
        raise ValueError(
            f"Cannot read GRIB field from {file_path.name} for product={product_name!r}"
        ) from last_exc


# ---------------------------------------------------------------------------
# Single-frame tile generation
# ---------------------------------------------------------------------------

def generate_frame(
    map_type: str,
    run_id: str,
    fff: int,
    product_name: str,
    data_dir: Path,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int | None = None,
    workers: int | None = None,
    skip_existing: bool = True,
) -> dict:
    """
    Generate PNG tiles for one (map_type, run_id, fff, product_name) frame.

    GRIB2 read from: data_dir/<map_type>/<run_id>/<product_name>/f<fff:03d>.grib2
    Tiles saved to:  output_dir/<map_type>/<run_id>/f<fff:03d>/<product_name>/{z}/{x}/{y}.png
    """
    cfg = get_settings()
    z_max = zoom_max if zoom_max is not None else cfg.TILE_ZOOM_EAGER_MAX
    w = workers if workers is not None else cfg.TILE_WORKERS

    grib_file = data_dir / map_type / run_id / product_name / f"f{fff:03d}.grib2"
    if not grib_file.exists():
        return {"skipped": True, "reason": f"GRIB missing: {grib_file}"}

    field = _load_primary_field(grib_file, product_name)
    cmap_type = _COLORMAP_PRODUCT.get(product_name, map_type)
    cmap_product = _COLORMAP_PRODUCT_ARG.get(product_name)
    rgba = apply_colormap(field.values, cmap_type, product=cmap_product)

    summary = cut_and_save(
        rgba=rgba,
        map_type=map_type,
        run_id=run_id,
        fff=fff,
        product=product_name,
        output_dir=output_dir,
        zoom_min=zoom_min,
        zoom_max=z_max,
        workers=w,
        skip_existing=skip_existing,
    )
    log.info(
        "Tiles %s/%s/f%03d/%s: saved=%d skipped=%d errors=%d",
        map_type, run_id, fff, product_name,
        summary["saved"], summary["skipped"], summary["errors"],
    )
    return summary


# ---------------------------------------------------------------------------
# Full-run tile generation (staging → atomic swap)
# ---------------------------------------------------------------------------

def generate_run(
    map_type: str,
    run_id: str,
    fff_values: list[int],
    data_dir: Path | None = None,
) -> dict:
    """
    Generate tiles for all (fff, product) combinations of a run.
    Writes to staging, then atomically swaps to live tiles dir.
    """
    cfg = get_settings()
    d_dir = data_dir or cfg.DATA_DIR

    if map_type in cfg.JSON_ONLY_MAP_TYPES:
        return {"skipped_json_only": True, "map_type": map_type, "run_id": run_id}

    products = _MAP_PRODUCTS.get(map_type, [])
    staging = cfg.STAGING_DIR / map_type / run_id
    live = cfg.TILES_DIR / map_type / run_id
    staging.mkdir(parents=True, exist_ok=True)

    # Progress tracking (import lazily so tile_generator works standalone too)
    try:
        from . import progress_tracker as _pt
    except ImportError:
        _pt = None  # type: ignore

    total_fp = len(fff_values) * max(len(products), 1)
    frames_done = 0
    total_tiles_saved = 0
    total_tiles_skipped = 0
    if _pt:
        _pt.update(map_type, frames_total=total_fp, frames_done=0, tiles_saved=0, tiles_skipped=0)

    all_results: dict = {}
    for fff in sorted(fff_values):
        all_results[fff] = {}
        for product_name in products:
            if _pt:
                _pt.update(
                    map_type,
                    current_fff=fff,
                    current_product=product_name,
                    step_detail=f"f{fff:03d} / {product_name}",
                )
            result = generate_frame(
                map_type=map_type,
                run_id=run_id,
                fff=fff,
                product_name=product_name,
                data_dir=d_dir,
                output_dir=staging,
            )
            all_results[fff][product_name] = result
            frames_done += 1
            if isinstance(result, dict) and "saved" in result:
                total_tiles_saved += result.get("saved", 0)
                total_tiles_skipped += result.get("skipped", 0)
            if _pt:
                _pt.update(
                    map_type,
                    frames_done=frames_done,
                    tiles_saved=total_tiles_saved,
                    tiles_skipped=total_tiles_skipped,
                )

    # Atomic swap: rename staging → live
    live.parent.mkdir(parents=True, exist_ok=True)
    old_live = live.with_name(live.name + ".old")
    if live.exists():
        live.rename(old_live)
    staging.rename(live)
    if old_live.exists():
        shutil.rmtree(old_live, ignore_errors=True)

    log.info("Atomic swap complete: %s/%s now live", map_type, run_id)
    return {"map_type": map_type, "run_id": run_id, "frames": all_results}


# ---------------------------------------------------------------------------
# Lazy tile generation (on-demand for z > TILE_ZOOM_EAGER_MAX)
# ---------------------------------------------------------------------------

def get_tile_lazy(
    map_type: str,
    run_id: str,
    fff: int,
    product_name: str,
    z: int,
    x: int,
    y: int,
    tiles_dir: Path | None = None,
    data_dir: Path | None = None,
) -> bytes | None:
    """
    Serve a tile for high-zoom: check disk cache first, then generate from GRIB2.
    Returns None if source GRIB2 is unavailable.
    """
    cfg = get_settings()
    t_dir = tiles_dir or cfg.TILES_DIR
    d_dir = data_dir or cfg.DATA_DIR

    tile_path = t_dir / map_type / run_id / f"f{fff:03d}" / product_name / str(z) / str(x) / f"{y}.png"
    if tile_path.exists():
        return tile_path.read_bytes()

    grib_file = d_dir / map_type / run_id / product_name / f"f{fff:03d}.grib2"
    if not grib_file.exists():
        return None

    field = _load_primary_field(grib_file, product_name)
    cmap_type = _COLORMAP_PRODUCT.get(product_name, map_type)
    cmap_product = _COLORMAP_PRODUCT_ARG.get(product_name)
    rgba = apply_colormap(field.values, cmap_type, product=cmap_product)

    png_bytes = get_lazy_tile(rgba, z, x, y)
    tile_path.parent.mkdir(parents=True, exist_ok=True)
    tile_path.write_bytes(png_bytes)
    return png_bytes

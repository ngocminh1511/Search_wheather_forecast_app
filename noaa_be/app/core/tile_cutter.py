from __future__ import annotations

"""
tile_cutter.py — scalar weather grid → Web Mercator XYZ PNG tiles.

Correct pipeline per Web Mercator / slippy-map standard:
  1. Receive scalar float32 ndarray (equirectangular, EPSG:4326,
     lat descending 90→-90, lon 0→360).
  2. Reproject scalar → square EPSG:3857 Mercator canvas via rasterio.warp.
     Warping happens on the RAW SCALAR data — never on colorized RGBA.
  3. Per tile (z/x/y): mercantile.xy_bounds() → pixel window on Mercator canvas
     → LANCZOS upscale scalar float to TILE_SIZE → colorize → RGBA uint8.
  4. Encode as transparent PNG (NaN cells → alpha=0).

Key invariant: colorization is applied AFTER reprojection, per tile.
"""

import io
import logging
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import mercantile
import numpy as np
import rasterio
from PIL import Image
from rasterio.crs import CRS
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds as _affine_from_bounds
from rasterio.warp import Resampling

_log = logging.getLogger(__name__)
from rasterio.warp import reproject as _warp_reproject

from .colormap import apply_colormap

TILE_SIZE = 256  # px

# EPSG:3857 half-extent in metres — covers exactly lat ±85.051129°
_MERC_HALF: float = 20037508.3427892

# Stepped INTEGER colormaps must use NEAREST upscaling to prevent cross-type blending.
_NEAREST_CMAP_TYPES: frozenset[str] = frozenset({
    "precip_debug_ptype",
    "advanced_precipitation_base",  # discrete combined_index 0..18 — bands must stay crisp
    "precip_base",                  # alias for advanced_precipitation_base
    "rain_advanced",                # alias
})


# ---------------------------------------------------------------------------
# Scalar reprojection: equirectangular EPSG:4326 → square EPSG:3857 canvas
# ---------------------------------------------------------------------------

def _warp_scalar_to_mercator(
    scalar: np.ndarray,   # (H, W) float32, lat descending 90→-90, lon 0→360
    lat: np.ndarray,      # (H,) degrees, descending
    lon: np.ndarray,      # (W,) degrees, 0→360
    canvas_size: int,
    resampling: Resampling = Resampling.bilinear,
) -> tuple[np.ndarray, float]:
    """
    Reproject a scalar equirectangular grid to a square EPSG:3857 canvas.

    Returns:
        merc_canvas  : float32 ndarray (canvas_size × canvas_size).
                       Row 0 = north (+85.05°), last row = south (−85.05°).
                       NaN where source data is absent.
        px_per_meter : canvas pixels per metre (identical in X and Y).
    """
    # Roll lon 0→360 → −180→180 for a valid EPSG:4326 affine transform
    roll_idx = int(np.searchsorted(lon, 180.0))
    cell_size = float(lon[1] - lon[0])
    lon_180 = np.concatenate([lon[roll_idx:] - 360.0, lon[:roll_idx]])
    scalar_180 = np.concatenate(
        [scalar[:, roll_idx:], scalar[:, :roll_idx]], axis=1
    )

    h, w = scalar_180.shape
    west = float(lon_180[0])              # ≈ −180.0
    east = float(lon_180[-1]) + cell_size  # ≈  180.0 (include last cell edge)
    north = float(lat[0])                  # ≈   90.0
    south = float(lat[-1])                 # ≈  −90.0

    src_transform = _affine_from_bounds(west, south, east, north, w, h)

    # Square Mercator canvas covers the full Web Mercator world
    dst_transform = _affine_from_bounds(
        -_MERC_HALF, -_MERC_HALF, _MERC_HALF, _MERC_HALF,
        canvas_size, canvas_size,
    )

    merc_canvas = np.full((canvas_size, canvas_size), np.nan, dtype=np.float32)

    with MemoryFile() as mem:
        with mem.open(
            driver="GTiff",
            dtype=rasterio.float32,
            width=w,
            height=h,
            count=1,
            crs=CRS.from_epsg(4326),
            transform=src_transform,
            nodata=float("nan"),
        ) as src_ds:
            src_ds.write(scalar_180.astype(np.float32), 1)
            _warp_reproject(
                source=rasterio.band(src_ds, 1),
                destination=merc_canvas,
                dst_transform=dst_transform,
                dst_crs=CRS.from_epsg(3857),
                resampling=resampling,
                dst_nodata=float("nan"),
            )

    px_per_meter = canvas_size / (2.0 * _MERC_HALF)
    return merc_canvas, px_per_meter


# ---------------------------------------------------------------------------
# Per-tile helpers
# ---------------------------------------------------------------------------

def _empty_tile_png() -> bytes:
    # Phase 1: Don't save empty tiles. Return empty bytes.
    return b""


def _cut_tile_from_merc(
    merc: np.ndarray,
    px_per_meter: float,
    tile: mercantile.Tile,
    cmap_type: str,
    cmap_product: str | None,
) -> bytes:
    """
    Extract scalar window for one tile from the Mercator canvas,
    colorize it, resize to TILE_SIZE×TILE_SIZE, encode as transparent PNG.
    """
    canvas_size = merc.shape[0]
    # (left, bottom, right, top) in EPSG:3857 metres
    xy = mercantile.xy_bounds(tile)

    # Canvas pixel coordinates:
    #   columns increase eastward  (origin = west edge = −_MERC_HALF)
    #   rows    increase southward (origin = north edge = +_MERC_HALF)
    col_left = (xy.left + _MERC_HALF) * px_per_meter
    col_right = (xy.right + _MERC_HALF) * px_per_meter
    row_top = (_MERC_HALF - xy.top) * px_per_meter
    row_bot = (_MERC_HALF - xy.bottom) * px_per_meter

    x0 = max(0, int(math.floor(col_left)))
    x1 = min(canvas_size, int(math.ceil(col_right)))
    y0 = max(0, int(math.floor(row_top)))
    y1 = min(canvas_size, int(math.ceil(row_bot)))

    if x1 <= x0 or y1 <= y0:
        return b""

    tile_scalar = merc[y0:y1, x0:x1]
    if tile_scalar.size == 0:
        return b""
        
    nan_mask_s = ~np.isfinite(tile_scalar)
    if nan_mask_s.all():
        return b""

    # --- scalar-first approach: upscale float scalar BEFORE colorizing ---
    # This produces smooth isoband/contour-style boundaries instead of
    # blocky colour patches that result from resizing an already-colorized RGBA.
    # For stepped integer colormaps (precip_base etc.) use NEAREST to keep
    # color bands crisp; for continuous colormaps (temperature etc.) use LANCZOS.
    pil_resample = (
        Image.Resampling.NEAREST
        if cmap_type in _NEAREST_CMAP_TYPES
        else Image.Resampling.LANCZOS
    )
    # Valid-mask resample: NEAREST for discrete (keeps sharp dry boundary),
    # BILINEAR for continuous (soft anti-alias at data edge).
    valid_resample = (
        Image.Resampling.NEAREST
        if cmap_type in _NEAREST_CMAP_TYPES
        else Image.Resampling.BILINEAR
    )
    h_s, w_s = tile_scalar.shape
    if h_s != TILE_SIZE or w_s != TILE_SIZE:
        nan_mask_s = ~np.isfinite(tile_scalar)
        # For NEAREST: fill NaN with 0 (maps to dry/transparent bin).
        # For LANCZOS: fill NaN with nanmean so the filter doesn't spread zeros.
        if pil_resample == Image.Resampling.NEAREST:
            fill_val = 0.0
        else:
            fill_val = float(np.nanmean(tile_scalar)) if not nan_mask_s.all() else 0.0
        filled = np.where(nan_mask_s, fill_val, tile_scalar).astype(np.float32)
        scalar_img = Image.fromarray(filled, mode="F")
        scalar_img = scalar_img.resize((TILE_SIZE, TILE_SIZE), pil_resample)
        # Upscale valid-data mask using the matching resample method
        valid_img = Image.fromarray((~nan_mask_s).astype(np.float32), mode="F")
        valid_img = valid_img.resize((TILE_SIZE, TILE_SIZE), valid_resample)
        scalar_up = np.array(scalar_img, dtype=np.float32)
        scalar_up[np.array(valid_img) < 0.01] = np.nan
        tile_scalar = scalar_up

    # Colorize the upscaled scalar → RGBA (NaN cells get alpha=0)
    rgba = apply_colormap(tile_scalar, cmap_type, product=cmap_product)
    
    # Phase 1: Skip if completely transparent after colormap
    if np.all(rgba[..., 3] == 0):
        return b""
        
    img = Image.fromarray(rgba, mode="RGBA")
    
    # Phase 2: Indexed PNG for banded layers
    if cmap_type in _NEAREST_CMAP_TYPES or cmap_type in {"rain_basic", "snow_depth"}:
        try:
            # Try to convert to indexed PNG for significant size reduction
            img = img.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
        except Exception:
            pass # Fallback to RGBA if quantization fails

    buf = io.BytesIO()
    # Phase 1: Lossless optimization with compress_level=9
    img.save(buf, format="PNG", optimize=True, compress_level=9)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def cut_and_save(
    scalar: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    cmap_type: str,
    cmap_product: str | None,
    run_id: str,
    fff: int,
    product: str,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 5,
    workers: int = 8,
    skip_existing: bool = True,
    warp_resampling: Resampling = Resampling.bilinear,
) -> dict:
    """
    Reproject scalar grid to EPSG:3857 and generate all XYZ tiles for
    zoom_min..zoom_max.  Tiles are saved to:
      {output_dir}/{fff:03d}/{product}/{z}/{x}/{y}.png

    output_dir is already the run-specific staging folder.
    Returns summary dict {total, saved, skipped, errors}.
    """
    base = output_dir / f"{fff:03d}" / product
    base.mkdir(parents=True, exist_ok=True)

    # Build Mercator canvas once at a resolution appropriate for zoom_max
    canvas_size = min((2 ** zoom_max) * TILE_SIZE, 8192)
    merc, px_per_meter = _warp_scalar_to_mercator(
        scalar, lat, lon, canvas_size, resampling=warp_resampling)

    jobs: list[mercantile.Tile] = []
    for z in range(zoom_min, zoom_max + 1):
        jobs.extend(mercantile.tiles(-180, -85.051129,
                    180, 85.051129, zooms=z))

    total = len(jobs)
    saved = skipped = empty_skipped = errors = 0

    def _save_one(tile: mercantile.Tile) -> str:
        out = base / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        if skip_existing and out.exists() and out.stat().st_size > 0:
            return "skip"
        
        try:
            png_bytes = _cut_tile_from_merc(
                merc, px_per_meter, tile, cmap_type, cmap_product
            )
            if not png_bytes:
                return "skip_empty"
                
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(png_bytes)
            return "ok"
        except Exception as exc:
            return f"err:{exc}"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_save_one, tile): tile for tile in jobs}
        for fut in as_completed(futures):
            res = fut.result()
            if res == "ok":
                saved += 1
            elif res == "skip":
                skipped += 1
            elif res == "skip_empty":
                empty_skipped += 1
            else:
                errors += 1

    import json
    import time
    
    total_size = sum(f.stat().st_size for f in base.rglob("*.png"))
    actual_on_disk = sum(1 for _ in base.rglob("*.png"))
    
    manifest = {
        "ready": True,
        "product": product,
        "total": total,
        "saved": saved,
        "skipped_existing": skipped,
        "empty_skipped": empty_skipped,
        "errors": errors,
        "actual_on_disk": actual_on_disk,
        "total_size_bytes": total_size,
        "timestamp": time.time()
    }
    (base / "manifest.json").write_text(json.dumps(manifest, indent=2))

    return {"total": total, "saved": saved, "skipped_existing": skipped, "empty_skipped": empty_skipped, "errors": errors}


def cut_and_save_from_canvas(
    merc: np.ndarray,
    px_per_meter: float,
    cmap_type: str,
    cmap_product: str | None,
    run_id: str,
    fff: int,
    product: str,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 5,
    workers: int = 8,
    skip_existing: bool = True,
) -> dict:
    """
    Cut XYZ tiles from an already-reprojected Mercator canvas.

    Used by precip_pipeline which pre-warps multiple fields separately
    (bilinear PRATE, nearest CRAIN/CSNOW) then classifies on the Mercator
    canvas before calling this function.  Skips the internal warp step.

    Args:
        merc         : float32 ndarray (canvas_size x canvas_size) in EPSG:3857.
                       NaN = dry/transparent.  Non-NaN values are the scalar
                       (e.g. combined_index 1..18 for advanced_precipitation_base).
        px_per_meter : canvas pixels per metre (returned by _warp_scalar_to_mercator).
    """
    base = output_dir / f"{fff:03d}" / product
    base.mkdir(parents=True, exist_ok=True)

    jobs: list[mercantile.Tile] = []
    for z in range(zoom_min, zoom_max + 1):
        jobs.extend(mercantile.tiles(-180, -85.051129,
                    180, 85.051129, zooms=z))

    total = len(jobs)
    saved = skipped = empty_skipped = errors = 0

    def _save_one(tile: mercantile.Tile) -> str:
        out = base / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        if skip_existing and out.exists() and out.stat().st_size > 0:
            return "skip"
            
        try:
            png_bytes = _cut_tile_from_merc(
                merc, px_per_meter, tile, cmap_type, cmap_product
            )
            if not png_bytes:
                return "skip_empty"
                
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(png_bytes)
            return "ok"
        except Exception as exc:
            return f"err:{exc}"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_save_one, tile): tile for tile in jobs}
        for fut in as_completed(futures):
            res = fut.result()
            if res == "ok":
                saved += 1
            elif res == "skip":
                skipped += 1
            elif res == "skip_empty":
                empty_skipped += 1
            else:
                errors += 1

    import json
    import time
    
    total_size = sum(f.stat().st_size for f in base.rglob("*.png"))
    actual_on_disk = sum(1 for _ in base.rglob("*.png"))
    
    manifest = {
        "ready": True,
        "product": product,
        "total": total,
        "saved": saved,
        "skipped_existing": skipped,
        "empty_skipped": empty_skipped,
        "errors": errors,
        "actual_on_disk": actual_on_disk,
        "total_size_bytes": total_size,
        "timestamp": time.time()
    }
    (base / "manifest.json").write_text(json.dumps(manifest, indent=2))

    return {"total": total, "saved": saved, "skipped_existing": skipped, "empty_skipped": empty_skipped, "errors": errors}


# ---------------------------------------------------------------------------
# SSAA + Late Classification for Advanced Precipitation
# ---------------------------------------------------------------------------

def _cut_tile_adv_precip(
    merc_prate: np.ndarray,
    merc_crain: np.ndarray,
    merc_csnow: np.ndarray,
    merc_cicep: np.ndarray,
    merc_cfrzr: np.ndarray,
    merc_cpofp: np.ndarray,
    px_per_meter: float,
    tile: mercantile.Tile,
) -> bytes:
    """
    Supersampling Anti-Aliasing (SSAA) + Late Classification on a single tile.
    1. Slice the 3 raw float canvases.
    2. Upscale slices to 512x512 using BILINEAR.
    3. Run classify_on_mercator on the 512x512 data -> final_index integer array.
    4. Colorize -> 512x512 RGBA.
    5. Downscale to 256x256 using LANCZOS to produce feathered soft alpha edges.
    """
    from PIL import Image
    from .precip_classifier import classify_on_mercator

    canvas_size = merc_prate.shape[0]
    xy = mercantile.xy_bounds(tile)

    col_left = (xy.left + _MERC_HALF) * px_per_meter
    col_right = (xy.right + _MERC_HALF) * px_per_meter
    row_top = (_MERC_HALF - xy.top) * px_per_meter
    row_bot = (_MERC_HALF - xy.bottom) * px_per_meter

    x0 = max(0, int(math.floor(col_left)))
    x1 = min(canvas_size, int(math.ceil(col_right)))
    y0 = max(0, int(math.floor(row_top)))
    y1 = min(canvas_size, int(math.ceil(row_bot)))

    if x1 <= x0 or y1 <= y0:
        return b""

    slice_prate = merc_prate[y0:y1, x0:x1]
    slice_crain = merc_crain[y0:y1, x0:x1]
    slice_csnow = merc_csnow[y0:y1, x0:x1]
    slice_cicep = merc_cicep[y0:y1, x0:x1]
    slice_cfrzr = merc_cfrzr[y0:y1, x0:x1]
    slice_cpofp = merc_cpofp[y0:y1, x0:x1]

    if slice_prate.size == 0:
        return b""
        
    nan_mask_s = ~np.isfinite(slice_prate)
    if nan_mask_s.all():
        return b""

    # Oversample size (2x TILE_SIZE)
    ssaa_size = TILE_SIZE * 2

    def _upsample(arr):
        nan_mask = ~np.isfinite(arr)
        if nan_mask.all():
            return np.full((ssaa_size, ssaa_size), np.nan, dtype=np.float32)
        filled = np.where(nan_mask, 0.0, arr).astype(np.float32)
        img = Image.fromarray(filled, mode="F")
        img_up = img.resize((ssaa_size, ssaa_size), Image.Resampling.BILINEAR)
        return np.array(img_up, dtype=np.float32)

    up_prate = _upsample(slice_prate)
    up_crain = _upsample(slice_crain)
    up_csnow = _upsample(slice_csnow)
    up_cicep = _upsample(slice_cicep)
    up_cfrzr = _upsample(slice_cfrzr)
    up_cpofp = _upsample(slice_cpofp)

    # Late Classification on the high-res 512x512 array
    idx_512 = classify_on_mercator(
        up_prate, up_crain, up_csnow, up_cicep, up_cfrzr, up_cpofp, 
        min_patch_pixels=10, verbose=False
    )

    # Colorize
    rgba_512 = apply_colormap(idx_512, "precip_base")
    img_512 = Image.fromarray(rgba_512, mode="RGBA")

    # Downsample with LANCZOS to feather alpha at boundaries
    img_256 = img_512.resize((TILE_SIZE, TILE_SIZE), Image.Resampling.LANCZOS)
    
    # Check if transparent after LANCZOS
    if np.all(np.array(img_256)[..., 3] == 0):
        return b""
        
    # Phase 2: Indexed PNG for advanced precipitation
    try:
        img_256 = img_256.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
    except Exception:
        pass

    buf = io.BytesIO()
    img_256.save(buf, format="PNG", optimize=True, compress_level=9)
    return buf.getvalue()


def cut_and_save_adv_precip(
    merc_prate: np.ndarray,
    merc_crain: np.ndarray,
    merc_csnow: np.ndarray,
    merc_cicep: np.ndarray,
    merc_cfrzr: np.ndarray,
    merc_cpofp: np.ndarray,
    px_per_meter: float,
    run_id: str,
    fff: int,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 5,
    workers: int = 8,
    skip_existing: bool = True,
) -> dict:
    """
    Dedicated tile cutter for advanced_precipitation_base using SSAA logic.
    """
    base = output_dir / f"{fff:03d}" / "precip_base"
    base.mkdir(parents=True, exist_ok=True)

    jobs: list[mercantile.Tile] = []
    for z in range(zoom_min, zoom_max + 1):
        jobs.extend(mercantile.tiles(-180, -85.051129,
                    180, 85.051129, zooms=z))

    total = len(jobs)
    saved = skipped = empty_skipped = errors = 0

    def _save_one(tile: mercantile.Tile) -> str:
        out = base / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        if skip_existing and out.exists() and out.stat().st_size > 0:
            return "skip"
            
        try:
            png_bytes = _cut_tile_adv_precip(
                merc_prate, merc_crain, merc_csnow, 
                merc_cicep, merc_cfrzr, merc_cpofp, 
                px_per_meter, tile
            )
            if not png_bytes:
                return "skip_empty"
                
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(png_bytes)
            return "ok"
        except Exception as exc:
            return f"err:{exc}"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_save_one, tile): tile for tile in jobs}
        for fut in as_completed(futures):
            res = fut.result()
            if res == "ok":
                saved += 1
            elif res == "skip":
                skipped += 1
            elif res == "skip_empty":
                empty_skipped += 1
            else:
                errors += 1

    import json
    import time
    
    total_size = sum(f.stat().st_size for f in base.rglob("*.png"))
    actual_on_disk = sum(1 for _ in base.rglob("*.png"))
    
    manifest = {
        "ready": True,
        "product": "precip_base",
        "total": total,
        "saved": saved,
        "skipped_existing": skipped,
        "empty_skipped": empty_skipped,
        "errors": errors,
        "actual_on_disk": actual_on_disk,
        "total_size_bytes": total_size,
        "timestamp": time.time()
    }
    (base / "manifest.json").write_text(json.dumps(manifest, indent=2))

    return {"total": total, "saved": saved, "skipped_existing": skipped, "empty_skipped": empty_skipped, "errors": errors}





def get_lazy_tile(
    scalar: np.ndarray,
    lat: np.ndarray,
    lon: np.ndarray,
    cmap_type: str,
    cmap_product: str | None,
    z: int,
    x: int,
    y: int,
) -> bytes:
    """
    Generate a single tile on-demand (for lazy high-zoom requests).
    Caller is responsible for caching the result to disk.
    """
    canvas_size = min((2 ** z) * TILE_SIZE, 8192)
    merc, px_per_meter = _warp_scalar_to_mercator(
        scalar, lat, lon, canvas_size)
    return _cut_tile_from_merc(
        merc, px_per_meter, mercantile.Tile(
            x=x, y=y, z=z), cmap_type, cmap_product
    )


# ---------------------------------------------------------------------------
# Wind (Base PNG + Field Bin)
# ---------------------------------------------------------------------------

def _cut_wind_tile_from_merc(
    merc_u: np.ndarray,
    merc_v: np.ndarray,
    merc_speed: np.ndarray,
    px_per_meter: float,
    tile: mercantile.Tile,
) -> tuple[bytes, bytes]:
    canvas_size = merc_u.shape[0]
    xy = mercantile.xy_bounds(tile)

    col_left = (xy.left + _MERC_HALF) * px_per_meter
    col_right = (xy.right + _MERC_HALF) * px_per_meter
    row_top = (_MERC_HALF - xy.top) * px_per_meter
    row_bot = (_MERC_HALF - xy.bottom) * px_per_meter

    x0 = max(0, int(math.floor(col_left)))
    x1 = min(canvas_size, int(math.ceil(col_right)))
    y0 = max(0, int(math.floor(row_top)))
    y1 = min(canvas_size, int(math.ceil(row_bot)))

    if x1 <= x0 or y1 <= y0:
        return b"", b""

    slice_u = merc_u[y0:y1, x0:x1]
    slice_v = merc_v[y0:y1, x0:x1]
    slice_speed = merc_speed[y0:y1, x0:x1]

    if slice_u.size == 0:
        return b"", b""
        
    if (~np.isfinite(slice_u)).all():
        return b"", b""

    # upscale to 256x256 using BILINEAR
    def _upsample(arr):
        nan_mask = ~np.isfinite(arr)
        if nan_mask.all():
            return np.full((TILE_SIZE, TILE_SIZE), np.nan, dtype=np.float32)
        filled = np.where(nan_mask, 0.0, arr).astype(np.float32)
        img = Image.fromarray(filled, mode="F")
        img_up = img.resize((TILE_SIZE, TILE_SIZE), Image.Resampling.BILINEAR)
        up_arr = np.array(img_up, dtype=np.float32)
        
        # Valid mask
        valid_img = Image.fromarray((~nan_mask).astype(np.float32), mode="F")
        valid_img = valid_img.resize((TILE_SIZE, TILE_SIZE), Image.Resampling.BILINEAR)
        up_arr[np.array(valid_img) < 0.01] = np.nan
        return up_arr

    h_s, w_s = slice_u.shape
    if h_s != TILE_SIZE or w_s != TILE_SIZE:
        up_u = _upsample(slice_u)
        up_v = _upsample(slice_v)
        up_speed = _upsample(slice_speed)
    else:
        up_u = slice_u
        up_v = slice_v
        up_speed = slice_speed

    # 1. Base PNG (wind speed)
    rgba = apply_colormap(up_speed, "wind_surface")
    
    empty_speed = np.all(rgba[..., 3] == 0)
    
    if not empty_speed:
        img_speed = Image.fromarray(rgba, mode="RGBA")
        try:
            img_speed = img_speed.quantize(colors=256, method=Image.Quantize.MEDIANCUT)
        except Exception:
            pass
        buf_png = io.BytesIO()
        img_speed.save(buf_png, format="PNG", optimize=True, compress_level=9)
        bytes_speed = buf_png.getvalue()
    else:
        bytes_speed = b""
    
    # 2. Field PNG (u, v mapped to [0-255] RGB)
    # U -> Red, V -> Green, B -> 0. Alpha -> 255 if valid, 0 if NaN.
    # Map range [-100, 100] m/s -> [0, 255] uint8
    nan_mask_uv = np.isnan(up_u) | np.isnan(up_v)
    
    if nan_mask_uv.all():
        bytes_uv = b""
    else:
        u_scaled = np.clip((np.nan_to_num(up_u, nan=0.0) + 100.0) * (255.0 / 200.0), 0, 255)
        v_scaled = np.clip((np.nan_to_num(up_v, nan=0.0) + 100.0) * (255.0 / 200.0), 0, 255)
        
        r = u_scaled.astype(np.uint8)
        g = v_scaled.astype(np.uint8)
        b = np.zeros_like(r, dtype=np.uint8)
        a = (~nan_mask_uv).astype(np.uint8) * 255
        
        rgba_uv = np.stack([r, g, b, a], axis=-1)
        img_uv = Image.fromarray(rgba_uv, mode="RGBA")
        
        buf_uv = io.BytesIO()
        img_uv.save(buf_uv, format="PNG", optimize=True, compress_level=9)
        bytes_uv = buf_uv.getvalue()

    return bytes_speed, bytes_uv



def cut_and_save_wind(
    merc_u: np.ndarray,
    merc_v: np.ndarray,
    merc_speed: np.ndarray,
    px_per_meter: float,
    run_id: str,
    fff: int,
    output_dir: Path,
    zoom_min: int = 0,
    zoom_max: int = 5,
    workers: int = 8,
    skip_existing: bool = True,
) -> dict:
    base_png = output_dir / f"{fff:03d}" / "wind_base"
    base_bin = output_dir / f"{fff:03d}" / "wind_field"
    base_png.mkdir(parents=True, exist_ok=True)
    base_bin.mkdir(parents=True, exist_ok=True)

    jobs: list[mercantile.Tile] = []
    for z in range(zoom_min, zoom_max + 1):
        jobs.extend(mercantile.tiles(-180, -85.051129, 180, 85.051129, zooms=z))

    total = len(jobs)
    saved = skipped = empty_skipped = errors = 0

    def _save_one(tile: mercantile.Tile) -> str:
        out_png = base_png / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        out_uv_png = base_bin / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        
        if skip_existing and out_png.exists() and out_uv_png.exists() and out_png.stat().st_size > 0 and out_uv_png.stat().st_size > 0:
            return "skip"
            
        try:
            png_bytes, uv_png_bytes = _cut_wind_tile_from_merc(
                merc_u, merc_v, merc_speed, px_per_meter, tile
            )
            
            if not png_bytes and not uv_png_bytes:
                return "skip_empty"
                
            if png_bytes:
                out_png.parent.mkdir(parents=True, exist_ok=True)
                out_png.write_bytes(png_bytes)
            if uv_png_bytes:
                out_uv_png.parent.mkdir(parents=True, exist_ok=True)
                out_uv_png.write_bytes(uv_png_bytes)
                
            return "ok"
        except Exception as exc:
            return f"err:{exc}"

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_save_one, tile): tile for tile in jobs}
        for fut in as_completed(futures):
            res = fut.result()
            if res == "ok":
                saved += 1
            elif res == "skip":
                skipped += 1
            elif res == "skip_empty":
                empty_skipped += 1
            else:
                errors += 1

    import json
    import time
    
    base_size = sum(f.stat().st_size for f in base_png.rglob("*.png"))
    field_size = sum(f.stat().st_size for f in base_bin.rglob("*.png"))
    
    base_count = sum(1 for _ in base_png.rglob("*.png"))
    field_count = sum(1 for _ in base_bin.rglob("*.png"))
    
    manifest_base = {
        "ready": True,
        "product": "wind_base",
        "total": total,
        "saved": saved,
        "skipped_existing": skipped,
        "empty_skipped": empty_skipped,
        "errors": errors,
        "actual_on_disk": base_count,
        "total_size_bytes": base_size,
        "timestamp": time.time()
    }
    
    manifest_field = {
        "ready": True,
        "product": "wind_field",
        "total": total,
        "saved": saved,
        "skipped_existing": skipped,
        "empty_skipped": empty_skipped,
        "errors": errors,
        "actual_on_disk": field_count,
        "total_size_bytes": field_size,
        "timestamp": time.time()
    }
    
    (base_png / "manifest.json").write_text(json.dumps(manifest_base, indent=2))
    (base_bin / "manifest.json").write_text(json.dumps(manifest_field, indent=2))

    return {"total": total, "saved": saved, "skipped_existing": skipped, "empty_skipped": empty_skipped, "errors": errors}

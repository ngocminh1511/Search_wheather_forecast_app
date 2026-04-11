from __future__ import annotations

"""
tile_cutter.py — convert a global RGBA numpy array into XYZ PNG tiles.

Flow:
  1. Receive RGBA ndarray (H × W × 4) covering global extent (lon 0..360, lat 90..-90).
  2. For each (z, x, y) in the requested zoom range, compute the tile's
     lat/lon bounds using mercantile, crop + bicubic-resize to TILE_SIZE×TILE_SIZE,
     and encode as PNG bytes.
  3. Save PNG to: {tiles_dir}/{map_type}/{run_id}/{fff}/{z}/{x}/{y}.png
     First saves to staging dir, caller atomically renames when all done.

Uses concurrent.futures.ThreadPoolExecutor for parallel encoding.
"""

import io
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import mercantile
import numpy as np
from PIL import Image


TILE_SIZE = 256  # px


# ---------------------------------------------------------------------------
# Coordinate helpers
# ---------------------------------------------------------------------------

def _lon360_to_pixel_x(lon360: float, img_width: int) -> float:
    """Map longitude [0, 360] → pixel column [0, img_width)."""
    return (lon360 / 360.0) * img_width


def _lat_to_pixel_y(lat: float, img_height: int) -> float:
    """
    Map latitude [-90, 90] → pixel row [0, img_height).
    Uses Web Mercator (EPSG:3857) Y projection so tiles align with slippy map.
    """
    lat_rad = math.radians(max(-85.051129, min(85.051129, lat)))
    merc_y = math.log(math.tan(math.pi / 4 + lat_rad / 2))
    # merc_y in range ≈ [-π, π]; normalize to [0, 1] then to pixel
    norm = (math.pi - merc_y) / (2 * math.pi)
    return norm * img_height


# ---------------------------------------------------------------------------
# Core cut function
# ---------------------------------------------------------------------------

def _cut_tile(
    rgba: np.ndarray,
    tile: mercantile.Tile,
    img_h: int,
    img_w: int,
) -> bytes:
    """
    Crop and resize the global RGBA image to a single 256×256 tile.
    Returns PNG bytes.
    """
    bounds = mercantile.bounds(tile)

    # Clamp to valid Mercator lat range
    south = max(-85.051129, bounds.south)
    north = min(85.051129, bounds.north)
    west = bounds.west % 360   # convert -180..180 → 0..360
    east = bounds.east % 360

    # Pixel coordinates in the source image
    # Longitude: 0..360 → 0..img_w
    px_left = _lon360_to_pixel_x(west, img_w)
    px_right = _lon360_to_pixel_x(east, img_w)

    # Latitude: north → top row, south → bottom row
    py_top = _lat_to_pixel_y(north, img_h)
    py_bottom = _lat_to_pixel_y(south, img_h)

    # Integer crop box (may be sub-pixel — PIL handles fractional via resize)
    x0 = max(0, int(math.floor(px_left)))
    x1 = min(img_w, int(math.ceil(px_right)))
    y0 = max(0, int(math.floor(py_top)))
    y1 = min(img_h, int(math.ceil(py_bottom)))

    if x1 <= x0 or y1 <= y0:
        # Empty tile (out of bounds) → transparent PNG
        return _empty_tile_png()

    crop = rgba[y0:y1, x0:x1, :]
    if crop.size == 0:
        return _empty_tile_png()

    img = Image.fromarray(crop, mode="RGBA")
    img = img.resize((TILE_SIZE, TILE_SIZE), resample=Image.Resampling.BICUBIC)

    buf = io.BytesIO()
    img.save(buf, format="PNG", optimize=False, compress_level=1)
    return buf.getvalue()


def _empty_tile_png() -> bytes:
    img = Image.new("RGBA", (TILE_SIZE, TILE_SIZE), (0, 0, 0, 0))
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Source image preparation
# ---------------------------------------------------------------------------

def _prepare_source_image(rgba: np.ndarray, zoom_max: int) -> np.ndarray:
    """
    Upscale the raw RGBA grid to a resolution suitable for the requested zoom.
    Target: 2^zoom_max × TILE_SIZE pixels per axis (approximately).
    Uses bicubic for smooth weather gradients.
    """
    target = (2 ** zoom_max) * TILE_SIZE
    # Cap to avoid OOM on high zooms
    target = min(target, 16384)

    src_h, src_w = rgba.shape[:2]
    if src_h >= target and src_w >= target:
        return rgba

    img = Image.fromarray(rgba, mode="RGBA")
    # Keep aspect: GFS is 721×1440 → roughly 1:2 ratio matches global extent
    # Cap at 8192×4096 to avoid OOM (GFS 0.25° has no detail above this anyway)
    new_w = min(target * 2, 8192)
    new_h = min(target, 4096)
    img = img.resize((new_w, new_h), resample=Image.Resampling.BICUBIC)
    return np.array(img, dtype=np.uint8)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def cut_and_save(
    rgba: np.ndarray,
    map_type: str,
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
    Generate all tiles for zoom_min..zoom_max and save to:
      {output_dir}/{map_type}/{run_id}/{fff}/{product}/{z}/{x}/{y}.png

    Returns summary dict {total, saved, skipped, errors}.
    """
    base = output_dir / map_type / run_id / f"{fff:03d}" / product
    base.mkdir(parents=True, exist_ok=True)

    # Build source image scaled for zoom_max
    src = _prepare_source_image(rgba, zoom_max)
    src_h, src_w = src.shape[:2]

    # Collect all tiles to generate
    jobs: list[mercantile.Tile] = []
    for z in range(zoom_min, zoom_max + 1):
        jobs.extend(mercantile.tiles(-180, -85.051129, 180, 85.051129, zooms=z))

    total = len(jobs)
    saved = skipped = errors = 0

    def _save_one(tile: mercantile.Tile) -> str:
        out = base / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        if skip_existing and out.exists() and out.stat().st_size > 0:
            return "skip"
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            png_bytes = _cut_tile(src, tile, src_h, src_w)
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
            else:
                errors += 1

    return {"total": total, "saved": saved, "skipped": skipped, "errors": errors}


def get_lazy_tile(
    rgba: np.ndarray,
    z: int,
    x: int,
    y: int,
) -> bytes:
    """
    Generate a single tile on-demand (for lazy z=6+ requests).
    rgba must be the pre-prepared source image at suitable resolution.
    """
    src_h, src_w = rgba.shape[:2]
    tile = mercantile.Tile(x=x, y=y, z=z)
    return _cut_tile(rgba, tile, src_h, src_w)

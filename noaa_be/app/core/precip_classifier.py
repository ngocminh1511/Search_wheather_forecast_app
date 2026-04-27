from __future__ import annotations

"""
precip_classifier.py — classify GFS precipitation fields into rain / mixed / snow zones.

Data sources (per map.docx):
  PRATE — precipitation rate (mm/h, bilinear-warped Mercator canvas)
  CRAIN — categorical rain  (0/1, nearest-warped Mercator canvas)
  CSNOW — categorical snow  (0/1, nearest-warped Mercator canvas)

Primary API:
  classify_on_mercator(prate, crain, csnow) → combined_index float32 H×W
    NaN = dry (transparent)
    1..6  = rain  level 1..6
    7..12 = mixed level 1..6
   13..18 = snow  level 1..6

Pipeline:
  1. Apply physics_threshold (< 0.01 mm/h → definitely dry)
  2. Apply visual_threshold  (< 0.10 mm/h → do not render on base tile)
  3. Classify zones: Rain / Mixed / Snow (Mixed = CRAIN AND CSNOW)
  4. Light cleanup: binary_opening + remove small patches
  5. Intensity binning with PRATE bins → level 1..6
  6. Build combined_index
"""

import logging
from dataclasses import dataclass

import numpy as np
from scipy.ndimage import binary_opening, label, find_objects


_log = logging.getLogger(__name__)

# ── Thresholds (mm/h) ────────────────────────────────────────────────────────
PHYSICS_THRESHOLD: float = 0.01   # below this → definitely dry, no render
VISUAL_THRESHOLD:  float = 0.10   # below this → not rendered on base tile
                                   # (drizzle/noise suppression)

# ── Intensity bins (mm/h) — shared by Rain / Mixed / Snow ────────────────────
# np.digitize(prate, PRATE_BINS):
#   value < 0.10       → 0  (dry, should be filtered by visual_threshold)
#   0.10 ≤ value < 0.25 → 1  level 1 (Very light)
#   0.25 ≤ value < 0.50 → 2  level 2 (Light)
#   0.50 ≤ value < 1.00 → 3  level 3 (Moderate)
#   1.00 ≤ value < 2.50 → 4  level 4 (Heavy)
#   2.50 ≤ value < 5.00 → 5  level 5 (Very Heavy)
#   5.00 ≤ value        → 6  level 6 (Extreme core)
PRATE_BINS = np.array([0.10, 0.25, 0.50, 1.0, 2.5, 5.0], dtype=np.float32)

# ── Minimum patch size (pixels on Mercator canvas) ───────────────────────────
# Relative to canvas: one GFS cell ≈ canvas_size / 1440 pixels (equatorial).
# We remove patches < _MIN_PATCH_PIXELS to eliminate single-cell speckle.
_MIN_PATCH_PIXELS: int = 20   # absolute floor; caller scales if needed





# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _remove_small_patches(mask: np.ndarray, min_size: int) -> np.ndarray:
    """Remove connected components smaller than min_size pixels."""
    if min_size <= 1 or not mask.any():
        return mask
    labeled, n = label(mask)
    if n == 0:
        return mask
    cleaned = np.zeros_like(mask, dtype=bool)
    for i, obj_slice in enumerate(find_objects(labeled)):
        if obj_slice is None:
            continue
        component = labeled[obj_slice] == (i + 1)
        if component.sum() >= min_size:
            cleaned[obj_slice] |= component
    return cleaned


def _cleanup_mask(mask: np.ndarray, min_size: int = _MIN_PATCH_PIXELS) -> np.ndarray:
    """Light morphological cleanup: binary_opening + remove small patches."""
    if not mask.any():
        return mask
    mask = binary_opening(mask, iterations=1)
    if not mask.any():
        return mask
    mask = _remove_small_patches(mask, min_size)
    return mask


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Primary public API — classify on Mercator canvas (new pipeline)
# ---------------------------------------------------------------------------

def classify_on_mercator(
    prate: np.ndarray,   # float32 H×W mm/h — bilinear-warped Mercator canvas
    crain: np.ndarray,   # float32 H×W 0/1  — bilinear-warped Mercator canvas
    csnow: np.ndarray,   # float32 H×W 0/1  — bilinear-warped Mercator canvas
    cicep: np.ndarray,   # float32 H×W 0/1  — bilinear-warped Mercator canvas
    cfrzr: np.ndarray,   # float32 H×W 0/1  — bilinear-warped Mercator canvas
    cpofp: np.ndarray,   # float32 H×W %    — bilinear-warped Mercator canvas
    min_patch_pixels: int = _MIN_PATCH_PIXELS,
    verbose: bool = True,
) -> np.ndarray:
    """
    Classify precipitation type and intensity on pre-warped Mercator canvases.

    Returns combined_index float32 H×W:
      NaN   = dry (transparent, alpha=0 in tile)
      1..6  = rain  levels (PRATE bins: 0.10/0.50/1/5/10/25 mm/h)
      7..12 = mixed levels
     13..18 = snow  levels

    Dry logic:
      - prate < VISUAL_THRESHOLD (0.10 mm/h) → dry (not rendered)

    Mixed logic (Hybrid):
      - Physically driven by CICEP and CFRZR.
      - Fallback: widened soft-boundary between CRAIN and CSNOW.

    Cleanup:
      - binary_opening 3×3 on each zone mask
      - remove patches < min_patch_pixels
    """
    H, W = prate.shape
    _total = H * W

    # ── Step 1 & 2: Apply visual threshold — only prate >= 0.10 mm/h gets rendered
    precip_mask = (prate >= VISUAL_THRESHOLD) & np.isfinite(prate)

    # ── Step 3: Classify zones with Physical Data + Soft Fallback ──────────────
    # If CICEP or CFRZR exist in the GRIB file, they dictate the Mixed zone physically.
    # If they are 0.0 (not downloaded yet), we fallback to the widened soft intersection
    # between Rain and Snow to ensure the Mixed band is wide and beautiful.
    mixed_raw = precip_mask & (
        (cicep >= 0.1) | 
        (cfrzr >= 0.1) | 
        ((crain >= 0.15) & (csnow >= 0.15))
    )

    # Rain = crain is dominant (excluding Mixed)
    rain_raw  = precip_mask & (crain >= csnow) & ~mixed_raw

    # Snow = csnow is dominant (excluding Mixed)
    snow_raw  = precip_mask & (csnow > crain) & ~mixed_raw

    # Pixels with prate >= visual but NO category flag → treated as dry
    # (model data inconsistency, don't render to avoid noise)

    # ── Step 4: Log pre-cleanup stats ──────────────────────────────────────────
    n_rain_raw  = int(rain_raw.sum())
    n_mixed_raw = int(mixed_raw.sum())
    n_snow_raw  = int(snow_raw.sum())
    n_dry       = _total - n_rain_raw - n_mixed_raw - n_snow_raw
    if verbose:
        _log.info(
            "classify_on_mercator PRE-CLEANUP:"
            " prate_max=%.3f mm/h | dry=%d(%.1f%%) rain=%d(%.1f%%) "
            "mixed=%d(%.1f%%) snow=%d(%.1f%%)",
            float(np.nanmax(prate)) if np.isfinite(prate).any() else 0.0,
            n_dry,   100.0 * n_dry   / _total,
            n_rain_raw,  100.0 * n_rain_raw  / _total,
            n_mixed_raw, 100.0 * n_mixed_raw / _total,
            n_snow_raw,  100.0 * n_snow_raw  / _total,
        )

    # ── Step 5: Cleanup each mask ──────────────────────────────────────────────
    rain_mask  = _cleanup_mask(rain_raw,  min_patch_pixels)
    mixed_mask = _cleanup_mask(mixed_raw, min_patch_pixels)
    snow_mask  = _cleanup_mask(snow_raw,  min_patch_pixels)

    # Log removed patches
    if verbose:
        _log.info(
            "classify_on_mercator POST-CLEANUP:"
            " rain removed=%d mixed removed=%d snow removed=%d",
            n_rain_raw  - int(rain_mask.sum()),
            n_mixed_raw - int(mixed_mask.sum()),
            n_snow_raw  - int(snow_mask.sum()),
        )

    # ── Step 6: Intensity binning — levels 1..6 ────────────────────────────────
    # np.digitize(x, bins): returns index of bin such that bins[i-1] <= x < bins[i]
    # With PRATE_BINS = [0.10, 0.50, 1.0, 5.0, 10.0, 25.0]:
    #   x < 0.10  → 0   (dry, already filtered)
    #   0.10 ≤ x < 0.50 → 1
    #   ...
    #   25.0 ≤ x  → 6
    level = np.digitize(prate, PRATE_BINS)       # 0..6
    level = np.clip(level, 1, 6).astype(np.int8) # clip to 1..6 (0 = below visual)

    # ── Step 7: Build combined_index ──────────────────────────────────────────
    # NaN everywhere initially (= dry, transparent)
    combined = np.full((H, W), np.nan, dtype=np.float32)
    combined[rain_mask]  = level[rain_mask].astype(np.float32)           # 1..6
    combined[mixed_mask] = (level[mixed_mask] + 6).astype(np.float32)   # 7..12
    combined[snow_mask]  = (level[snow_mask] + 12).astype(np.float32)   # 13..18

    # ── Step 8: Full stats log ─────────────────────────────────────────────────
    if verbose:
        n_rain_f  = int(rain_mask.sum())
        n_mixed_f = int(mixed_mask.sum())
        n_snow_f  = int(snow_mask.sum())
        n_dry_f   = _total - n_rain_f - n_mixed_f - n_snow_f

        _log.info(
            "classify_on_mercator FINAL:"
            " dry=%d(%.1f%%) rain=%d(%.1f%%) mixed=%d(%.1f%%) snow=%d(%.1f%%)",
            n_dry_f,   100.0 * n_dry_f   / _total,
            n_rain_f,  100.0 * n_rain_f  / _total,
            n_mixed_f, 100.0 * n_mixed_f / _total,
            n_snow_f,  100.0 * n_snow_f  / _total,
        )

        # combined_index histogram (1..18)
        ci_valid = combined[np.isfinite(combined)].astype(int)
        if ci_valid.size > 0:
            counts = np.bincount(ci_valid, minlength=19)
            _log.info(
                "classify_on_mercator combined_index histogram 0..18: %s",
                counts.tolist(),
            )

    return combined




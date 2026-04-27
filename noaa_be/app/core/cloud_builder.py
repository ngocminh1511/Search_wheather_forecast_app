from __future__ import annotations

"""
cloud_builder.py — build cloud_total scalar field (0–100 %) from CloudFields.

Two paths:
  A) source == "total"   → use TCDC directly (already normalized to % by grib_reader)
  B) source == "layered" → random-overlap formula:
       total_frac = 1 − (1 − f_low) × (1 − f_mid) × (1 − f_high)
       total_pct  = clip(total_frac × 100, 0, 100)

NaN handling:
  - One layer NaN, others have data → treat NaN layer as 0 % (clear sky at that level)
  - All layers NaN → output pixel is NaN (no data)

Output is always float32, shape (H, W), values in 0–100 %.
Debug logging: min/max/nan_pct/histogram + 5 sample pixels.
"""

import logging
from dataclasses import dataclass

import numpy as np

from .cloud_reader import CloudFields

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------

@dataclass
class CloudBuildResult:
    values:  np.ndarray  # float32, (H, W), 0–100 %
    lat:     np.ndarray
    lon:     np.ndarray
    source:  str         # "total" | "layered"
    nan_pct: float       # % of NaN pixels (0–100)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_fraction(arr: np.ndarray) -> np.ndarray:
    """
    Convert a cloud cover array to fraction [0, 1].
    Values already in fraction (max ≤ 1.5) are returned as-is.
    Values in percent (max > 1.5) are divided by 100.
    """
    v = arr.astype(np.float64)
    if np.nanmax(v) > 1.5:
        v = v / 100.0
    return v


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_total_cloud_cover(fields: CloudFields) -> CloudBuildResult:
    """
    Build cloud_total (0–100 %) from a CloudFields instance.

    - source == "total"   → use fields.total directly
    - source == "layered" → random-overlap from fields.low/mid/high
    - source == "none"    → raises ValueError
    """
    if fields.source == "none":
        raise ValueError("No cloud data available — cannot build cloud_total")

    lat = fields.lat
    lon = fields.lon

    # ── Path A: total cloud cover available ─────────────────────────────
    if fields.total is not None:
        total_pct = np.clip(
            fields.total.values.astype(np.float32), 0.0, 100.0
        )
        nan_pct = float(np.isnan(total_pct).mean() * 100.0)

        _log.debug(
            "cloud_builder: source=total  min=%.1f max=%.1f nan_pct=%.1f%%",
            float(np.nanmin(total_pct)),
            float(np.nanmax(total_pct)),
            nan_pct,
        )
        _log_histogram(total_pct)

        return CloudBuildResult(
            values=total_pct, lat=lat, lon=lon,
            source="total", nan_pct=nan_pct,
        )

    # ── Path B: build from layered fields ───────────────────────────────
    # Determine reference shape from first available layer
    shape: tuple[int, int] = next(
        f.values.shape
        for f in (fields.low, fields.mid, fields.high)
        if f is not None
    )

    def _layer_fraction(f) -> np.ndarray:
        if f is None:
            return np.full(shape, np.nan, dtype=np.float64)
        return _to_fraction(f.values)

    lf = _layer_fraction(fields.low)
    mf = _layer_fraction(fields.mid)
    hf = _layer_fraction(fields.high)

    # Track pixels where ALL layers are NaN → output must be NaN
    all_nan_mask = np.isnan(lf) & np.isnan(mf) & np.isnan(hf)

    # NaN layer → treat as 0 (clear sky at that pressure level)
    lf = np.nan_to_num(lf, nan=0.0)
    mf = np.nan_to_num(mf, nan=0.0)
    hf = np.nan_to_num(hf, nan=0.0)

    # Random overlap (maximum random overlap assumption)
    total_frac = 1.0 - (1.0 - lf) * (1.0 - mf) * (1.0 - hf)
    total_frac = np.clip(total_frac, 0.0, 1.0)

    total_pct = (total_frac * 100.0).astype(np.float32)
    total_pct[all_nan_mask] = np.nan

    nan_pct = float(np.isnan(total_pct).mean() * 100.0)

    _log.debug(
        "cloud_builder: source=layered  min=%.1f max=%.1f nan_pct=%.1f%%",
        float(np.nanmin(total_pct)),
        float(np.nanmax(total_pct)),
        nan_pct,
    )

    # ── 5 sample pixels ─────────────────────────────────────────────────
    if _log.isEnabledFor(logging.DEBUG):
        flat_l = fields.low.values.flatten(
        ) if fields.low is not None else np.full(total_pct.size, np.nan)
        flat_m = fields.mid.values.flatten(
        ) if fields.mid is not None else np.full(total_pct.size, np.nan)
        flat_h = fields.high.values.flatten(
        ) if fields.high is not None else np.full(total_pct.size, np.nan)
        flat_t = total_pct.flatten()
        idxs = np.linspace(0, flat_t.size - 1, min(5, flat_t.size), dtype=int)
        for i in idxs:
            _log.debug(
                "cloud_builder pixel: low=%.1f mid=%.1f high=%.1f → total=%.1f%%",
                flat_l[i], flat_m[i], flat_h[i], flat_t[i],
            )
        _log_histogram(total_pct)

    return CloudBuildResult(
        values=total_pct, lat=lat, lon=lon,
        source="layered", nan_pct=nan_pct,
    )


# ---------------------------------------------------------------------------
# Debug helper
# ---------------------------------------------------------------------------

def _log_histogram(total_pct: np.ndarray) -> None:
    """Log a 10-bin histogram of cloud_total % values (debug level only)."""
    if not _log.isEnabledFor(logging.DEBUG):
        return
    valid = total_pct[np.isfinite(total_pct)]
    if valid.size == 0:
        return
    hist, edges = np.histogram(valid, bins=10, range=(0.0, 100.0))
    _log.debug(
        "cloud_builder histogram  edges=%s counts=%s",
        [int(e) for e in edges],
        hist.tolist(),
    )

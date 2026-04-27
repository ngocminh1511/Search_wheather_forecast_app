from __future__ import annotations

"""
cloud_reader.py — detect and read cloud cover GRIB2 fields.

Priority:
  1. Total cloud cover:   data/cloud_total/<run_id>/tcdc_entire_atmosphere/f<fff>.grib2
                          short names: tcc, tcdc
  2. Layered cloud cover: data/cloud_layered/<run_id>/{low,mid,high}_cloud/f<fff>.grib2
                          short names: lcc/lcdc, mcc/mcdc, hcc/hcdc

Returns a CloudFields dataclass describing what was loaded.
source attribute: "total" | "layered" | "none"

Debug logging emits min/max/NaN info per field and which source is used.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .grib_reader import GribField, read_first_field

_log = logging.getLogger(__name__)

# Short-name candidates per product — tried in order
_TOTAL_SHORT_NAMES = ["tcc", "tcdc"]
_LOW_SHORT_NAMES = ["lcc", "lcdc"]
_MID_SHORT_NAMES = ["mcc", "mcdc"]
_HIGH_SHORT_NAMES = ["hcc", "hcdc"]


# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------

@dataclass
class CloudFields:
    """Holds whatever cloud GRIB fields were successfully loaded."""

    total: GribField | None  # TCDC — entire atmosphere total cloud cover
    low:   GribField | None  # LCDC — low cloud layer
    mid:   GribField | None  # MCDC — mid cloud layer
    high:  GribField | None  # HCDC — high cloud layer
    # "total" | "layered" | "none"
    source: str

    @property
    def lat(self) -> np.ndarray:
        """Reference lat array from whichever field is available."""
        for f in (self.total, self.low, self.mid, self.high):
            if f is not None:
                return f.lat
        raise ValueError("No cloud fields loaded — cannot get lat")

    @property
    def lon(self) -> np.ndarray:
        """Reference lon array from whichever field is available."""
        for f in (self.total, self.low, self.mid, self.high):
            if f is not None:
                return f.lon
        raise ValueError("No cloud fields loaded — cannot get lon")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _try_read(file_path: Path, short_names: list[str]) -> GribField | None:
    """
    Try to read the GRIB2 file with each short_name in order.
    Falls back to filter-free open if all candidates fail.
    Returns None if the file cannot be read at all.
    """
    for sn in short_names:
        try:
            field = read_first_field(
                file_path, filter_by_keys={"shortName": sn})
            _log.debug("cloud_reader: loaded %s  short_name=%s  shape=%s",
                       file_path.name, sn, field.values.shape)
            return field
        except Exception:
            pass

    # Fallback: open without filter
    try:
        field = read_first_field(file_path)
        _log.debug("cloud_reader: loaded %s (no filter)  shape=%s",
                   file_path.name, field.values.shape)
        return field
    except Exception as exc:
        _log.debug("cloud_reader: cannot read %s — %s", file_path.name, exc)
        return None


def _usable(path: Path) -> bool:
    """True if the path exists and is non-empty."""
    return path.exists() and path.stat().st_size > 0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_cloud_fields(
    run_id: str,
    fff: int,
    data_dir: Path,
) -> CloudFields:
    """
    Detect and load available cloud GRIB fields for (run_id, fff).

    Tries cloud_total product first (TCDC entire atmosphere).
    Falls back to cloud_layered products (LCDC / MCDC / HCDC).
    Never raises — returns CloudFields(source="none") if nothing is available.

    Debug logging includes:
      - which source is used
      - min/max per loaded field
      - NaN% per loaded field
    """
    total_grib = (
        data_dir / "cloud_total" / run_id
        / "tcdc_entire_atmosphere" / f"f{fff:03d}.grib2"
    )
    low_grib = data_dir / "cloud_layered" / \
        run_id / "low_cloud" / f"f{fff:03d}.grib2"
    mid_grib = data_dir / "cloud_layered" / \
        run_id / "mid_cloud" / f"f{fff:03d}.grib2"
    high_grib = data_dir / "cloud_layered" / \
        run_id / "high_cloud" / f"f{fff:03d}.grib2"

    # ── Priority 1: total cloud cover ────────────────────────────────────
    if _usable(total_grib):
        field = _try_read(total_grib, _TOTAL_SHORT_NAMES)
        if field is not None:
            nan_pct = float(np.isnan(field.values).mean() * 100.0)
            _log.debug(
                "cloud_reader: source=total  run=%s fff=%03d "
                "min=%.1f max=%.1f nan_pct=%.1f%%",
                run_id, fff,
                float(np.nanmin(field.values)),
                float(np.nanmax(field.values)),
                nan_pct,
            )
            return CloudFields(
                total=field, low=None, mid=None, high=None, source="total"
            )
        _log.warning(
            "cloud_reader: GRIB exists but unreadable — %s", total_grib
        )

    # ── Priority 2: layered cloud cover ──────────────────────────────────
    low = _try_read(low_grib,  _LOW_SHORT_NAMES) if _usable(low_grib) else None
    mid = _try_read(mid_grib,  _MID_SHORT_NAMES) if _usable(mid_grib) else None
    high = _try_read(high_grib, _HIGH_SHORT_NAMES) if _usable(
        high_grib) else None

    if any(f is not None for f in (low, mid, high)):
        for label, f in [("low", low), ("mid", mid), ("high", high)]:
            if f is not None:
                nan_pct = float(np.isnan(f.values).mean() * 100.0)
                _log.debug(
                    "cloud_reader: layer=%-4s min=%.1f max=%.1f nan_pct=%.1f%%",
                    label,
                    float(np.nanmin(f.values)),
                    float(np.nanmax(f.values)),
                    nan_pct,
                )
            else:
                _log.debug("cloud_reader: layer=%-4s missing (no GRIB)", label)
        _log.debug(
            "cloud_reader: source=layered  run=%s fff=%03d", run_id, fff
        )
        return CloudFields(
            total=None, low=low, mid=mid, high=high, source="layered"
        )

    # ── Nothing found ────────────────────────────────────────────────────
    _log.warning(
        "cloud_reader: source=none  run=%s fff=%03d  "
        "(checked total_grib=%s, layered dir=%s)",
        run_id, fff,
        total_grib,
        data_dir / "cloud_layered" / run_id,
    )
    return CloudFields(total=None, low=None, mid=None, high=None, source="none")

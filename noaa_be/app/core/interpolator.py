from __future__ import annotations

"""
interpolator.py — linear interpolation of GFS precipitation fields for sub-hour frames.

GFS provides PRATE/CRAIN/CSNOW at 1-hour intervals (f001–f003).
This module generates 15-minute intermediate frames by linear interpolation,
allowing rain_advanced to show smooth animation at 15-min resolution for the
first 3h of the forecast.

Frame naming convention for interpolated tiles:
  {hours:03d}_{minutes:02d}  e.g.  001_15, 001_30, 001_45

The standard GFS hourly frames keep their original naming:
  {fff:03d}  e.g.  001, 002, 003

Interpolation strategy:
  - PRATE (precipitation rate): pure linear interpolation, clamp to ≥ 0
  - CRAIN, CSNOW, CICEP, CFRZR (binary 0/1): linear interpolation → soft
    probability values; the classifier thresholds handle the soft boundary
  - CPOFP (0–100 %): pure linear interpolation, clamp to [0, 100]

Backward extrapolation for the 000_xx frames (before f001):
  Uses a virtual "f000" estimated as 2*f001 - f002 (linear backward extrapolation),
  clamped to non-negative for PRATE and [0,1] for binary fields.
  This gives a physically plausible approach-to-current-time estimate.
"""

import logging
from dataclasses import dataclass

import numpy as np

from .precip_reader import PrecipFields

_log = logging.getLogger(__name__)


@dataclass
class InterpFrame:
    """A single interpolated precipitation frame."""
    hours: int        # integer hours component (e.g. 1 for 1h15m)
    minutes: int      # integer minutes component (0, 15, 30, 45)
    fields: PrecipFields

    @property
    def path_name(self) -> str:
        """Directory name used for tile storage: '{hours:03d}_{minutes:02d}'."""
        return f"{self.hours:03d}_{self.minutes:02d}"


def _lerp_field(a: np.ndarray, b: np.ndarray, alpha: float) -> np.ndarray:
    """Linear interpolation: a + alpha * (b - a), result as float32."""
    return (a + alpha * (b.astype(np.float32) - a.astype(np.float32))).astype(np.float32)


def _extrapolate_back(f1: np.ndarray, f2: np.ndarray) -> np.ndarray:
    """Estimate virtual f0 by linear extrapolation: 2*f1 - f2."""
    return (2.0 * f1.astype(np.float32) - f2.astype(np.float32)).astype(np.float32)


def _make_fields(
    f_low: PrecipFields,
    f_high: PrecipFields,
    alpha: float,
) -> PrecipFields:
    """Interpolate all precipitation fields at position alpha in [0, 1]."""
    prate = np.clip(_lerp_field(f_low.prate, f_high.prate, alpha), 0.0, None)
    crain = np.clip(_lerp_field(f_low.crain, f_high.crain, alpha), 0.0, 1.0)
    csnow = np.clip(_lerp_field(f_low.csnow, f_high.csnow, alpha), 0.0, 1.0)
    cicep = np.clip(_lerp_field(f_low.cicep, f_high.cicep, alpha), 0.0, 1.0)
    cfrzr = np.clip(_lerp_field(f_low.cfrzr, f_high.cfrzr, alpha), 0.0, 1.0)
    cpofp = np.clip(_lerp_field(f_low.cpofp, f_high.cpofp, alpha), 0.0, 100.0)
    return PrecipFields(
        prate=prate,
        crain=crain,
        csnow=csnow,
        cicep=cicep,
        cfrzr=cfrzr,
        cpofp=cpofp,
        lat=f_low.lat,
        lon=f_low.lon,
    )


def interpolate_hour_pair(
    f_low: PrecipFields,
    f_high: PrecipFields,
    base_hour: int,
    substeps: int = 3,
) -> list[InterpFrame]:
    """Generate intra-hour interpolated frames between two consecutive 1-hour GFS frames.

    Args:
        f_low:     PrecipFields at ``base_hour`` (e.g. f001 = 1h).
        f_high:    PrecipFields at ``base_hour + 1`` (e.g. f002 = 2h).
        base_hour: Integer hour of f_low (e.g. 1 for f001).
        substeps:  Number of sub-frames per hour (3 → 15, 30, 45 min marks).

    Returns:
        List of InterpFrame at 15-min, 30-min, 45-min marks within the hour.
    """
    frames: list[InterpFrame] = []
    for step in range(1, substeps + 1):
        alpha = step / (substeps + 1)          # 0.25, 0.50, 0.75
        minutes = int(round(60 * alpha))        # 15, 30, 45
        fields = _make_fields(f_low, f_high, alpha)
        frames.append(InterpFrame(hours=base_hour, minutes=minutes, fields=fields))
        _log.debug(
            "Interpolated frame %03d_%02d (alpha=%.3f)  prate_max=%.3f mm/h",
            base_hour, minutes, alpha, float(np.nanmax(fields.prate)),
        )
    return frames


def generate_rain_advanced_interp_frames(
    sources: dict[int, PrecipFields],
    substeps: int = 3,
    anchor_start: int = 6,
    anchor_end: int = 15,
) -> list[InterpFrame]:
    """Generate interpolated 15-min frames for rain_advanced sliding-"now" animation.

    Per user spec: "3h /15min from NOW" must be valid for any "now" ∈ [f_live, f_next_live].
    Default range f006–f015 covers "now" ∈ [f006, f012] with 3h forward window each.

    Generates 27 sub-frames covering 006_15 to 014_45 at 15-min resolution:
      For each consecutive anchor pair (f006,f007), (f007,f008), …, (f014,f015):
        produce 3 sub-frames at 15, 30, 45 min marks.
      → 9 pairs × 3 sub-frames = 27 frames total.

    User view at any "now":
      Picks 9 sub-frames in [now+0:15, now+2:45] from this superset.

    Args:
        sources: Dict mapping fff hour → PrecipFields, must contain {anchor_start..anchor_end}.
        substeps: Sub-frames per hour (default 3 → 15-min).
        anchor_start: First hour anchor (default 6 = live moment).
        anchor_end: Last hour anchor (default 15 = f012 + 3h forward).

    Returns:
        Sorted list of InterpFrame (006_15 … 014_45 by default).
    """
    missing = [h for h in range(anchor_start, anchor_end + 1) if h not in sources]
    if missing:
        raise ValueError(
            f"generate_rain_advanced_interp_frames: missing anchor hours {missing}; "
            f"need full range f{anchor_start:03d}–f{anchor_end:03d}."
        )

    frames: list[InterpFrame] = []
    for low_hour in range(anchor_start, anchor_end):
        high_hour = low_hour + 1
        frames.extend(
            interpolate_hour_pair(
                sources[low_hour],
                sources[high_hour],
                base_hour=low_hour,
                substeps=substeps,
            )
        )

    _log.info(
        "generate_rain_advanced_interp_frames: produced %d frames "
        "(f%03d_%02d to f%03d_%02d at %d-min resolution)",
        len(frames),
        anchor_start,
        60 // (substeps + 1),
        anchor_end - 1,
        60 - 60 // (substeps + 1),
        60 // (substeps + 1),
    )
    return frames

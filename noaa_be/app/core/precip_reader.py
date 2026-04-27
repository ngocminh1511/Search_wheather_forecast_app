from __future__ import annotations

"""
precip_reader.py — read precipitation-type GFS fields from a single GRIB2 file.

Expected GRIB2 file contains 3 messages (all at surface level):
  PRATE  — precipitation rate (kg m-2 s-1) → normalized to mm/h
  CRAIN  — categorical rain   (binary 0/1)
  CSNOW  — categorical snow   (binary 0/1)

All fields share the same lat/lon grid (GFS 0.25°).
"""

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .grib_reader import GribField, read_multi_fields

_log = logging.getLogger(__name__)

_PRATE_NAMES = ["prate"]
_CRAIN_NAMES = ["crain"]
_CSNOW_NAMES = ["csnow"]
_CICEP_NAMES = ["cicep"]
_CFRZR_NAMES = ["cfrzr"]
_CPOFP_NAMES = ["cpofp"]

# GFS PRATE is stored in GRIB2 in SI units: kg m⁻² s⁻¹.
# 1 kg m⁻² s⁻¹  =  1 mm s⁻¹  =  3600 mm h⁻¹
# grib_reader.py ALREADY normalizes this to mm/h! Do not multiply again!


@dataclass
class PrecipFields:
    """Precipitation fields read from one GFS frame GRIB2 file."""
    prate:  np.ndarray          # float32 H×W mm/h — precipitation rate
    crain:  np.ndarray          # float32 H×W binary 0/1 — categorical rain
    csnow:  np.ndarray          # float32 H×W binary 0/1 — categorical snow
    cicep:  np.ndarray          # float32 H×W binary 0/1 — categorical ice pellets
    cfrzr:  np.ndarray          # float32 H×W binary 0/1 — categorical freezing rain
    cpofp:  np.ndarray          # float32 H×W percent 0-100 — percent of frozen precipitation
    lat:    np.ndarray          # (H,) degrees north, descending
    lon:    np.ndarray          # (W,) degrees east, 0→360


def read_precip_fields(grib_path: Path) -> PrecipFields:
    """
    Read PRATE, CRAIN, CSNOW fields from a GRIB2 file.

    Returns a PrecipFields dataclass.
    Raises ValueError if any mandatory field cannot be found.
    """
    all_names = _PRATE_NAMES + _CRAIN_NAMES + _CSNOW_NAMES + _CICEP_NAMES + _CFRZR_NAMES + _CPOFP_NAMES
    found = read_multi_fields(grib_path, all_names)

    def _pick(candidates: list[str], label: str, optional: bool = False) -> GribField | None:
        for name in candidates:
            if name in found:
                return found[name]
        if optional:
            return None
        raise ValueError(
            f"Cannot find mandatory {label!r} in {grib_path.name}. "
            f"Tried short names: {candidates}. "
            f"Available: {list(found.keys())}"
        )

    prate = _pick(_PRATE_NAMES, "PRATE")
    crain = _pick(_CRAIN_NAMES, "CRAIN")
    csnow = _pick(_CSNOW_NAMES, "CSNOW")
    
    # Optional fields (if not found, we will create zero-arrays)
    cicep = _pick(_CICEP_NAMES, "CICEP", optional=True)
    cfrzr = _pick(_CFRZR_NAMES, "CFRZR", optional=True)
    cpofp = _pick(_CPOFP_NAMES, "CPOFP", optional=True)

    lat = prate.lat
    lon = prate.lon

    p_vals = prate.values
    _log.info(
        "read_precip_fields: %s | PRATE mm/h: min=%.3f max=%.3f p50=%.3f p95=%.3f p99=%.3f",
        grib_path.name,
        float(np.nanmin(p_vals)),
        float(np.nanmax(p_vals)),
        float(np.nanpercentile(p_vals, 50)),
        float(np.nanpercentile(p_vals, 95)),
        float(np.nanpercentile(p_vals, 99)),
    )

    def _val(field: GribField | None) -> np.ndarray:
        if field is not None:
            return field.values.astype(np.float32)
        return np.zeros_like(p_vals, dtype=np.float32)

    return PrecipFields(
        prate=p_vals.astype(np.float32), # Already mm/h from grib_reader
        crain=_val(crain),
        csnow=_val(csnow),
        cicep=_val(cicep),
        cfrzr=_val(cfrzr),
        cpofp=_val(cpofp),
        lat=lat,
        lon=lon,
    )

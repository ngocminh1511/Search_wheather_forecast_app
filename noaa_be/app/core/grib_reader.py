from __future__ import annotations

"""
grib_reader.py — open a GRIB2 file with cfgrib, normalize units, return numpy arrays.

Unit normalization:
  TMP         : Kelvin  → °C        (- 273.15)
  APCP        : kg/m²   → mm        (× 1)
  PRATE       : kg/m²/s → mm/h      (× 3600)
  SNOD        : m       → cm        (× 100)
  TCDC/xCDC  : fraction → %        (× 100 if max < 2)
  UGRD/VGRD  : m/s      unchanged
  CRAIN/CSNOW: binary 0/1 unchanged
"""

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import xarray as xr


@dataclass
class GribField:
    variable: str       # short name used in MAP_SPECS (e.g. "tmp_2m")
    short_name: str     # GRIB short name (e.g. "t2m", "tp", ...)
    lat: np.ndarray     # 1-D, degrees north, descending (90→-90)
    lon: np.ndarray     # 1-D, degrees east, 0→360
    values: np.ndarray  # 2-D (lat × lon), normalized units
    unit: str           # human-readable unit string


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _squeeze_to_2d(arr: np.ndarray) -> np.ndarray:
    arr = np.squeeze(arr)
    if arr.ndim != 2:
        raise ValueError(f"Expected 2-D array after squeeze, got shape {arr.shape}")
    return arr


def _get_coord(ds: xr.Dataset, candidates: list[str]) -> np.ndarray:
    for name in candidates:
        if name in ds.coords:
            return ds[name].values
    raise ValueError(f"Cannot find coordinate from candidates {candidates} in dataset")


def _normalize(short_name: str, values: np.ndarray) -> tuple[np.ndarray, str]:
    """Return (normalized_values, unit_string)."""
    sn = short_name.lower()

    if sn in ("2t", "t2m", "tmp", "t"):
        # Kelvin → °C
        v = values - 273.15
        return v.astype(np.float32), "°C"

    if sn in ("tp", "apcp"):
        # kg/m² == mm — no conversion needed
        return values.astype(np.float32), "mm"

    if sn in ("prate",):
        # kg/m²/s → mm/h
        v = values * 3600.0
        return v.astype(np.float32), "mm/h"

    if sn in ("snod",):
        # m → cm
        v = values * 100.0
        return v.astype(np.float32), "cm"

    if sn in ("tcdc", "lcdc", "mcdc", "hcdc", "tcc", "lcc", "mcc", "hcc"):
        # fraction [0,1] → % [0,100]
        v = values.copy().astype(np.float32)
        if np.nanmax(v) <= 1.5:
            v = v * 100.0
        return np.clip(v, 0.0, 100.0), "%"

    if sn in ("ugrd", "u10", "u", "vgrd", "v10", "v"):
        return values.astype(np.float32), "m/s"

    if sn in ("crain", "csnow"):
        return values.astype(np.float32), "binary"

    # Fallback: return as-is
    return values.astype(np.float32), "raw"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def read_first_field(file_path: Path, filter_by_keys: dict | None = None) -> GribField:
    """
    Open a GRIB2 file and return the first variable found.
    Optionally filter with cfgrib backend_kwargs (e.g. {'shortName': 'prate'}).
    """
    open_kwargs: dict = {"engine": "cfgrib"}
    if filter_by_keys:
        open_kwargs["backend_kwargs"] = {"filter_by_keys": filter_by_keys}

    ds = xr.open_dataset(file_path, **open_kwargs)
    try:
        if not ds.data_vars:
            raise ValueError(f"No data variables in {file_path.name}")

        var_name = list(ds.data_vars)[0]
        da = ds[var_name]

        lat = _get_coord(ds, ["latitude", "lat"])
        lon = _get_coord(ds, ["longitude", "lon"])

        raw = _squeeze_to_2d(da.values)
        short_name = da.attrs.get("GRIB_shortName", var_name)
        values, unit = _normalize(short_name, raw)

        # Ensure lat is descending (North→South) for mercator consistency
        if lat[0] < lat[-1]:
            lat = lat[::-1]
            values = values[::-1, :]

        return GribField(
            variable=var_name,
            short_name=short_name,
            lat=lat.astype(np.float32),
            lon=lon.astype(np.float32),
            values=values,
            unit=unit,
        )
    finally:
        ds.close()


def read_multi_fields(
    file_path: Path,
    short_names: list[str],
) -> dict[str, GribField]:
    """
    Open a GRIB2 file containing multiple variables (e.g. rain_advanced:
    PRATE + CRAIN + CSNOW) and return a dict keyed by short_name.
    Falls back to cfgrib.open_datasets for multi-message files.
    """
    import cfgrib  # local import to keep startup fast if cfgrib not installed

    result: dict[str, GribField] = {}
    datasets = cfgrib.open_datasets(str(file_path))

    for sn in short_names:
        for ds in datasets:
            matched_var = None
            for vname in ds.data_vars:
                da = ds[vname]
                if da.attrs.get("GRIB_shortName", "").lower() == sn.lower():
                    matched_var = (vname, da)
                    break
            if matched_var is None:
                continue

            vname, da = matched_var
            lat = _get_coord(ds, ["latitude", "lat"])
            lon = _get_coord(ds, ["longitude", "lon"])
            raw = _squeeze_to_2d(da.values)
            values, unit = _normalize(sn, raw)

            if lat[0] < lat[-1]:
                lat = lat[::-1]
                values = values[::-1, :]

            result[sn] = GribField(
                variable=vname,
                short_name=sn,
                lat=lat.astype(np.float32),
                lon=lon.astype(np.float32),
                values=values,
                unit=unit,
            )
            break  # found this short_name, move to next

        for ds in datasets:
            ds.close()

    return result


def downsample_field(field: GribField, target_deg: float = 1.0) -> GribField:
    """
    Spatially downsample a GribField to the given degree resolution.
    Used for wind/rain_advanced JSON grids to reduce response size for mobile.
    """
    lat_step = field.lat[1] - field.lat[0]  # usually negative (descending)
    lon_step = field.lon[1] - field.lon[0]  # usually positive

    native_lat_res = abs(float(lat_step))
    native_lon_res = abs(float(lon_step))

    row_skip = max(1, round(target_deg / native_lat_res))
    col_skip = max(1, round(target_deg / native_lon_res))

    return GribField(
        variable=field.variable,
        short_name=field.short_name,
        lat=field.lat[::row_skip],
        lon=field.lon[::col_skip],
        values=field.values[::row_skip, ::col_skip],
        unit=field.unit,
    )

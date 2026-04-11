from __future__ import annotations

"""
grid_service.py — produce downsampled JSON grids for wind_animation and rain_advanced.

GRIB2 path convention (noaa_be internal):
  data/<map_type>/<run_id>/<product_name>/f<fff:03d>.grib2

Wind products map to MAP_SPECS Product names:
  wind_30m, wind_50m, wind_100m, wind_600mb, wind_300mb, wind_250mb, wind_200mb

Each product GRIB2 file contains UGRD + VGRD for that level.
"""

import json
import logging
from datetime import timedelta
from pathlib import Path

import numpy as np

from ..config import get_settings
from ..core.grib_reader import GribField, downsample_field, read_first_field, read_multi_fields
from ..services.availability_service import run_id_to_datetime

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Wind product names (match MAP_SPECS Product.name exactly)
# ---------------------------------------------------------------------------

# All wind product names in wind_animation MapSpec
_WIND_PRODUCTS: list[str] = [
    "wind_30m",
    "wind_50m",
    "wind_100m",
    "wind_600mb",
    "wind_300mb",
    "wind_250mb",
    "wind_200mb",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _grid_cache_path(map_type: str, run_id: str, fff: int, product: str, grids_dir: Path) -> Path:
    return grids_dir / map_type / run_id / f"{fff:03d}" / f"{product}.json"


def _valid_time_str(run_id: str, fff: int) -> str:
    dt = run_id_to_datetime(run_id) + timedelta(hours=fff)
    return dt.strftime("%Y-%m-%dT%H:%MZ")


def _field_to_list(field: GribField) -> list[list[float]]:
    """Convert 2-D numpy array → Python nested list (JSON serializable)."""
    return np.round(field.values, 4).tolist()


# ---------------------------------------------------------------------------
# Wind grid
# ---------------------------------------------------------------------------

def _read_wind_fields(grib_path: Path) -> tuple[GribField, GribField]:
    """
    Read UGRD + VGRD from a wind product GRIB2 file.
    Each wind product file (e.g. wind_30m/f000.grib2) contains only one level,
    so we read by shortName without additional level filtering.
    """
    u = read_first_field(grib_path, filter_by_keys={"shortName": "u"})
    v = read_first_field(grib_path, filter_by_keys={"shortName": "v"})
    return u, v


def generate_wind_grid(
    run_id: str,
    fff: int,
    product_name: str,
    data_dir: Path,
    grids_dir: Path,
    downsample_deg: float = 1.0,
    bbox: tuple[float, float, float, float] | None = None,
) -> dict:
    """
    Generate wind JSON grid for one (run_id, fff, product_name).
    Reads from: data_dir/wind_animation/<run_id>/<product_name>/f<fff:03d>.grib2
    """
    cache_path = _grid_cache_path("wind_animation", run_id, fff, product_name, grids_dir)
    if cache_path.exists():
        return json.loads(cache_path.read_text())

    grib_file = data_dir / "wind_animation" / run_id / product_name / f"f{fff:03d}.grib2"
    u_field, v_field = _read_wind_fields(grib_file)

    u_ds = downsample_field(u_field, downsample_deg)
    v_ds = downsample_field(v_field, downsample_deg)
    speed = np.sqrt(u_ds.values ** 2 + v_ds.values ** 2)

    if bbox is not None:
        u_ds, v_ds, speed = _crop_bbox(u_ds, v_ds, speed, bbox)

    payload = {
        "lat": u_ds.lat.tolist(),
        "lon": u_ds.lon.tolist(),
        "u": np.round(u_ds.values, 3).tolist(),
        "v": np.round(v_ds.values, 3).tolist(),
        "speed_max": float(np.nanmax(speed)),
        "unit": "m/s",
        "valid_time": _valid_time_str(run_id, fff),
        "product": product_name,
    }

    _save_cache(cache_path, payload)
    return payload


# ---------------------------------------------------------------------------
# Rain advanced grid
# ---------------------------------------------------------------------------

def generate_rain_advanced_grid(
    run_id: str,
    fff: int,
    data_dir: Path,
    grids_dir: Path,
    downsample_deg: float = 1.0,
    bbox: tuple[float, float, float, float] | None = None,
) -> dict:
    """
    Generate rain_advanced JSON grid.
    Reads from: data_dir/rain_advanced/<run_id>/rain_adv_surface/f<fff:03d>.grib2
    """
    product = "rain_adv_surface"
    cache_path = _grid_cache_path("rain_advanced", run_id, fff, "rain_advanced", grids_dir)
    if cache_path.exists():
        return json.loads(cache_path.read_text())

    grib_file = data_dir / "rain_advanced" / run_id / product / f"f{fff:03d}.grib2"
    fields = read_multi_fields(grib_file, ["prate", "crain", "csnow"])

    prate_f = fields.get("prate")
    if prate_f is None:
        raise ValueError(f"PRATE not found in {grib_file}")

    prate = downsample_field(prate_f, downsample_deg)
    crain = downsample_field(fields["crain"], downsample_deg) if "crain" in fields else prate
    csnow = downsample_field(fields["csnow"], downsample_deg) if "csnow" in fields else prate

    payload = {
        "lat": prate.lat.tolist(),
        "lon": prate.lon.tolist(),
        "prate":  np.round(prate.values, 4).tolist(),
        "crain":  np.round(crain.values, 1).tolist(),
        "csnow":  np.round(csnow.values, 1).tolist(),
        "unit": "mm/h",
        "valid_time": _valid_time_str(run_id, fff),
        "product": "rain_advanced",
    }

    _save_cache(cache_path, payload)
    return payload


# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def generate_grid(
    map_type: str,
    run_id: str,
    fff: int,
    product: str,
    data_dir: Path | None = None,
    grids_dir: Path | None = None,
    downsample_deg: float = 1.0,
    bbox: tuple[float, float, float, float] | None = None,
) -> dict:
    """Entry point — routes to wind or rain_advanced generator."""
    cfg = get_settings()
    d_dir = data_dir or cfg.DATA_DIR
    g_dir = grids_dir or cfg.JSON_GRIDS_DIR

    if map_type == "wind_animation":
        return generate_wind_grid(
            run_id=run_id,
            fff=fff,
            product_name=product,
            data_dir=d_dir,
            grids_dir=g_dir,
            downsample_deg=downsample_deg,
            bbox=bbox,
        )
    if map_type == "rain_advanced":
        return generate_rain_advanced_grid(
            run_id=run_id,
            fff=fff,
            data_dir=d_dir,
            grids_dir=g_dir,
            downsample_deg=downsample_deg,
            bbox=bbox,
        )
    raise ValueError(f"grid_service does not handle map_type={map_type!r}")


# ---------------------------------------------------------------------------
# Internal utilities
# ---------------------------------------------------------------------------

def _save_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, separators=(",", ":")))
    tmp.rename(path)


def _crop_bbox(u: GribField, v: GribField, speed: np.ndarray, bbox: tuple) -> tuple:
    west, south, east, north = bbox
    lat_mask = (u.lat >= south) & (u.lat <= north)
    lon_mask = (u.lon >= west) & (u.lon <= east)

    import dataclasses
    u_crop = dataclasses.replace(
        u,
        lat=u.lat[lat_mask],
        lon=u.lon[lon_mask],
        values=u.values[np.ix_(lat_mask, lon_mask)],
    )
    v_crop = dataclasses.replace(
        v,
        lat=v.lat[lat_mask],
        lon=v.lon[lon_mask],
        values=v.values[np.ix_(lat_mask, lon_mask)],
    )
    speed_crop = speed[np.ix_(lat_mask, lon_mask)]
    return u_crop, v_crop, speed_crop

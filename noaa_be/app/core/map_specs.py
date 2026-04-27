from __future__ import annotations

"""
map_specs.py — single source of truth for all 7 map types.

Ported directly from scripts/noaa_map_pipeline.py — noaa_be is fully self-contained.

Naming:
  Product.name   → used as sub-folder name under data/<map_type>/
  Product.query  → NOAA filter_gfs_0p25.pl query parameters for selective download
  MapSpec.fff_segments_full → full GFS forecast range (not the 24h window used in BE)
"""

from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class Product:
    name: str
    query: Dict[str, str]


@dataclass(frozen=True)
class MapSpec:
    map_type: str
    description: str
    products: List[Product]
    fff_segments_full: List[Tuple[int, int, int]]


# ---------------------------------------------------------------------------
# NOAA NOMADS filter endpoint
# ---------------------------------------------------------------------------

NOMADS_BASE_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
NOMADS_COMMON_QUERY: Dict[str, str] = {
    "leftlon": "0",
    "rightlon": "360",
    "toplat": "90",
    "bottomlat": "-90",
}


# ---------------------------------------------------------------------------
# MAP_SPECS registry
# ---------------------------------------------------------------------------

MAP_SPECS: Dict[str, MapSpec] = {
    "temperature_feels_like": MapSpec(
        map_type="temperature_feels_like",
        description="TMP at 2m — full forecast: 0-120h hourly, then 3-hourly to 384h",
        products=[
            Product(
                name="tmp_2m",
                query={"lev_2_m_above_ground": "on", "var_TMP": "on"},
            ),
        ],
        fff_segments_full=[(0, 120, 1), (123, 384, 3)],
    ),

    "rain_basic": MapSpec(
        map_type="rain_basic",
        description="APCP at surface — 24h every 3h (f003–f024; no APCP at f000)",
        products=[
            Product(
                name="apcp_surface",
                query={"lev_surface": "on", "var_APCP": "on"},
            ),
        ],
        fff_segments_full=[(3, 24, 3)],
    ),

    "rain_advanced": MapSpec(
        map_type="rain_advanced",
        description="PRATE + CRAIN + CSNOW (+ CICEP/CFRZR/CPOFP) at surface — 24h every 3h (f003–f024)",
        products=[
            Product(
                name="rain_adv_surface",
                query={
                    "lev_surface": "on",
                    "var_PRATE": "on",
                    "var_CRAIN": "on",
                    "var_CSNOW": "on",
                    "var_CICEP": "on",
                    "var_CFRZR": "on",
                    "var_CPOFP": "on",
                },
            ),
        ],
        fff_segments_full=[(3, 24, 3)],
    ),

    "cloud_total": MapSpec(
        map_type="cloud_total",
        description="TCDC entire atmosphere — analysis snapshot per cycle (f000)",
        products=[
            Product(
                name="tcdc_entire_atmosphere",
                query={"lev_entire_atmosphere": "on", "var_TCDC": "on"},
            ),
        ],
        fff_segments_full=[(0, 0, 1)],
    ),

    "cloud_layered": MapSpec(
        map_type="cloud_layered",
        description="LCDC / MCDC / HCDC split by layer — analysis snapshot per cycle (f000)",
        products=[
            Product(
                name="low_cloud",
                query={"lev_low_cloud_layer": "on", "var_LCDC": "on"},
            ),
            Product(
                name="mid_cloud",
                query={"lev_middle_cloud_layer": "on", "var_MCDC": "on"},
            ),
            Product(
                name="high_cloud",
                query={"lev_high_cloud_layer": "on", "var_HCDC": "on"},
            ),
        ],
        fff_segments_full=[(0, 0, 1)],
    ),

    "snow_depth": MapSpec(
        map_type="snow_depth",
        description="SNOD surface — ~10 days: 0-48h/3h, then 6h to 54h, then 12h to 240h",
        products=[
            Product(
                name="snod_surface",
                query={"lev_surface": "on", "var_SNOD": "on"},
            ),
        ],
        fff_segments_full=[(0, 48, 3), (54, 54, 6), (66, 240, 12)],
    ),

    "wind_animation": MapSpec(
        map_type="wind_animation",
        description="U/V winds for multiple altitude layers — 16 days: 0-120h hourly, then 3-hourly",
        products=[
            Product(
                name="wind_30m",
                query={
                    "lev_30_m_above_ground": "on",
                    "var_UGRD": "on",
                    "var_VGRD": "on",
                },
            ),
            Product(
                name="wind_50m",
                query={
                    "lev_50_m_above_ground": "on",
                    "var_UGRD": "on",
                    "var_VGRD": "on",
                },
            ),
            Product(
                name="wind_100m",
                query={
                    "lev_100_m_above_ground": "on",
                    "var_UGRD": "on",
                    "var_VGRD": "on",
                },
            ),
            Product(
                name="wind_600mb",
                query={"lev_600_mb": "on", "var_UGRD": "on", "var_VGRD": "on"},
            ),
            Product(
                name="wind_300mb",
                query={"lev_300_mb": "on", "var_UGRD": "on", "var_VGRD": "on"},
            ),
            Product(
                name="wind_250mb",
                query={"lev_250_mb": "on", "var_UGRD": "on", "var_VGRD": "on"},
            ),
            Product(
                name="wind_200mb",
                query={"lev_200_mb": "on", "var_UGRD": "on", "var_VGRD": "on"},
            ),
        ],
        fff_segments_full=[(0, 120, 1), (123, 384, 3)],
    ),

    "wind_surface": MapSpec(
        map_type="wind_surface",
        description="Animated Wind Map at 10m height (Base PNG + Field Bin)",
        products=[
            Product(
                name="wind_10m",
                query={
                    "lev_10_m_above_ground": "on",
                    "var_UGRD": "on",
                    "var_VGRD": "on",
                },
            ),
        ],
        fff_segments_full=[(0, 120, 1), (123, 384, 3)],
    ),
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def segment_fff(segments: List[Tuple[int, int, int]]) -> List[int]:
    """Expand [(start, end, step), ...] → sorted list of unique fff values."""
    values: set[int] = set()
    for start, end, step in segments:
        for v in range(start, end + 1, step):
            values.add(v)
    return sorted(values)


def resolve_fff(map_type: str, mode: str = "full") -> List[int]:
    spec = MAP_SPECS[map_type]
    if mode in ("init_only", "f000_only"):
        return [0]
    return segment_fff(spec.fff_segments_full)

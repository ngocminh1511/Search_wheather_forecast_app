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
from typing import Dict, List, Optional, Tuple


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
    # Multi-tier cold-zone optimisation.
    # cold_tiers: ((fff_min, max_age_h[, stagger_n]), ...) sorted by fff_min ascending.
    #   A frame f belongs to the LAST tier whose fff_min <= f.
    #   Frames below the first tier's fff_min are "hot" (always regenerated).
    #   stagger_n (optional, default 1): split tier frames into N rotating groups.
    #     Each cycle refreshes group (cycle_slot % stagger_n), where cycle_slot = run_hour//6.
    #     stagger_n=1 → standard: refresh when run_hour % max_age_h == 0.
    # Example:  ((73, 12), (121, 24, 4))
    #   f073–f120  → refresh 2×/day  (00z + 12z)
    #   f121+      → split into 4 groups, each refreshed at one cycle slot per day
    # Empty tuple = no cold zones (all frames always regenerated).
    cold_tiers: tuple = ()

    # ── Backward-compat properties (single-tier code still works) ──────────
    @property
    def cold_fff_min(self) -> int:
        """Lowest cold boundary (first tier). 9999 = no cold zones."""
        return self.cold_tiers[0][0] if self.cold_tiers else 9999

    @property
    def cold_max_age_h(self) -> int:
        """Max age of the first (nearest) cold tier."""
        return self.cold_tiers[0][1] if self.cold_tiers else 24


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
        description=(
            "TMP at 2m — user window 72h (24h/1h + 48h/3h). "
            "Trim 6h đầu (live time), extend +12h mỗi segment (slide buffer). "
            "Storage: f006–f036 step 1h (31fr) + f039–f084 step 3h (16fr) = 47 frames."
        ),
        products=[
            Product(
                name="tmp_2m",
                query={"lev_2_m_above_ground": "on", "var_TMP": "on"},
            ),
        ],
        # Trim: bỏ f001–f005 (past at live +6h)
        # Extend mỗi segment +12h để cover slide "now" từ f006 → f012:
        #   Seg 1 step 1h: f006–f036 (was f001–f024, end +12h)
        #   Seg 2 step 3h: f039–f084 (start shifted vì f027–f036 nằm trong seg 1, end +0h)
        fff_segments_full=[(6, 36, 1), (39, 84, 3)],
        # Cách 2 hot/cold:
        #   HOT (f006–f036, 0–24h forward): refresh 4×/day
        #   COLD-12h (f039–f072, 24–66h forward): refresh 2×/day
        #   COLD-24h (f075–f084, 66h+ buffer): refresh 1×/day (vùng xa, ít quan trọng)
        cold_tiers=((37, 12), (75, 24)),
    ),

    "rain_basic": MapSpec(
        map_type="rain_basic",
        description=(
            "APCP at surface — user window 24h (step 3h). "
            "Trim 6h đầu, extend +12h cuối. "
            "Storage: f006–f036 step 3h = 11 frames."
        ),
        products=[
            Product(
                name="apcp_surface",
                query={"lev_surface": "on", "var_APCP": "on"},
            ),
        ],
        # Trim: bỏ f003 (past at live +6h)
        # Extend +12h cuối: end giữ f036 (user 24h + 12h buffer đã có sẵn)
        fff_segments_full=[(6, 36, 3)],
        # Cách 2 hot/cold:
        #   HOT (f006–f024, 0–18h forward): refresh 4×/day
        #   COLD-12h (f027–f036, buffer zone): refresh 2×/day
        cold_tiers=((27, 12),),
    ),

    "rain_advanced": MapSpec(
        map_type="rain_advanced",
        description=(
            "PRATE + CRAIN + CSNOW (+ CICEP/CFRZR/CPOFP) — user window 27h FROM NOW. "
            "3h /15min interp + 24h /3h std forward from now. "
            "Source: f006–f015 step 1h (10fr) + f018–f039 step 3h (8fr). "
            "Plus 27 interp sub-frames (006_15..014_45) generated sliding với 'now' ∈ [f006,f012]."
        ),
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
        # Trim 6h, slide 6h. Interp window slides với "now":
        #   Seg 1 step 1h (anchor cho 15-min interp): f006–f015 (10 frames)
        #     → cover [now, now+3h] cho mọi now ∈ [f006, f012]
        #   Seg 2 step 3h (standard 3-27h forward): f018–f039 (8 frames)
        #     → cover [now+3, now+27] cho mọi now ∈ [f006, f012]
        # f009, f012, f015 dùng được cho cả interp anchor lẫn standard step 3h (downloaded once).
        fff_segments_full=[(6, 15, 1), (18, 39, 3)],
        # Cách 2 hot/cold (mirror temperature pattern):
        #   HOT (f006–f036, all frames within 24h of any "now"): refresh 4×/day
        #   COLD-12h (f039, buffer zone): refresh 2×/day
        cold_tiers=((37, 12),),
    ),

    "snow_depth": MapSpec(
        map_type="snow_depth",
        description=(
            "SNOD surface — user window 228h (48h/3h + 12h/6h + 168h/12h). "
            "Trim 6h đầu, extend +12h mỗi segment. "
            "Storage: f006–f060 step 3h + f054–f072 step 6h + f072–f240 step 12h = 35 frames."
        ),
        products=[
            Product(
                name="snod_surface",
                query={"lev_surface": "on", "var_SNOD": "on"},
            ),
        ],
        # Trim: bỏ f003 (past at live +6h)
        # Extend mỗi segment +12h:
        #   Seg 1 (3h): f006–f060 (was f003–f048)
        #   Seg 2 (6h): f054–f072 (was f054–f060)
        #   Seg 3 (12h): f072–f240 (was f072–f228)
        fff_segments_full=[(6, 60, 3), (54, 72, 6), (72, 240, 12)],
        # Cách 2 hot/cold:
        #   HOT (f006–f024, 0–18h forward): refresh 4×/day
        #   COLD-12h (f027–f071, 24–66h forward): refresh 2×/day
        #   COLD-24h (f072+, 66h+ snow tích lũy chậm): refresh 1×/day
        cold_tiers=((27, 12), (72, 24)),
    ),

    "wind_surface": MapSpec(
        map_type="wind_surface",
        description=(
            "Animated Wind Map at 10m (Base PNG + Field WFLD) — user window 14 ngày. "
            "168h/1h + 168h/3h = 224 user frames. Trim 6h, extend +12h mỗi segment. "
            "Storage: f006–f180 step 1h (175fr) + f183–f348 step 3h (56fr) = 231 frames."
        ),
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
        # User spec: 14 ngày = 168h/1h + 168h/3h = 224 frames
        # Trim 6h đầu, extend +12h mỗi segment:
        #   Seg 1 (1h): f006–f180 (was f001–f120, mở rộng 5d→7.5d step 1h)
        #   Seg 2 (3h): f183–f348 (was f123–f240, mở rộng 5d→7d step 3h)
        # KHÔNG dùng segment 3 step 12h (user spec không có).
        fff_segments_full=[(6, 180, 1), (183, 348, 3)],
        # Cách 2 hot/cold:
        #   HOT (f006–f036, 0–24h step 1h): refresh 4×/day
        #   COLD-12h-stagger2 (f037–f180, 24h–7.5d step 1h): 2×/day, tải đều 2 cycles
        #   COLD-24h-stagger4 (f183–f348, 7.5d–14.5d step 3h): 1×/day, tải đều 4 cycles
        cold_tiers=((37, 12, 2), (183, 24, 4)),
    ),
}


# ---------------------------------------------------------------------------
# Rolling window buffer constants (informational — used by scheduler)
# ---------------------------------------------------------------------------

#: Extra hours downloaded beyond the user window so that users always have
#: enough frames even when waiting for the next GFS cycle.
#:   buffer_h = CYCLE_INTERVAL(6h) + NOAA_UPLOAD(5h) + PROC_BUFFER(1h) = 12h
ROLLING_BUFFER_H: int = 12

#: Per-map user-facing window (hours). Frames beyond this are buffer only.
USER_WINDOW_H: Dict[str, int] = {
    "temperature_feels_like": 72,
    "rain_basic":             24,
    "rain_advanced":          24,   # + first 3h at 15-min via interpolation
    "snow_depth":             228,  # 32 user frames → ~228h
    "wind_surface":           336,  # 14 days user window
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def tier_max_age_for_fff(spec: "MapSpec", fff: int) -> Optional[int]:
    """Return max_age_h of the governing cold tier, or None if hot."""
    result: Optional[int] = None
    for tier in spec.cold_tiers:
        if fff >= tier[0]:
            result = tier[1]
    return result


def tier_info_for_fff(spec: "MapSpec", fff: int) -> Optional[Tuple[int, int]]:
    """
    Return (max_age_h, stagger_n) for the governing cold tier, or None if hot.
    stagger_n defaults to 1 when not specified in the tier tuple.
    """
    result: Optional[Tuple[int, int]] = None
    for tier in spec.cold_tiers:
        if fff >= tier[0]:
            result = (tier[1], tier[2] if len(tier) > 2 else 1)
    return result


def tier_frame_groups(spec: "MapSpec", fffs: List[int]) -> Dict[int, int]:
    """
    Precompute stagger group index (0..stagger_n-1) for each cold frame in fffs.
    Frames within the same tier are sorted and assigned groups round-robin so
    load is spread evenly across cycle slots.  Hot frames are excluded.
    Returns {fff: group}.
    """
    tier_frames: Dict[int, List[int]] = {}
    for f in sorted(fffs):
        governing: Optional[int] = None
        for i, tier in enumerate(spec.cold_tiers):
            if f >= tier[0]:
                governing = i
        if governing is None:
            continue
        tier_frames.setdefault(governing, []).append(f)

    groups: Dict[int, int] = {}
    for tier_idx, frames in tier_frames.items():
        tier = spec.cold_tiers[tier_idx]
        stagger_n = tier[2] if len(tier) > 2 else 1
        for i, f in enumerate(frames):
            groups[f] = i % stagger_n
    return groups


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

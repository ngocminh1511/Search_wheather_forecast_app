"""Build timeline metadata that does NOT depend on server "now".

The output is suitable for:
- BE timeline API (router adds now_offset_hours + is_past after calling this)
- Uploading to Bunny CDN as _timeline.json (FE computes now/is_past client-side)
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from ..core.map_specs import MAP_SPECS, segment_fff
from .availability_service import (
    json_grid_ready,
    load_map_availability,
    run_id_to_datetime,
    tiles_ready,
)
from .tile_generator import _MAP_PRODUCTS


def build_timeline_static(
    map_type: str,
    run_id: str,
    cfg: Any,
    bunny_run_ready: bool = False,
) -> dict:
    """Build timeline metadata as a plain dict.

    Returns None-equivalent (empty frames) if no spec frames are available.
    The caller decides how to handle that (404, skip upload, etc.).

    Fields:
      - map_type, run_id, run_time
      - window_start_time, window_end_time
      - segments[]: step structure from MAP_SPECS
      - frames[]: {fff, label, valid_time, tiles_ready, has_json_grid}
                  NOTE: is_past is NOT included — client computes it.
    """
    avail = load_map_availability(run_id, map_type)
    spec_set = set(segment_fff(MAP_SPECS[map_type].fff_segments_full))
    fffs = sorted(f for f in avail if f in spec_set)

    run_dt = run_id_to_datetime(run_id)
    products = _MAP_PRODUCTS.get(map_type, ["default"])
    first_product = products[0]

    frames: list[dict] = []
    for fff in fffs:
        valid_dt = run_dt + timedelta(hours=fff)
        tr = bunny_run_ready or tiles_ready(
            map_type, run_id, fff, first_product, cfg.TILES_DIR
        )
        has_grid = (map_type == "rain_advanced") and json_grid_ready(
            map_type, run_id, fff, "rain_advanced", cfg.JSON_GRIDS_DIR
        )
        frames.append(
            {
                "fff": fff,
                "label": f"+{fff}h",
                "valid_time": valid_dt.strftime("%Y-%m-%dT%H:%MZ"),
                "tiles_ready": tr,
                "has_json_grid": has_grid,
            }
        )

    spec = MAP_SPECS[map_type]
    segments = [
        {
            "start_fff": s,
            "end_fff": e,
            "step_hours": step,
            "start_time": (run_dt + timedelta(hours=s)).strftime("%Y-%m-%dT%H:%MZ"),
            "end_time": (run_dt + timedelta(hours=e)).strftime("%Y-%m-%dT%H:%MZ"),
        }
        for s, e, step in spec.fff_segments_full
    ]

    ready = [f for f in frames if f["tiles_ready"]]
    anchor = ready if ready else frames
    window_start_time = anchor[0]["valid_time"] if anchor else ""
    window_end_time = anchor[-1]["valid_time"] if anchor else ""

    return {
        "map_type": map_type,
        "run_id": run_id,
        "run_time": run_dt.strftime("%Y-%m-%dT%H:%MZ"),
        "window_start_time": window_start_time,
        "window_end_time": window_end_time,
        "segments": segments,
        "frames": frames,
    }

"""Build timeline metadata that does NOT depend on server "now".

The output is suitable for:
- BE timeline API (router adds now_offset_hours + is_past after calling this)
- Uploading to Bunny CDN as _timeline.json (FE computes now/is_past client-side)
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta
from typing import Any, Optional, TYPE_CHECKING

from ..core.map_specs import MAP_SPECS, segment_fff
from .availability_service import (
    json_grid_ready,
    load_map_availability,
    run_id_to_datetime,
    tiles_ready,
)
from .tile_generator import _MAP_PRODUCTS

if TYPE_CHECKING:
    from .bunny_storage import BunnyStorageClient

log = logging.getLogger(__name__)


def _probe_bunny_ready_fffs(
    bunny: "BunnyStorageClient",
    map_type: str,
    run_id: str,
    product: str,
    fffs: list[int],
    max_workers: int = 8,
) -> set[int]:
    """Probe Bunny for the existence of each frame's anchor chunk in parallel.

    A frame is considered "ready on Bunny" when its z=0 metatile chunk
    (`{map_type}/{run_id}/{fff:03d}/{product}/0/0_0.chunk`) exists. This is
    the same anchor `tiles_ready()` uses for local check, just on Bunny.

    Uses HTTP HEAD (no payload transfer) for cheap existence checks.
    """
    if not fffs:
        return set()

    def _probe_one(fff: int) -> tuple[int, bool]:
        path = f"{map_type}/{run_id}/{fff:03d}/{product}/0/0_0.chunk"
        return fff, bunny.head_object(path)

    ready: set[int] = set()
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futs = [pool.submit(_probe_one, fff) for fff in fffs]
        for fut in as_completed(futs):
            try:
                fff, ok = fut.result()
                if ok:
                    ready.add(fff)
            except Exception as exc:
                log.warning("Bunny probe error for %s/%s: %s", map_type, run_id, exc)
    return ready


def build_timeline_static(
    map_type: str,
    run_id: str,
    cfg: Any,
    bunny_client: Optional["BunnyStorageClient"] = None,
) -> dict:
    """Build timeline metadata as a plain dict.

    Returns None-equivalent (empty frames) if no spec frames are available.
    The caller decides how to handle that (404, skip upload, etc.).

    `tiles_ready` is determined per-frame:
      * Khi `bunny_client` được truyền vào → probe Bunny (HEAD on z=0 chunk)
        cho từng fff. Đây là source of truth khi LOCAL TILES_DIR có thể đã bị
        wipe sau finalize. Chính xác kể cả pipeline push lỡ vài frame.
      * Khi `bunny_client` là None → fallback check LOCAL TILES_DIR (dùng cho
        BE API trả về timeline mid-pipeline khi chưa publish lên Bunny).

    KHÔNG hardcode `tiles_ready=True` cho toàn bộ frames — đó là nguồn gốc
    của lỗi FE hiển thị toàn bộ timeline trong khi chỉ vài frame thực sự có
    chunk trên Bunny.

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

    # Compute the "real" ready set up-front.
    if bunny_client is not None:
        bunny_ready = _probe_bunny_ready_fffs(
            bunny_client, map_type, run_id, first_product, fffs,
        )
        log.info(
            "Bunny ready probe %s/%s: %d/%d frames have z=0 chunk",
            map_type, run_id, len(bunny_ready), len(fffs),
        )
    else:
        bunny_ready = None

    frames: list[dict] = []
    for fff in fffs:
        valid_dt = run_dt + timedelta(hours=fff)
        if bunny_ready is not None:
            tr = fff in bunny_ready
        else:
            tr = tiles_ready(
                map_type, run_id, fff, first_product, cfg.TILES_DIR,
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

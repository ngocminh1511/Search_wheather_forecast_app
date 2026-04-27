from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..config import get_settings
from ..core.colormap import get_colormap_meta, get_legend_stops
from ..core.pipeline_adapter import get_map_specs
from ..schemas.map import LegendResponse, LegendStop, MapTypeInfo, TimelineFrame, TimelineResponse
from ..services.availability_service import (
    all_run_ids,
    json_grid_ready,
    latest_run_id,
    load_map_availability,
    run_id_to_datetime,
    tiles_ready,
)
from ..services.tile_generator import _MAP_PRODUCTS

router = APIRouter(prefix="/maps")


def _resolve_run_id(run_id: Optional[str]) -> str:
    rid = run_id or latest_run_id()
    if not rid:
        raise HTTPException(status_code=404, detail="No runs available")
    return rid


@router.get("", response_model=list[MapTypeInfo])
def list_map_types() -> list[MapTypeInfo]:
    cfg = get_settings()
    specs = get_map_specs()
    result = []
    for mt, spec in specs.items():
        meta = get_colormap_meta(mt)
        result.append(MapTypeInfo(
            map_type=mt,
            label=spec.get("label", mt),
            products=_MAP_PRODUCTS.get(mt, []),
            is_json_only=(mt in cfg.JSON_ONLY_MAP_TYPES),
            is_archive=(mt in cfg.ARCHIVE_MAP_TYPES),
            vmin=meta["vmin"],
            vmax=meta["vmax"],
            unit=meta["unit"],
        ))
    return result


@router.get("/{map_type}/timeline", response_model=TimelineResponse)
def map_timeline(
    map_type: str,
    run_id: Optional[str] = Query(default=None),
) -> TimelineResponse:
    cfg = get_settings()
    specs = get_map_specs()
    if map_type not in specs:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown map_type '{map_type}'. Valid values: {sorted(specs.keys())}",
        )
    rid = _resolve_run_id(run_id)
    fffs = load_map_availability(rid, map_type)
    if not fffs:
        raise HTTPException(
            status_code=404, detail=f"No frames available for {map_type}/{rid}")

    run_dt = run_id_to_datetime(rid)
    now_utc = datetime.now(tz=timezone.utc)
    now_offset_hours = (now_utc - run_dt).total_seconds() / 3600

    products = _MAP_PRODUCTS.get(map_type, ["default"])
    first_product = products[0]
    is_json_only = map_type in cfg.JSON_ONLY_MAP_TYPES

    frames: list[TimelineFrame] = []
    for fff in fffs:
        valid_dt = run_dt + timedelta(hours=fff)
        is_past = valid_dt < now_utc

        if is_json_only:
            tr = False
            has_grid = json_grid_ready(
                map_type, rid, fff, "wind_30m", cfg.JSON_GRIDS_DIR)
        else:
            tr = tiles_ready(map_type, rid, fff, first_product, cfg.TILES_DIR)
            has_grid = (map_type == "rain_advanced") and json_grid_ready(
                map_type, rid, fff, "rain_advanced", cfg.JSON_GRIDS_DIR
            )

        frames.append(TimelineFrame(
            fff=fff,
            label=f"+{fff}h",
            valid_time=valid_dt.strftime("%Y-%m-%dT%H:%MZ"),
            is_past=is_past,
            tiles_ready=tr,
            has_json_grid=has_grid,
        ))

    return TimelineResponse(
        map_type=map_type,
        run_id=rid,
        run_time=run_dt.strftime("%Y-%m-%dT%H:%MZ"),
        frames=frames,
        now_offset_hours=round(now_offset_hours, 2),
    )


@router.get("/{map_type}/legend", response_model=LegendResponse)
def map_legend(
    map_type: str,
    product: Optional[str] = Query(default=None),
    n_stops: int = Query(default=10, ge=2, le=20),
) -> LegendResponse:
    meta = get_colormap_meta(map_type, product)
    raw_stops = get_legend_stops(map_type, product, n_stops)
    stops = [LegendStop(value=s["value"], color_hex=s["color_hex"])
             for s in raw_stops]
    return LegendResponse(
        map_type=map_type,
        product=product,
        unit=meta["unit"],
        vmin=meta["vmin"],
        vmax=meta["vmax"],
        stops=stops,
    )

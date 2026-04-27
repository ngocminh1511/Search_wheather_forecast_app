from __future__ import annotations

from fastapi import APIRouter, HTTPException

from ..config import get_settings
from ..schemas.run import LatestRunResponse, RunAvailabilityResponse, RunInfo, MapAvailability
from ..services.availability_service import (
    all_run_ids,
    latest_run_id,
    load_map_availability,
    run_id_to_datetime,
    tiles_ready,
    json_grid_ready,
)
from ..core.pipeline_adapter import get_map_specs

router = APIRouter(prefix="/runs")


@router.get("", response_model=list[str])
def list_runs() -> list[str]:
    return all_run_ids()


@router.get("/latest", response_model=LatestRunResponse)
def get_latest_run() -> LatestRunResponse:
    run_id = latest_run_id()
    if not run_id:
        raise HTTPException(status_code=404, detail="No runs available")
    dt = run_id_to_datetime(run_id)
    return LatestRunResponse(
        run_id=run_id,
        run_time=dt.isoformat(),
        all_run_ids=all_run_ids(),
    )


@router.get("/{run_id}/availability", response_model=RunAvailabilityResponse)
def run_availability(run_id: str) -> RunAvailabilityResponse:
    cfg = get_settings()
    map_specs = get_map_specs()
    availability: dict[str, MapAvailability] = {}

    for mt in map_specs:
        fffs = load_map_availability(run_id, mt)
        # Check first and last frame readiness as a proxy
        first_tiles = tiles_ready(mt, run_id, fffs[0], _first_product(
            mt), cfg.TILES_DIR) if fffs else False
        first_grid = json_grid_ready(mt, run_id, fffs[0], "wind_30m", cfg.JSON_GRIDS_DIR) if (
            fffs and mt == "wind_animation") else False
        availability[mt] = MapAvailability(
            fff_available=fffs,
            tiles_ready=first_tiles,
            has_json_grid=first_grid,
        )

    return RunAvailabilityResponse(run_id=run_id, availability=availability)


def _first_product(map_type: str) -> str:
    from ..services.tile_generator import _MAP_PRODUCTS
    products = _MAP_PRODUCTS.get(map_type, ["default"])
    return products[0]

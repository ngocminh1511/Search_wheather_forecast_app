from __future__ import annotations

from pydantic import BaseModel


class RunInfo(BaseModel):
    run_id: str          # e.g. "20260409_06z"
    run_date: str        # "2026-04-09"
    run_hour: int        # 0 | 6 | 12 | 18
    cycle_time_utc: str  # ISO-8601 e.g. "2026-04-09T06:00:00Z"
    maps_ready: list[str]  # map_types whose tiles are done


class LatestRunResponse(BaseModel):
    run_id: str
    run_time: str
    all_run_ids: list[str]


class MapAvailability(BaseModel):
    fff_available: list[int]
    tiles_ready: bool
    has_json_grid: bool


class RunAvailabilityResponse(BaseModel):
    run_id: str
    availability: dict[str, MapAvailability]

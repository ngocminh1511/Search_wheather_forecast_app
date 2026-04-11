from __future__ import annotations

from pydantic import BaseModel


class MapTypeInfo(BaseModel):
    map_type: str
    label: str
    products: list[str]
    is_json_only: bool
    is_archive: bool
    vmin: float
    vmax: float
    unit: str


class TimelineFrame(BaseModel):
    fff: int
    label: str             # "+0h", "+3h", etc.
    valid_time: str        # ISO-8601 UTC
    is_past: bool
    tiles_ready: bool
    has_json_grid: bool


class TimelineResponse(BaseModel):
    map_type: str
    run_id: str
    run_time: str          # cycle start time ISO-8601 UTC
    now_offset_hours: float
    frames: list[TimelineFrame]


class LegendStop(BaseModel):
    value: float
    color_hex: str         # "#rrggbb"


class LegendResponse(BaseModel):
    map_type: str
    product: str | None = None
    unit: str
    vmin: float
    vmax: float
    stops: list[LegendStop]


class GridResponse(BaseModel):
    """Returned by /api/v1/maps/{map_type}/grid for animation layers."""
    map_type: str
    product: str
    run_id: str
    fff: int
    valid_time: str
    unit: str
    lat: list[float]
    lon: list[float]
    # For wind_animation: u + v fields
    u: list[list[float]] | None = None
    v: list[list[float]] | None = None
    speed_max: float | None = None
    # For rain_advanced: prate + crain + csnow
    prate: list[list[float]] | None = None
    crain: list[list[float]] | None = None
    csnow: list[list[float]] | None = None


class AdminJobStatus(BaseModel):
    status: str                    # "idle" | "running" | "ok" | "error"
    last_started: str | None = None
    last_success: str | None = None
    last_error: str | None = None
    # Live progress (populated while running; retained after job ends)
    step: str | None = None        # "checking"|"discovering"|"downloading"|"generating"|"done"|"error"
    step_detail: str | None = None
    run_id: str | None = None
    frames_total: int | None = None
    frames_done: int | None = None
    current_fff: int | None = None
    current_product: str | None = None
    tiles_saved: int | None = None
    tiles_skipped: int | None = None

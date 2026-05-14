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


class TimelineSegment(BaseModel):
    start_fff: int      # e.g. 6
    end_fff: int        # e.g. 36
    step_hours: int     # e.g. 3
    start_time: str     # ISO UTC — run_dt + start_fff hours
    end_time: str       # ISO UTC — run_dt + end_fff hours


class TimelineResponse(BaseModel):
    map_type: str
    run_id: str
    run_time: str              # cycle start time ISO-8601 UTC
    now_offset_hours: float
    window_start_time: str     # validTime of first ready frame (or first frame)
    window_end_time: str       # validTime of last ready frame (or last frame)
    segments: list[TimelineSegment]   # step structure from MAP_SPECS
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
    # For rain_advanced: prate + crain + csnow
    prate: list[list[float]] | None = None
    crain: list[list[float]] | None = None
    csnow: list[list[float]] | None = None


class AdminJobStatus(BaseModel):
    status: str                    # "idle" | "running" | "ok" | "error" | "cancelled"
    last_started: str | None = None
    last_success: str | None = None
    last_error: str | None = None
    # Live progress (populated while running; retained after job ends)
    step: str | None = None        # "checking"|"discovering"|"downloading"|"staging"|"swapping"|"generating"|"done"|"cancelled"|"error"
    step_detail: str | None = None
    run_id: str | None = None
    frames_total: int | None = None
    frames_done: int | None = None
    current_fff: int | None = None
    current_product: str | None = None
    tiles_saved: int | None = None
    tiles_skipped: int | None = None
    started_at: str | None = None          # ISO UTC — for UI elapsed-time calculation
    download_duration_s: float | None = None
    tile_duration_s: float | None = None

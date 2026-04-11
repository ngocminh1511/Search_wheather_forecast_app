from __future__ import annotations

"""
grid.py — JSON grid endpoint for wind_animation and rain_advanced animation.

GET /api/v1/maps/{map_type}/grid
  ?run_id=20260406_00z
  &fff=0
  &product=wind_10m  (wind_animation only)
  &downsample=1.0    (degree resolution, default 1.0)
  &bbox=west,south,east,north  (optional crop)
"""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from ..config import get_settings
from ..schemas.map import GridResponse
from ..services.availability_service import latest_run_id
from ..services.grid_service import generate_grid

router = APIRouter(prefix="/maps")


@router.get("/{map_type}/grid", response_model=GridResponse)
def get_grid(
    map_type: str,
    run_id: Optional[str] = Query(default=None),
    fff: int = Query(default=0, ge=0),
    product: str = Query(default="wind_10m"),
    downsample: float = Query(default=1.0, ge=0.25, le=5.0),
    bbox: Optional[str] = Query(default=None, description="west,south,east,north"),
) -> GridResponse:
    cfg = get_settings()

    allowed_json = cfg.JSON_ONLY_MAP_TYPES | {"rain_advanced"}
    if map_type not in allowed_json:
        raise HTTPException(
            status_code=400,
            detail=f"map_type={map_type!r} does not support JSON grid. Use /tiles/ endpoint.",
        )

    rid = run_id or latest_run_id()
    if not rid:
        raise HTTPException(status_code=404, detail="No runs available")

    # Parse optional bbox
    bbox_tuple: tuple[float, float, float, float] | None = None
    if bbox:
        parts = bbox.split(",")
        if len(parts) != 4:
            raise HTTPException(status_code=400, detail="bbox must be 'west,south,east,north'")
        try:
            bbox_tuple = tuple(float(p) for p in parts)  # type: ignore[assignment]
        except ValueError:
            raise HTTPException(status_code=400, detail="bbox values must be numeric")

    # Check JSON cache first (no GRIB needed if already cached)
    from ..services.grid_service import _grid_cache_path
    cache = _grid_cache_path(map_type, rid, fff, product, cfg.JSON_GRIDS_DIR)
    if cache.exists():
        import json
        payload = json.loads(cache.read_text())
        return GridResponse(**payload)

    payload = generate_grid(
        map_type=map_type,
        run_id=rid,
        fff=fff,
        product=product,
        data_dir=cfg.DATA_DIR,
        grids_dir=cfg.JSON_GRIDS_DIR,
        downsample_deg=downsample,
        bbox=bbox_tuple,
    )

    return GridResponse(**payload)

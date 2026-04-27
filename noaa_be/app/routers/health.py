from __future__ import annotations

from fastapi import APIRouter
from ..schemas.health import HealthResponse
from ..config import get_settings

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    cfg = get_settings()
    return HealthResponse(
        status="ok",
        version="1.0.0",
        base_dir=str(cfg.BASE_DIR),
        db_dir=str(cfg.DB_DIR),
        tiles_dir=str(cfg.TILES_DIR),
        scheduler_enabled=cfg.SCHEDULER_ENABLED,
    )

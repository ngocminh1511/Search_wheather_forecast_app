from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
    base_dir: str
    db_dir: str
    tiles_dir: str
    scheduler_enabled: bool

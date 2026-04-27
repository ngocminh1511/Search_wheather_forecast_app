from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import tiles, runs, maps, health, grid

# Read-only admin router (jobs, logs)
from fastapi import APIRouter
from app.services.scheduler_service import get_all_job_status
from app.services.progress_tracker import get_all as get_all_progress
from app.services.log_buffer import get_lines, get_max_seq
from app.schemas.map import AdminJobStatus

admin_router = APIRouter(prefix="/admin")

@admin_router.get("/jobs", response_model=dict[str, AdminJobStatus])
def list_jobs() -> dict[str, AdminJobStatus]:
    all_status = get_all_job_status()
    all_progress = get_all_progress()
    return {
        mt: AdminJobStatus(**{**status, **all_progress.get(mt, {})})
        for mt, status in all_status.items()
    }

@admin_router.get("/logs")
def get_logs(since: int = 0, limit: int = 200) -> dict:
    lines = get_lines(since_seq=since, limit=limit)
    return {"max_seq": get_max_seq(), "lines": lines}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # API Service is read-only, no scheduler started here.
    # It just serves files and reads from SQLite.
    yield

cfg = get_settings()

app = FastAPI(
    title="NOAA Tile API Service",
    version=cfg.APP_VERSION,
    lifespan=lifespan,
    docs_url="/docs" if cfg.API_VERSION == "v1" else None,
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cfg.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api_prefix = f"/api/{cfg.API_VERSION}"

# Register only read-only endpoints
app.include_router(tiles.router)
app.include_router(runs.router, prefix=api_prefix)
app.include_router(maps.router, prefix=api_prefix)
app.include_router(health.router, prefix=api_prefix)
app.include_router(grid.router, prefix=api_prefix)
app.include_router(admin_router, prefix=api_prefix)

if __name__ == "__main__":
    import uvicorn
    # Default to 8000 for API
    uvicorn.run("api_main:app", host=cfg.HOST, port=cfg.PORT, reload=False)

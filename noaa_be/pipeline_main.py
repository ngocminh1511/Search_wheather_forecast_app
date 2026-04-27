from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from app.config import get_settings
from app.routers import admin
from app.services.scheduler_service import start_scheduler, stop_scheduler

import logging
from app.services.log_buffer import install_handler as _install_log_buffer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
_install_log_buffer()

def _warmup_eccodes() -> None:
    try:
        import eccodes as _ec
        _ec.codes_get_api_version()
        logging.info("eccodes initialised (version %s, definitions: %s)",
                 _ec.__version__, _ec.codes_definition_path())
    except Exception as exc:
        logging.warning("eccodes warm-up failed (non-fatal): %s", exc)

@asynccontextmanager
async def lifespan(app: FastAPI):
    _warmup_eccodes()
    
    from app.services.pipeline_orchestrator import get_orchestrator
    orchestrator = get_orchestrator()
    await orchestrator.start()
    
    # Pipeline Service handles all heavy background jobs
    start_scheduler()
    yield
    stop_scheduler()
    
    await orchestrator.stop()

cfg = get_settings()

app = FastAPI(
    title="NOAA Pipeline Service",
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

# Register full admin endpoints (including mutation/trigger endpoints)
app.include_router(admin.router, prefix=api_prefix)

# Mount Admin UI at /admin/
_static_dir = Path(__file__).parent / "app" / "static"
app.mount("/admin", StaticFiles(directory=str(_static_dir), html=True), name="admin-ui")

if __name__ == "__main__":
    import uvicorn
    # Pipeline service runs on port 8001 by default
    uvicorn.run("pipeline_main:app", host=cfg.HOST, port=8001, reload=False)

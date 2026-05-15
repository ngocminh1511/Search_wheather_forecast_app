from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pathlib import Path

from app.config import get_settings
from app.routers import admin
from app.services.scheduler_service import (
    reset_zombie_jobs,
    start_scheduler,
    stop_scheduler,
)

import logging
from app.services.log_buffer import install_handler as _install_log_buffer

_cfg_boot = get_settings()
logging.basicConfig(
    level=getattr(logging, _cfg_boot.LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
_install_log_buffer()

if _cfg_boot.LOG_FILE_ENABLED:
    try:
        from app.services.log_files import install_file_handlers
        install_file_handlers(
            _cfg_boot.LOG_DIR,
            retention_days=_cfg_boot.LOG_RETENTION_DAYS,
            level=getattr(logging, _cfg_boot.LOG_LEVEL, logging.INFO),
        )
    except Exception as _exc:
        logging.warning("Failed to install file log handlers (non-fatal): %s", _exc)

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

    cfg_l = get_settings()
    if not cfg_l.ADMIN_API_TOKEN:
        logging.warning(
            "ADMIN_API_TOKEN is empty — admin endpoints are UNAUTHENTICATED. "
            "Set ADMIN_API_TOKEN env for any non-localhost deployment."
        )
    if "*" in cfg_l.CORS_ORIGINS:
        logging.warning(
            "CORS_ORIGINS contains '*' — wildcard origin allowed. "
            "Set CORS_ORIGINS env to a comma-separated allowlist."
        )

    from app.services.pipeline_orchestrator import get_orchestrator
    orchestrator = get_orchestrator()
    await orchestrator.start()

    # Wipe any "running" job status left over from a previously-crashed
    # process, otherwise the per-job stale-lock guard silently no-ops every
    # new trigger for up to 2h.
    reset_zombie_jobs()

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

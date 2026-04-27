from __future__ import annotations
from .services.log_buffer import install_handler as _install_log_buffer

"""
main.py — FastAPI application entry point.

Mount order:
  /tiles/...          — raw PNG tile serving (no /api/v1 prefix, for slippy-map compat)
  /api/v1/health
  /api/v1/runs
  /api/v1/maps        — map list, timeline, legend (maps.py)
  /api/v1/maps        — grid endpoint (grid.py, separate router to avoid prefix clash)
  /api/v1/admin
"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from .config import get_settings
from .routers import admin, grid, health, maps, runs, tiles
from .services.scheduler_service import start_scheduler, stop_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

# Install in-memory log buffer so admin UI can stream logs
_install_log_buffer()


# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

def _warmup_eccodes() -> None:
    """Force eccodes to initialise its MEMFS definitions in the main thread.

    On Windows the eccodes wheel embeds definitions in an in-memory filesystem
    (MEMFS).  If two threads trigger initialisation simultaneously, the C
    library prints harmless but noisy "syntax error at line 1 of
    /MEMFS/definitions/boot.def" messages.  Calling any eccodes function here
    — before the request-handling threads start — serialises the one-time
    bootstrap so those messages never appear during normal operation.
    """
    try:
        import eccodes as _ec  # noqa: PLC0415
        _ec.codes_get_api_version()
        log.info("eccodes initialised (version %s, definitions: %s)",
                 _ec.__version__, _ec.codes_definition_path())
    except Exception as exc:  # pragma: no cover
        log.warning("eccodes warm-up failed (non-fatal): %s", exc)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cfg = get_settings()
    log.info("Starting noaa_be  base_dir=%s db_dir=%s",
             cfg.BASE_DIR, cfg.DB_DIR)
    # Warm up eccodes so Windows MEMFS is initialised before concurrent requests
    _warmup_eccodes()
    # Ensure storage dirs exist
    for d in [cfg.DATA_DIR, cfg.TILES_DIR, cfg.JSON_GRIDS_DIR, cfg.STAGING_DIR, cfg.AVAILABLE_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    start_scheduler()
    yield
    stop_scheduler()
    log.info("noaa_be shut down")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    cfg = get_settings()
    app = FastAPI(
        title="NOAA Weather Map API",
        version="1.0.0",
        description=(
            "Global weather tile and grid API. "
            "Serves XYZ PNG tiles (temperature, rain, cloud, snow) "
            "and JSON animation grids (wind, rain_advanced)."
        ),
        lifespan=lifespan,
    )

    # CORS — allow any origin in dev; lock down in prod via env
    app.add_middleware(
        CORSMiddleware,
        allow_origins=cfg.CORS_ORIGINS,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    # Tiles served at root path for slippy-map compat
    app.include_router(tiles.router)

    # API v1
    api_prefix = "/api/v1"
    app.include_router(health.router, prefix=api_prefix, tags=["Health"])
    app.include_router(runs.router,   prefix=api_prefix, tags=["Runs"])
    app.include_router(maps.router,   prefix=api_prefix, tags=["Maps"])
    app.include_router(grid.router,   prefix=api_prefix, tags=["Grid"])
    app.include_router(admin.router,  prefix=api_prefix, tags=["Admin"])

    # Admin UI — http://localhost:8000/admin/
    _static_dir = Path(__file__).parent / "static"
    app.mount("/admin", StaticFiles(directory=str(_static_dir),
              html=True), name="admin-ui")

    return app


app = create_app()

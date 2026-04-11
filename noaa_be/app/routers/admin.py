from __future__ import annotations

"""
admin.py — management endpoints (NOT exposed to public internet).

POST /api/v1/admin/trigger-discover          → discover new GFS cycles
POST /api/v1/admin/trigger-download          → download GRIB files for a run
POST /api/v1/admin/trigger-generate-tiles   → generate tiles (one map_type or all)
POST /api/v1/admin/trigger-generate-grids   → generate JSON grids (one map_type or all)
POST /api/v1/admin/trigger-job/{map_type}   → fire scheduler job immediately
GET  /api/v1/admin/jobs                     → all job statuses
"""

from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Body, HTTPException, Query

from ..schemas.map import AdminJobStatus
from ..services.scheduler_service import get_all_job_status, trigger_job

router = APIRouter(prefix="/admin")


# ---------------------------------------------------------------------------
# Job status
# ---------------------------------------------------------------------------

@router.get("/jobs", response_model=dict[str, AdminJobStatus])
def list_jobs() -> dict[str, AdminJobStatus]:
    from ..services import progress_tracker
    all_status = get_all_job_status()
    all_progress = progress_tracker.get_all()
    return {
        mt: AdminJobStatus(**{**status, **all_progress.get(mt, {})})
        for mt, status in all_status.items()
    }


@router.get("/logs")
def get_logs(since: int = Query(default=0), limit: int = Query(default=200)) -> dict:
    """Return recent server log lines for the admin UI log panel."""
    from ..services.log_buffer import get_lines, get_max_seq
    lines = get_lines(since_seq=since, limit=limit)
    return {"max_seq": get_max_seq(), "lines": lines}


# ---------------------------------------------------------------------------
# Manual job trigger
# ---------------------------------------------------------------------------

@router.post("/trigger-job/{map_type}")
def trigger_map_job(map_type: str, background_tasks: BackgroundTasks) -> dict:
    """Fire the scheduler job for one map_type immediately in background."""
    from ..core.pipeline_adapter import get_map_specs
    specs = get_map_specs()
    if map_type not in specs:
        raise HTTPException(status_code=404, detail=f"Unknown map_type: {map_type!r}")
    background_tasks.add_task(trigger_job, map_type)
    return {"queued": True, "map_type": map_type}


@router.post("/trigger-job-all")
def trigger_all_jobs(background_tasks: BackgroundTasks) -> dict:
    """Fire scheduler jobs for all map types immediately."""
    from ..core.pipeline_adapter import get_map_specs
    specs = get_map_specs()
    for mt in specs:
        background_tasks.add_task(trigger_job, mt)
    return {"queued": True, "map_types": list(specs.keys())}


# ---------------------------------------------------------------------------
# Fine-grained admin operations
# ---------------------------------------------------------------------------

@router.post("/trigger-discover")
def trigger_discover(
    run_date: str = Body(..., example="20260406"),
    run_hour: int = Body(..., example=0),
    max_fff: int = Body(default=48),
) -> dict:
    """Probe NOAA and update availability JSON for a specific cycle."""
    from datetime import date
    from ..core.pipeline_adapter import discover_update_times
    from ..config import get_settings

    cfg = get_settings()
    d = date(int(run_date[:4]), int(run_date[4:6]), int(run_date[6:8]))
    result = discover_update_times(
        run_date=d,
        run_hour=run_hour,
        max_fff=max_fff,
        rpm_limit=30,
        available_dir=cfg.AVAILABLE_DIR,
    )
    return {"status": "ok", "result": result}


@router.post("/trigger-download")
def trigger_download(
    map_type: str = Body(...),
    run_date: str = Body(..., example="20260406"),
    run_hour: int = Body(..., example=0),
    background_tasks: BackgroundTasks = BackgroundTasks(),
) -> dict:
    """Download GRIB files for a specific (map_type, run) in background."""
    from datetime import date
    from ..core.pipeline_adapter import download_map_dataset
    from ..config import get_settings
    from ..core.downloader import run_id_from_date
    from ..services.scheduler_service import coverage_sufficient

    cfg = get_settings()
    d = date(int(run_date[:4]), int(run_date[4:6]), int(run_date[6:8]))
    run_id = run_id_from_date(d, run_hour)

    ok, reason = coverage_sufficient(map_type, run_id, cfg)
    if ok:
        return {
            "status": "sufficient",
            "skipped": True,
            "message": reason,
            "map_type": map_type,
            "run_id": run_id,
        }

    def _download():
        download_map_dataset(
            map_type=map_type,
            run_date=d,
            run_hour=run_hour,
            data_dir=cfg.DATA_DIR,
            skip_existing=True,
        )

    background_tasks.add_task(_download)
    return {"queued": True, "map_type": map_type, "run_date": run_date, "run_hour": run_hour, "run_id": run_id}


@router.post("/trigger-generate-tiles")
def trigger_generate_tiles(
    map_type: str = Body(...),
    run_id: str = Body(..., example="20260406_00z"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
) -> dict:
    """Generate PNG tiles for a (map_type, run_id) in background."""
    from ..config import get_settings
    from ..services.tile_generator import generate_run
    from ..services.scheduler_service import _compute_needed_fffs

    cfg = get_settings()

    def _gen():
        fffs = _compute_needed_fffs(map_type, cfg)
        generate_run(map_type, run_id, fffs, data_dir=cfg.DATA_DIR)

    background_tasks.add_task(_gen)
    return {"queued": True, "map_type": map_type, "run_id": run_id}


@router.post("/trigger-generate-grids")
def trigger_generate_grids(
    map_type: str = Body(...),
    run_id: str = Body(..., example="20260406_00z"),
    background_tasks: BackgroundTasks = BackgroundTasks(),
) -> dict:
    """Generate JSON grids for a (map_type, run_id) in background."""
    from ..config import get_settings
    from ..services.grid_service import generate_grid
    from ..services.scheduler_service import _compute_needed_fffs

    cfg = get_settings()

    def _gen():
        from ..services.grid_service import _WIND_PRODUCTS
        fffs = _compute_needed_fffs(map_type, cfg)
        for fff in fffs:
            if map_type == "wind_animation":
                for product in _WIND_PRODUCTS:
                    try:
                        generate_grid(map_type, run_id, fff, product,
                                      data_dir=cfg.DATA_DIR, grids_dir=cfg.JSON_GRIDS_DIR)
                    except Exception:
                        pass
            elif map_type == "rain_advanced":
                try:
                    generate_grid(map_type, run_id, fff, "rain_advanced",
                                  data_dir=cfg.DATA_DIR, grids_dir=cfg.JSON_GRIDS_DIR)
                except Exception:
                    pass

    background_tasks.add_task(_gen)
    return {"queued": True, "map_type": map_type, "run_id": run_id}

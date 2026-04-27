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
    from ..core.db import get_all_job_status
    all_status = get_all_job_status()
    all_progress = progress_tracker.get_all()
    return {
        mt: AdminJobStatus(**{**status, **all_progress.get(mt, {})})
        for mt, status in all_status.items()
    }

@router.post("/jobs/{map_type}/cancel")
def cancel_job(map_type: str) -> dict:
    """Request cancellation for a running job.

    Immediately writes status='cancelled' to DB so the UI stops showing
    the job as running — even if the background thread already died without
    cleaning up (zombie run).  Any live thread will also see cancel_requested
    and re-confirm the status, which is idempotent.
    """
    from ..core.db import set_cancel_requested, get_job_status, update_job_status
    from ..services import progress_tracker
    status = get_job_status(map_type)
    current = status.get("status")
    if current == "running":
        # Set flag for any live thread to pick up
        set_cancel_requested(map_type, True)
        # Also immediately mark as cancelled in DB so UI reflects it right away
        # (handles zombie jobs where the thread died without updating status)
        status["status"] = "cancelled"
        status["cancel_requested"] = True
        status.pop("last_error", None)
        update_job_status(map_type, status)
        progress_tracker.update(map_type, step="cancelled",
                                step_detail="\u0110\u00e3 h\u1ee7y b\u1edfi ng\u01b0\u1eddi d\u00f9ng \u2717")
        return {"status": "ok", "message": f"Job {map_type} cancelled."}
    return {"status": "ignored", "message": f"Job {map_type} is not running (current: {current})."}



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
        raise HTTPException(
            status_code=404, detail=f"Unknown map_type: {map_type!r}")
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
    max_fff: Optional[int] = Body(
        default=None, description="Giới hạn forecast hour tối đa. None = dùng NEEDED_FORECAST_H từ config."),
    background_tasks: BackgroundTasks = BackgroundTasks(),
) -> dict:
    """Download GRIB files for a specific (map_type, run) in background."""
    from datetime import date
    from ..core.pipeline_adapter import download_map_dataset
    from ..config import get_settings
    from ..core.downloader import run_id_from_date
    from ..core.map_specs import MAP_SPECS
    from ..core.map_specs import segment_fff
    from ..services.scheduler_service import coverage_sufficient, _compute_needed_fffs

    cfg = get_settings()
    d = date(int(run_date[:4]), int(run_date[4:6]), int(run_date[6:8]))
    run_id = run_id_from_date(d, run_hour)

    # Tính fff_values theo max_fff được truyền vào, hoặc dùng NEEDED_FORECAST_H
    if max_fff is not None:
        all_fff = segment_fff(MAP_SPECS[map_type].fff_segments_full)
        explicit_fffs = [f for f in all_fff if f <= max_fff]
    else:
        explicit_fffs = _compute_needed_fffs(map_type, cfg)

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
        from ..services import progress_tracker
        from ..core.db import (
            get_job_status as db_get_job_status,
            update_job_status as db_update_job_status,
            reset_cancel_requested,
            check_cancel_requested,
            JobCancelledError,
        )
        from datetime import datetime, timezone

        # Reset stale cancel flag before starting
        reset_cancel_requested(map_type)

        status = db_get_job_status(map_type)
        status["status"] = "running"
        status["last_started"] = datetime.now(tz=timezone.utc).isoformat()
        status.pop("cancel_requested", None)
        db_update_job_status(map_type, status)

        progress_tracker.reset(map_type)
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        progress_tracker.update(
            map_type,
            step="downloading",
            step_detail=f"Tải GRIB2 cho {map_type} / {run_id} (f000–f{explicit_fffs[-1]:03d})…",
            run_id=run_id,
            frames_total=len(explicit_fffs),
            frames_done=0,
            tiles_saved=0,
            tiles_skipped=0,
            started_at=now_iso,
        )
        try:
            # Check cancel before starting the heavy download
            if check_cancel_requested(map_type):
                raise JobCancelledError("Cancelled before download.")

            import time as _time
            _dl_start = _time.perf_counter()
            download_map_dataset(
                map_type=map_type,
                run_date=d,
                run_hour=run_hour,
                data_dir=cfg.DATA_DIR,
                fff_values=explicit_fffs,
                skip_existing=True,
            )
            _dl_elapsed = round(_time.perf_counter() - _dl_start, 1)

            status = db_get_job_status(map_type)
            status["status"] = "ok"
            status["last_success"] = datetime.now(tz=timezone.utc).isoformat()
            status["download_duration_s"] = _dl_elapsed
            status.pop("last_error", None)
            status.pop("cancel_requested", None)
            db_update_job_status(map_type, status)

            progress_tracker.update(
                map_type,
                step="done",
                step_detail=f"Tải xong {len(explicit_fffs)} frames GRIB2 ✓",
                frames_done=len(explicit_fffs),
                download_duration_s=_dl_elapsed,
            )
        except JobCancelledError:
            status = db_get_job_status(map_type)
            status["status"] = "cancelled"
            status["cancel_requested"] = False
            status.pop("last_error", None)
            db_update_job_status(map_type, status)
            progress_tracker.update(map_type, step="cancelled", step_detail="Đã hủy bởi người dùng ✗")
        except Exception as exc:
            status = db_get_job_status(map_type)
            status["status"] = "error"
            status["last_error"] = str(exc)
            status.pop("cancel_requested", None)
            db_update_job_status(map_type, status)

            progress_tracker.update(
                map_type, step="error", step_detail=str(exc)[:120])

    background_tasks.add_task(_download)
    return {
        "queued": True,
        "map_type": map_type,
        "run_date": run_date,
        "run_hour": run_hour,
        "run_id": run_id,
        "fff_count": len(explicit_fffs),
        "fff_max": explicit_fffs[-1] if explicit_fffs else 0,
    }


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
        from ..services import progress_tracker
        from ..core.db import (
            get_job_status as db_get_job_status,
            update_job_status as db_update_job_status,
            reset_cancel_requested,
            JobCancelledError,
        )
        from datetime import datetime, timezone

        # Reset stale cancel flag before starting
        reset_cancel_requested(map_type)

        status = db_get_job_status(map_type)
        status["status"] = "running"
        status["last_started"] = datetime.now(tz=timezone.utc).isoformat()
        status.pop("cancel_requested", None)
        db_update_job_status(map_type, status)

        progress_tracker.reset(map_type)
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        progress_tracker.update(
            map_type,
            step="generating",
            step_detail=f"Tạo tiles cho {map_type} / {run_id}…",
            run_id=run_id,
            frames_total=0,
            frames_done=0,
            tiles_saved=0,
            tiles_skipped=0,
            started_at=now_iso,
        )
        try:
            fffs = _compute_needed_fffs(map_type, cfg)
            _tile_result = generate_run(
                map_type, run_id, fffs, data_dir=cfg.DATA_DIR)
            _tile_dur = _tile_result.get("duration_s", 0) if isinstance(
                _tile_result, dict) else 0

            status = db_get_job_status(map_type)
            status["status"] = "ok"
            status["last_success"] = datetime.now(tz=timezone.utc).isoformat()
            status["tile_duration_s"] = _tile_dur
            status.pop("last_error", None)
            status.pop("cancel_requested", None)
            db_update_job_status(map_type, status)

            progress_tracker.update(
                map_type, step="done", step_detail="Tạo tiles hoàn thành ✓",
                tile_duration_s=_tile_dur,
            )
        except JobCancelledError:
            status = db_get_job_status(map_type)
            status["status"] = "cancelled"
            status["cancel_requested"] = False
            status.pop("last_error", None)
            db_update_job_status(map_type, status)
            progress_tracker.update(map_type, step="cancelled", step_detail="Đã hủy bởi người dùng ✗")
        except Exception as exc:
            status = db_get_job_status(map_type)
            status["status"] = "error"
            status["last_error"] = str(exc)
            status.pop("cancel_requested", None)
            db_update_job_status(map_type, status)

            progress_tracker.update(
                map_type, step="error", step_detail=str(exc)[:120])

    background_tasks.add_task(_gen)
    return {"queued": True, "map_type": map_type, "run_id": run_id}


@router.get("/check-missing-tiles")
def check_missing_tiles(
    map_type: str = Query(...),
    run_id: str = Query(..., example="20260406_00z"),
) -> dict:
    """
    Check how many PNG tiles are missing compared to available raw GRIB2 data
    for the given (map_type, run_id).

    Returns a summary (total expected / existing / missing) and a per-frame,
    per-product breakdown with per-zoom detail.
    """
    from ..config import get_settings
    from ..services.tile_generator import _MAP_PRODUCTS

    cfg = get_settings()

    if map_type in cfg.JSON_ONLY_MAP_TYPES:
        return {
            "map_type": map_type,
            "run_id": run_id,
            "note": "JSON-only map type — no PNG tiles generated",
        }

    products = _MAP_PRODUCTS.get(map_type)
    if not products:
        raise HTTPException(
            status_code=404, detail=f"Unknown map_type: {map_type!r}")

    # Map output product name to source GRIB folder
    grib_folder_map: dict[str, str] = {}
    for p in products:
        if map_type == "rain_advanced":
            grib_folder_map[p] = "rain_adv_surface"
        elif map_type == "wind_surface":
            grib_folder_map[p] = "wind_10m"
        else:
            grib_folder_map[p] = p

    zoom_max = cfg.TILE_ZOOM_EAGER_MAX
    # At zoom z the full-world grid has 4^z tiles (2^z × 2^z)
    per_zoom_expected: dict[int, int] = {
        z: 4 ** z for z in range(zoom_max + 1)}
    tiles_per_frame: int = sum(per_zoom_expected.values())

    data_base = cfg.DATA_DIR / map_type / run_id
    tiles_base = cfg.TILES_DIR / map_type / run_id

    breakdown: dict[str, dict] = {}
    total_expected = 0
    total_existing = 0
    total_grib_frames = 0

    for product in products:
        grib_folder = grib_folder_map.get(product, product)
        product_data_dir = data_base / grib_folder
        if not product_data_dir.exists():
            continue
        for grib_file in sorted(product_data_dir.glob("f*.grib2")):
            try:
                fff = int(grib_file.stem[1:])   # "f000" → 0
            except ValueError:
                continue

            tile_dir = tiles_base / f"{fff:03d}" / product
            existing_total = 0
            per_zoom_actual: dict[int, dict] = {}

            for z in range(zoom_max + 1):
                exp_z = per_zoom_expected[z]
                z_dir = tile_dir / str(z)
                cnt_z = 0
                if z_dir.exists():
                    try:
                        for x_entry in z_dir.iterdir():
                            if x_entry.is_dir():
                                cnt_z += sum(1 for _ in x_entry.iterdir()
                                             if _.is_file())
                    except OSError:
                        pass
                per_zoom_actual[z] = {
                    "expected": exp_z,
                    "existing": cnt_z,
                    "missing": max(0, exp_z - cnt_z),
                }
                existing_total += cnt_z

            frame_key = f"f{fff:03d}"
            breakdown.setdefault(frame_key, {})[product] = {
                "tiles_expected": tiles_per_frame,
                "tiles_existing": existing_total,
                "tiles_missing": max(0, tiles_per_frame - existing_total),
                "complete": existing_total >= tiles_per_frame,
                "per_zoom": per_zoom_actual,
            }
            total_expected += tiles_per_frame
            total_existing += existing_total
            total_grib_frames += 1

    if not breakdown:
        return {
            "map_type": map_type,
            "run_id": run_id,
            "zoom_max": zoom_max,
            "note": "No GRIB2 data found for this run — nothing to compare against",
            "total_grib_frames": 0,
            "total_tiles_expected": 0,
            "total_tiles_existing": 0,
            "total_tiles_missing": 0,
            "all_complete": False,
            "breakdown": {},
        }

    return {
        "map_type": map_type,
        "run_id": run_id,
        "zoom_max": zoom_max,
        "total_grib_frames": total_grib_frames,
        "total_tiles_expected": total_expected,
        "total_tiles_existing": total_existing,
        "total_tiles_missing": max(0, total_expected - total_existing),
        "all_complete": total_existing >= total_expected,
        "breakdown": breakdown,
    }


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
        from ..services import progress_tracker
        from ..core.db import (
            get_job_status as db_get_job_status,
            update_job_status as db_update_job_status,
            reset_cancel_requested,
            check_cancel_requested,
            JobCancelledError,
        )
        from datetime import datetime, timezone

        # Reset stale cancel flag before starting
        reset_cancel_requested(map_type)

        status = db_get_job_status(map_type)
        status["status"] = "running"
        status["last_started"] = datetime.now(tz=timezone.utc).isoformat()
        status.pop("cancel_requested", None)
        db_update_job_status(map_type, status)

        progress_tracker.reset(map_type)
        fffs = _compute_needed_fffs(map_type, cfg)
        total = len(fffs)
        now_iso = datetime.now(tz=timezone.utc).isoformat()
        progress_tracker.update(
            map_type,
            step="generating",
            step_detail=f"Tạo JSON grids cho {map_type} / {run_id}…",
            run_id=run_id,
            frames_total=total,
            frames_done=0,
            tiles_saved=0,
            tiles_skipped=0,
            started_at=now_iso,
        )
        try:
            done = 0
            for fff in fffs:
                if check_cancel_requested(map_type):
                    raise JobCancelledError("Job cancelled by user.")

                progress_tracker.update(
                    map_type, current_fff=fff, step_detail=f"Grid f{fff:03d}…")
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
                done += 1
                progress_tracker.update(map_type, frames_done=done)

            status = db_get_job_status(map_type)
            status["status"] = "ok"
            status["last_success"] = datetime.now(tz=timezone.utc).isoformat()
            status.pop("last_error", None)
            status.pop("cancel_requested", None)
            db_update_job_status(map_type, status)

            progress_tracker.update(
                map_type, step="done", step_detail="Tạo JSON grids hoàn thành ✓")
        except JobCancelledError:
            status = db_get_job_status(map_type)
            status["status"] = "cancelled"
            status["cancel_requested"] = False
            status.pop("last_error", None)
            db_update_job_status(map_type, status)
            progress_tracker.update(map_type, step="cancelled", step_detail="Đã hủy bởi người dùng ✗")
        except Exception as exc:
            status = db_get_job_status(map_type)
            status["status"] = "error"
            status["last_error"] = str(exc)
            status.pop("cancel_requested", None)
            db_update_job_status(map_type, status)

            progress_tracker.update(
                map_type, step="error", step_detail=str(exc)[:120])

    background_tasks.add_task(_gen)
    return {"queued": True, "map_type": map_type, "run_id": run_id}


# ---------------------------------------------------------------------------
# Delete run data
# ---------------------------------------------------------------------------

@router.delete("/runs/{map_type}/{run_id}")
def delete_run_data(map_type: str, run_id: str) -> dict:
    """Permanently delete all on-disk data for a (map_type, run_id).

    Removes (each only if present):
      - live tiles      → TILES_DIR/<map_type>/<run_id>/
      - staging tiles   → STAGING_DIR/<map_type>/<run_id>/
      - raw GRIB2 data  → DATA_DIR/<map_type>/<run_id>/
      - JSON grids      → JSON_GRIDS_DIR/<map_type>/<run_id>/
      - per-map avail   → AVAILABLE_DIR/<map_type>/availability_{run_id}_{map_type}.json

    Uses shutil.rmtree for fast single-syscall deletion of each directory.
    """
    import shutil as _shutil
    from pathlib import Path as _Path
    from ..config import get_settings
    from ..core.pipeline_adapter import get_map_specs
    from ..services.availability_service import parse_run_id

    # Validate run_id format before touching any files
    try:
        parse_run_id(run_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Invalid run_id format: {run_id!r}")

    specs = get_map_specs()
    if map_type not in specs:
        raise HTTPException(
            status_code=404, detail=f"Unknown map_type: {map_type!r}")

    cfg = get_settings()
    deleted: list[str] = []
    errors: list[str] = []

    def _rm(path: _Path, label: str) -> None:
        if path.exists():
            try:
                _shutil.rmtree(path)
                deleted.append(label)
                log.info("delete_run: removed %s → %s", label, path)
            except Exception as exc:
                errors.append(f"{label}: {exc}")
                log.error("delete_run: failed %s → %s (%s)", label, path, exc)

    _rm(cfg.TILES_DIR / map_type / run_id, "tiles")
    _rm(cfg.STAGING_DIR / map_type / run_id, "staging")
    _rm(cfg.DATA_DIR / map_type / run_id, "data")
    _rm(cfg.JSON_GRIDS_DIR / map_type / run_id, "json_grids")

    # Per-map availability JSON (small file → unlink, not rmtree)
    avail_file = cfg.AVAILABLE_DIR / map_type / \
        f"availability_{run_id}_{map_type}.json"
    if avail_file.exists():
        try:
            avail_file.unlink()
            deleted.append("availability")
            log.info("delete_run: removed availability → %s", avail_file)
        except Exception as exc:
            errors.append(f"availability: {exc}")
            log.error("delete_run: failed availability → %s (%s)",
                      avail_file, exc)

    log.info(
        "delete_run %s/%s complete: deleted=%s errors=%d",
        map_type, run_id, deleted, len(errors),
    )
    return {"map_type": map_type, "run_id": run_id, "deleted": deleted, "errors": errors}



_storage_stats_cache = {"data": None, "is_computing": False, "last_updated": 0}

def _compute_storage_stats():
    from ..config import get_settings
    import os
    import time
    global _storage_stats_cache
    cfg = get_settings()
    
    stats = {}
    total_bytes_all = 0
    
    def get_size(path):
        total_size = 0
        file_count = 0
        if not os.path.exists(path):
            return 0, 0
        try:
            for dirpath, _, filenames in os.walk(path):
                for f in filenames:
                    fp = os.path.join(dirpath, f)
                    if not os.path.islink(fp):
                        total_size += os.path.getsize(fp)
                        file_count += 1
        except Exception:
            pass
        return total_size, file_count
        
    def ensure_run(mt, rid):
        if mt not in stats:
            stats[mt] = {"total_bytes": 0, "runs": {}}
        if rid not in stats[mt]["runs"]:
            stats[mt]["runs"][rid] = {
                "data_bytes": 0, "data_files": 0,
                "tiles_bytes": 0, "tiles_files": 0,
                "json_bytes": 0, "json_files": 0,
                "staging_bytes": 0, "staging_files": 0,
            }

    try:
        from ..core.pipeline_adapter import get_map_specs
        for mt in get_map_specs().keys():
            stats[mt] = {"total_bytes": 0, "runs": {}}
            
        tasks = []
        if cfg.TILES_DIR.exists():
            for mt in os.listdir(cfg.TILES_DIR):
                mt_path = cfg.TILES_DIR / mt
                if mt_path.is_dir():
                    for run_id in os.listdir(mt_path):
                        if (mt_path / run_id).is_dir():
                            tasks.append(("tiles", mt, run_id, mt_path / run_id))
        
        if cfg.DATA_DIR.exists():
            for mt in os.listdir(cfg.DATA_DIR):
                mt_path = cfg.DATA_DIR / mt
                if mt_path.is_dir():
                    for run_id in os.listdir(mt_path):
                        if (mt_path / run_id).is_dir():
                            tasks.append(("data", mt, run_id, mt_path / run_id))

        if cfg.STAGING_DIR.exists():
            for mt in os.listdir(cfg.STAGING_DIR):
                mt_path = cfg.STAGING_DIR / mt
                if mt_path.is_dir():
                    for run_id in os.listdir(mt_path):
                        if (mt_path / run_id).is_dir():
                            tasks.append(("staging", mt, run_id, mt_path / run_id))

        if cfg.JSON_GRIDS_DIR.exists():
            for mt in os.listdir(cfg.JSON_GRIDS_DIR):
                mt_path = cfg.JSON_GRIDS_DIR / mt
                if mt_path.is_dir():
                    for fname in os.listdir(mt_path):
                        if fname.endswith(".json"):
                            run_id = fname[:-5]
                            tasks.append(("json", mt, run_id, mt_path / fname))

        total_tasks = len(tasks)
        processed_tasks = 0
        _storage_stats_cache["progress"] = 0

        for t_type, mt, run_id, path in tasks:
            if t_type == "tiles":
                size, count = get_size(path)
                ensure_run(mt, run_id)
                stats[mt]["runs"][run_id]["tiles_bytes"] = size
                stats[mt]["runs"][run_id]["tiles_files"] = count
                stats[mt]["total_bytes"] += size
                total_bytes_all += size
            elif t_type == "data":
                size, count = get_size(path)
                ensure_run(mt, run_id)
                stats[mt]["runs"][run_id]["data_bytes"] = size
                stats[mt]["runs"][run_id]["data_files"] = count
                stats[mt]["total_bytes"] += size
                total_bytes_all += size
            elif t_type == "staging":
                size, count = get_size(path)
                ensure_run(mt, run_id)
                stats[mt]["runs"][run_id]["staging_bytes"] = size
                stats[mt]["runs"][run_id]["staging_files"] = count
                stats[mt]["total_bytes"] += size
                total_bytes_all += size
            elif t_type == "json":
                size = os.path.getsize(path)
                ensure_run(mt, run_id)
                stats[mt]["runs"][run_id]["json_bytes"] = size
                stats[mt]["runs"][run_id]["json_files"] = 1
                stats[mt]["total_bytes"] += size
                total_bytes_all += size
            
            processed_tasks += 1
            if total_tasks > 0:
                _storage_stats_cache["progress"] = int((processed_tasks / total_tasks) * 100)

        _storage_stats_cache["data"] = {
            "total_bytes": total_bytes_all,
            "map_types": stats
        }
        _storage_stats_cache["last_updated"] = time.time()
    finally:
        _storage_stats_cache["is_computing"] = False

@router.get("/storage/stats")
def get_storage_stats(background_tasks: BackgroundTasks, force: bool = False) -> dict:
    import time
    global _storage_stats_cache
    
    if force:
        _storage_stats_cache["data"] = None
        _storage_stats_cache["is_computing"] = False
    
    if _storage_stats_cache["data"] and time.time() - _storage_stats_cache["last_updated"] < 300:
        return _storage_stats_cache["data"]
        
    if not _storage_stats_cache["is_computing"]:
        _storage_stats_cache["is_computing"] = True
        _storage_stats_cache["progress"] = 0
        background_tasks.add_task(_compute_storage_stats)
        
    if _storage_stats_cache["data"]:
        return _storage_stats_cache["data"]
        
    return {"status": "computing", "progress": _storage_stats_cache.get("progress", 0)}


# ---------------------------------------------------------------------------
# Background delete jobs
# ---------------------------------------------------------------------------

@router.post("/delete-jobs/{map_type}")
def enqueue_delete_map(
    map_type: str,
    run_id: Optional[str] = Query(
        default=None,
        description="Specific run_id to delete. Omit to delete ALL runs for this map_type.",
    ),
) -> dict:
    """Start a background job to delete data for a map_type.

    Pass ?run_id=20260419_12z to delete only that run; omit it to delete
    every run found on disk.
    """
    from ..core.pipeline_adapter import get_map_specs
    from ..services.delete_service import enqueue_delete, is_map_locked
    from ..services.availability_service import parse_run_id

    specs = get_map_specs()
    if map_type not in specs:
        raise HTTPException(
            status_code=404, detail=f"Unknown map_type: {map_type!r}")

    if run_id is not None:
        try:
            parse_run_id(run_id)
        except ValueError:
            raise HTTPException(
                status_code=400, detail=f"Invalid run_id format: {run_id!r}")

    if is_map_locked(map_type):
        raise HTTPException(
            status_code=409, detail=f"{map_type!r} is already being deleted")

    global _storage_stats_cache
    _storage_stats_cache["data"] = None

    job_id = enqueue_delete([map_type], run_ids=[run_id] if run_id else None)
    return {
        "job_id": job_id,
        "status": "pending",
        "map_type": map_type,
        "run_id": run_id,
    }

@router.get("/delete-jobs")
def get_delete_jobs() -> list[dict]:
    """Get status of all delete jobs."""
    from ..services.delete_service import list_jobs
    return list_jobs()


@router.post("/bulk-delete")
def enqueue_bulk_delete(
    map_types: list[str] = Body(...,
                                description="List of map_type strings to delete"),
    run_ids: Optional[list[str]] = Body(
        default=None,
        description="Specific run_ids to delete from each map_type. Omit to delete ALL runs.",
    ),
) -> dict:
    """Start a background job to delete data for multiple map types in parallel."""
    from ..core.pipeline_adapter import get_map_specs
    from ..services.delete_service import enqueue_delete, is_map_locked
    from ..services.availability_service import parse_run_id

    if not map_types:
        raise HTTPException(
            status_code=400, detail="map_types must not be empty")

    specs = get_map_specs()
    unknown = [mt for mt in map_types if mt not in specs]
    if unknown:
        raise HTTPException(
            status_code=404, detail=f"Unknown map_types: {unknown}")

    if run_ids:
        for rid in run_ids:
            try:
                parse_run_id(rid)
            except ValueError:
                raise HTTPException(
                    status_code=400, detail=f"Invalid run_id format: {rid!r}")

    locked = [mt for mt in map_types if is_map_locked(mt)]
    if locked:
        raise HTTPException(
            status_code=409, detail=f"Already being deleted: {locked}")

    job_id = enqueue_delete(map_types, run_ids=run_ids)
    return {
        "job_id": job_id,
        "status": "pending",
        "map_types": map_types,
        "run_ids": run_ids,
    }


@router.get("/delete-jobs")
def list_delete_jobs() -> list[dict]:
    """List all delete jobs (in-memory, current server session)."""
    from ..services.delete_service import list_jobs as _list_jobs
    return _list_jobs()


@router.get("/delete-jobs/{job_id}")
def get_delete_job(job_id: str) -> dict:
    """Get status and progress of a specific delete job."""
    from ..services.delete_service import get_job
    job = get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=404, detail=f"Delete job not found: {job_id!r}")
    return job.to_dict()


# ---------------------------------------------------------------------------
# User API Management
# ---------------------------------------------------------------------------

@router.get("/user-api/status")
def user_api_status() -> dict:
    from ..services.user_api_manager import get_user_api_status
    return get_user_api_status()

@router.get("/user-api/logs")
def user_api_logs(limit: int = Query(default=100)) -> dict:
    from ..services.user_api_manager import get_user_api_logs
    lines = get_user_api_logs(limit)
    return {"lines": lines}

@router.post("/user-api/start")
def user_api_start() -> dict:
    from ..services.user_api_manager import start_user_api
    return start_user_api()

@router.post("/user-api/stop")
def user_api_stop() -> dict:
    from ..services.user_api_manager import stop_user_api
    return stop_user_api()

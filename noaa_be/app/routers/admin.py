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

import logging
from typing import Optional, Any, Dict

from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, Request

log = logging.getLogger(__name__)
from fastapi.responses import StreamingResponse

from ..core.auth import verify_admin_token
from ..schemas.map import AdminJobStatus
from ..services.scheduler_service import get_all_job_status, trigger_job

router = APIRouter(prefix="/admin", dependencies=[Depends(verify_admin_token)])


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

def _validate_run_id_or_400(run_id: Optional[str]) -> Optional[str]:
    """Reject malformed run_id early. Empty/None → returns None (auto-discover)."""
    if not run_id:
        return None
    from ..services.availability_service import parse_run_id
    try:
        parse_run_id(run_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid run_id: {exc}")
    return run_id


@router.post("/trigger-job/{map_type}")
def trigger_map_job(
    map_type: str,
    background_tasks: BackgroundTasks,
    max_fff: Optional[int] = Query(
        default=None,
        description="Clamp spec fff list to f ≤ max_fff. None = full auto spec.",
    ),
    run_id: Optional[str] = Query(
        default=None,
        description="Manual override of GFS cycle (format YYYYMMDD_HHz, e.g. 20260514_00z). "
                    "If omitted, auto-discover the latest cycle on NOAA.",
    ),
) -> dict:
    """Fire the scheduler job for one map_type immediately in background."""
    from ..core.pipeline_adapter import get_map_specs
    specs = get_map_specs()
    if map_type not in specs:
        raise HTTPException(
            status_code=404, detail=f"Unknown map_type: {map_type!r}")
    rid = _validate_run_id_or_400(run_id)
    background_tasks.add_task(trigger_job, map_type, max_fff, rid)
    return {"queued": True, "map_type": map_type, "max_fff": max_fff, "run_id": rid}


@router.post("/trigger-job-all")
def trigger_all_jobs(
    background_tasks: BackgroundTasks,
    max_fff: Optional[int] = Query(
        default=None,
        description="Clamp spec fff list to f ≤ max_fff for ALL maps. "
                    "None = full auto spec (same as scheduler cron fire).",
    ),
    run_id: Optional[str] = Query(
        default=None,
        description="Manual override of GFS cycle for all maps (YYYYMMDD_HHz). "
                    "Omit to auto-discover latest cycle per map.",
    ),
    stagger_seconds: float = Query(
        default=5.0,
        ge=0.0,
        le=600.0,
        description="Delay between submitting each map_type. Mirrors auto "
                    "mode's 3-min cron stagger but tighter for manual testing. "
                    "Set to 0 to submit all at once (legacy behaviour).",
    ),
) -> dict:
    """Fire the full auto pipeline for ALL maps. Same code path as scheduler.

    This is the "Run Pipeline" button in the manual admin UI — identical to
    auto except you can pass `max_fff` and/or `run_id`.

    Submits are STAGGERED by `stagger_seconds` (default 5s) so that, like the
    cron-driven auto mode, maps don't all hammer the `_job_executor` (which
    caps at SCHEDULER_CONCURRENCY=2) simultaneously. With a short `max_fff`
    each map finishes quickly within the next stagger window, so the queue
    drains as fast as the workers can handle.
    """
    import threading

    from ..core.pipeline_adapter import get_map_specs
    specs = get_map_specs()
    rid = _validate_run_id_or_400(run_id)
    map_types = list(specs.keys())

    def _staggered_submit():
        for i, mt in enumerate(map_types):
            if i > 0 and stagger_seconds > 0:
                # threading.Event.wait is interruptible (vs time.sleep) — keeps
                # the worker thread responsive if FastAPI shuts down mid-stagger.
                threading.Event().wait(stagger_seconds)
            try:
                trigger_job(mt, max_fff, rid)
            except Exception as exc:
                log.exception(
                    "trigger_job failed during staggered submit for %s: %s",
                    mt, exc,
                )

    background_tasks.add_task(_staggered_submit)

    return {
        "queued": True,
        "map_types": map_types,
        "max_fff": max_fff,
        "run_id": rid,
        "stagger_seconds": stagger_seconds,
        "note": (
            f"Same pipeline as auto. Submits staggered by {stagger_seconds}s. "
            f"Concurrency cap (SCHEDULER_CONCURRENCY, default 2) still applies."
        ),
    }


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

    if not explicit_fffs:
        return {
            "status": "error",
            "message": f"Không có frame nào trong khoảng max_fff={max_fff} cho {map_type}. "
                       f"fff_segments_full bắt đầu từ {segment_fff(MAP_SPECS[map_type].fff_segments_full)[0] if MAP_SPECS.get(map_type) else '?'}.",
        }

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
    max_fff: Optional[int] = Body(
        default=None,
        description="Giới hạn forecast hour tối đa cho generate. None = dùng spec đầy đủ. "
                    "Dùng để test với vài frame đầu (vd max_fff=12 cho rain_basic → 3 frames f006/f009/f012).",
    ),
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
            if max_fff is not None:
                fffs = [f for f in fffs if f <= max_fff]
                if not fffs:
                    raise RuntimeError(
                        f"max_fff={max_fff} loại bỏ hết frame của {map_type}; "
                        f"min spec fff = {_compute_needed_fffs(map_type, cfg)[0]}"
                    )
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


@router.post("/finalize-bunny/{map_type}/{run_id}")
def finalize_bunny(
    map_type: str,
    run_id: str,
    cleanup_local: bool = Query(
        default=True,
        description="Xoá DATA/TILES/STAGING/GRID local sau khi pointer switch OK. "
                    "Mặc định True — Bunny là canonical store, local chỉ là tạm.",
    ),
) -> dict:
    """Write/refresh `_current.json` + `_timeline.json` lên Bunny cho run này.

    Dùng khi:
    - Pipeline đã push chunks lên Bunny nhưng chưa kịp gọi finalize (crash giữa chừng)
    - Manual Step 3 sinh tile xong và cần FE thấy được

    Sau khi pointer switch OK, **xoá toàn bộ local data** của (map_type, run_id)
    để tránh ứ đọng đĩa khi Bunny đã có data. Pass `cleanup_local=false` nếu
    muốn giữ lại để debug.
    """
    from ..config import get_settings
    from ..services.bunny_storage import get_bunny_client
    from ..services.scheduler_service import _cleanup_local_after_bunny_finalize
    from ..services.timeline_builder import build_timeline_static

    cfg = get_settings()
    if not cfg.BUNNY_ENABLED:
        raise HTTPException(
            status_code=400, detail="BUNNY_ENABLED=0 → cannot finalize")

    try:
        bunny = get_bunny_client()
    except Exception as exc:
        raise HTTPException(
            status_code=500, detail=f"Cannot init Bunny client: {exc}")
    if bunny is None:
        raise HTTPException(
            status_code=500, detail="Bunny client unavailable")

    # 1. Write pointer (preserve previous_run from existing pointer)
    existing_ptr = bunny.read_pointer(map_type)
    prev = existing_ptr.get("previous_run") if existing_ptr else None
    if existing_ptr and existing_ptr.get("current_run") and existing_ptr["current_run"] != run_id:
        prev = existing_ptr["current_run"]

    pointer_ok = bunny.write_pointer(map_type, current_run=run_id, previous_run=prev)

    # 2. Build + upload timeline (bunny_run_ready=True since pointer now matches)
    timeline_doc = build_timeline_static(map_type, run_id, cfg, bunny_run_ready=True)
    if not timeline_doc["frames"]:
        return {
            "pointer_ok": pointer_ok,
            "timeline_ok": False,
            "reason": "no spec frames available in availability JSON",
            "map_type": map_type,
            "run_id": run_id,
        }
    timeline_ok = bunny.write_timeline_metadata(map_type, timeline_doc)

    # 3. Cleanup local — Bunny is canonical, local is dead weight after switch
    cleanup_done = False
    if cleanup_local and pointer_ok:
        try:
            _cleanup_local_after_bunny_finalize(map_type, run_id, cfg)
            cleanup_done = True
        except Exception as exc:
            log.warning(
                "Local cleanup after finalize failed for %s/%s: %s",
                map_type, run_id, exc,
            )

    return {
        "pointer_ok": bool(pointer_ok),
        "local_cleaned": cleanup_done,
        "timeline_ok": bool(timeline_ok),
        "frame_count": len(timeline_doc["frames"]),
        "map_type": map_type,
        "run_id": run_id,
    }


@router.post("/cleanup-local/{map_type}/{run_id}")
def cleanup_local(map_type: str, run_id: str) -> dict:
    """Xoá local DATA/TILES/STAGING/GRIDS cho (map_type, run_id) — KHÔNG đụng Bunny.

    Dùng khi tiles đã đẩy lên Bunny thành công nhưng local còn ứ đọng
    (vd. finalize bị crash, hoặc Step 3 manual chưa được dọn). Khác với
    `DELETE /runs/{mt}/{run_id}` ở chỗ KHÔNG xoá Bunny mirror.
    """
    from ..config import get_settings
    from ..services.scheduler_service import _cleanup_local_after_bunny_finalize

    cfg = get_settings()
    try:
        _cleanup_local_after_bunny_finalize(map_type, run_id, cfg)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"map_type": map_type, "run_id": run_id, "status": "cleaned"}


@router.post("/cleanup-orphan-local")
def cleanup_orphan_local(
    dry_run: bool = Body(default=True, description="True = chỉ liệt kê, không xoá"),
) -> dict:
    """Quét local TILES/DATA/STAGING/GRIDS, xoá run nào đã có pointer match trên Bunny.

    Ý nghĩa: nếu Bunny `_current.json` của map đã trỏ tới run_id ↔ Bunny là
    canonical → local không cần nữa. Đây là cleanup-on-demand cho mọi backlog
    do crash giữa chừng hoặc manual flow trước khi fix.

    Trả về danh sách (map_type, run_id) đã/sẽ xoá + tổng dung lượng giải phóng.
    """
    import os as _os
    from ..config import get_settings
    from ..services.bunny_storage import get_bunny_client
    from ..services.scheduler_service import _cleanup_local_after_bunny_finalize

    cfg = get_settings()
    if not cfg.BUNNY_ENABLED:
        raise HTTPException(status_code=400, detail="BUNNY_ENABLED=0")

    bunny = get_bunny_client()
    if bunny is None:
        raise HTTPException(status_code=500, detail="Bunny client unavailable")

    # Build set of (mt, run_id) present locally
    candidates: set[tuple[str, str]] = set()
    for base in (cfg.TILES_DIR, cfg.DATA_DIR, cfg.STAGING_DIR, cfg.JSON_GRIDS_DIR):
        if not base.exists():
            continue
        for mt_dir in base.iterdir():
            if not mt_dir.is_dir():
                continue
            for rid_dir in mt_dir.iterdir():
                if rid_dir.is_dir():
                    candidates.add((mt_dir.name, rid_dir.name))

    def _dir_size(p) -> int:
        total = 0
        if not p.exists():
            return 0
        try:
            for dp, _, fns in _os.walk(p):
                for fn in fns:
                    fp = _os.path.join(dp, fn)
                    if not _os.path.islink(fp):
                        total += _os.path.getsize(fp)
        except OSError:
            pass
        return total

    # For each candidate, check Bunny pointer
    cleaned: list[dict] = []
    skipped: list[dict] = []
    pointer_cache: dict[str, str | None] = {}

    for map_type, run_id in sorted(candidates):
        try:
            if map_type not in pointer_cache:
                ptr = bunny.read_pointer(map_type)
                pointer_cache[map_type] = ptr.get("current_run") if ptr else None
            bunny_current = pointer_cache[map_type]
        except Exception as exc:
            skipped.append({"map_type": map_type, "run_id": run_id, "reason": f"pointer read failed: {exc}"})
            continue

        if bunny_current != run_id:
            skipped.append({
                "map_type": map_type, "run_id": run_id,
                "reason": f"Bunny pointer={bunny_current!r} != run_id (not safe to clean)",
            })
            continue

        # Bunny pointer matches → local is dead weight
        size_bytes = sum(
            _dir_size(b / map_type / run_id)
            for b in (cfg.TILES_DIR, cfg.DATA_DIR, cfg.STAGING_DIR, cfg.JSON_GRIDS_DIR)
        )
        entry = {"map_type": map_type, "run_id": run_id, "bytes_freed": size_bytes}
        if not dry_run:
            try:
                _cleanup_local_after_bunny_finalize(map_type, run_id, cfg)
                entry["status"] = "deleted"
            except Exception as exc:
                entry["status"] = f"error: {exc}"
        else:
            entry["status"] = "dry_run"
        cleaned.append(entry)

    total_bytes = sum(c.get("bytes_freed", 0) for c in cleaned)
    return {
        "dry_run": dry_run,
        "total_bytes_freed": total_bytes,
        "total_mb_freed": round(total_bytes / 1e6, 1),
        "cleaned": cleaned,
        "skipped": skipped,
    }


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
                if map_type == "rain_advanced":
                    try:
                        generate_grid(map_type, run_id, fff, "rain_advanced",
                                      data_dir=cfg.DATA_DIR, grids_dir=cfg.JSON_GRIDS_DIR)
                    except Exception as exc:
                        log.warning("generate_grid failed for %s/%s/f%03d: %s",
                                    map_type, run_id, fff, exc)
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
    """Permanently delete all on-disk data + Bunny mirror for a (map_type, run_id).

    Wraps the same `_delete_run` used by background delete jobs so behaviour is
    identical: dọn local (tiles, staging, data, grids, availability) + dọn Bunny
    (pointer/timeline reset + run prefix purge). Idempotent — safe to call on
    map_types đã bị remove khỏi MAP_SPECS.
    """
    from ..config import get_settings
    from ..services.availability_service import parse_run_id
    from ..services.delete_service import _delete_run

    try:
        parse_run_id(run_id)
    except ValueError:
        raise HTTPException(
            status_code=400, detail=f"Invalid run_id format: {run_id!r}")

    cfg = get_settings()
    try:
        _delete_run(map_type, run_id, cfg)
    except Exception as exc:
        log.exception("delete_run sync failed %s/%s", map_type, run_id)
        raise HTTPException(status_code=500, detail=str(exc))

    log.info("delete_run sync %s/%s complete", map_type, run_id)
    return {"map_type": map_type, "run_id": run_id, "status": "deleted"}



_storage_stats_cache: Dict[str, Any] = {"data": None, "is_computing": False, "last_updated": 0}

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
        except Exception as exc:
            log.warning("storage size walk failed for %s: %s", path, exc)
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

        # JSON grids: structure is {map_type}/{run_id}/{fff:03d}/{product}.json
        # — scan run_id dirs the same way TILES_DIR and STAGING_DIR are scanned.
        if cfg.JSON_GRIDS_DIR.exists():
            for mt in os.listdir(cfg.JSON_GRIDS_DIR):
                mt_path = cfg.JSON_GRIDS_DIR / mt
                if mt_path.is_dir():
                    for run_id in os.listdir(mt_path):
                        run_path = mt_path / run_id
                        if run_path.is_dir():
                            tasks.append(("json", mt, run_id, run_path))

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
                size, count = get_size(path)
                ensure_run(mt, run_id)
                stats[mt]["runs"][run_id]["json_bytes"] = size
                stats[mt]["runs"][run_id]["json_files"] = count
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
    from ..services.delete_service import enqueue_delete, is_map_locked
    from ..services.availability_service import parse_run_id

    # KHÔNG check map_type vs specs — cho phép dọn data cũ của map đã bỏ khỏi spec
    # (vd: cloud_total, cloud_layered đã remove). Delete service chỉ thao tác
    # trên dir thực sự tồn tại nên an toàn.

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

    # KHÔNG check vs specs — cho phép dọn data của map cũ đã remove khỏi spec.
    # Nhưng vẫn enforce slug-safe charset để tránh path-traversal trên local FS.
    import re as _re
    _slug_re = _re.compile(r"^[a-z0-9_]+$")
    for mt in map_types:
        if not isinstance(mt, str) or not _slug_re.match(mt):
            raise HTTPException(
                status_code=400,
                detail=f"Invalid map_type {mt!r}: must match [a-z0-9_]+",
            )

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
# Cold Zone Info
# ---------------------------------------------------------------------------

@router.get("/cold-zone/info")
def cold_zone_info() -> dict:
    """Return multi-tier cold zone configuration and daily cycle schedule."""
    from ..core.map_specs import MAP_SPECS, segment_fff, tier_max_age_for_fff, tier_frame_groups

    result = {}
    for map_type, spec in MAP_SPECS.items():
        fffs = segment_fff(spec.fff_segments_full)
        hot_fffs = [f for f in fffs if tier_max_age_for_fff(spec, f) is None]
        total_cold = len(fffs) - len(hot_fffs)

        # Build per-tier frame counts, ranges, and stagger info
        tiers_info = []
        for tier_idx, tier in enumerate(spec.cold_tiers):
            fff_min, max_age_h = tier[0], tier[1]
            stagger_n = tier[2] if len(tier) > 2 else 1
            next_fff_min = spec.cold_tiers[tier_idx + 1][0] if tier_idx + 1 < len(spec.cold_tiers) else None
            tier_fffs = [
                f for f in fffs
                if f >= fff_min and (next_fff_min is None or f < next_fff_min)
            ]
            frames_per_cycle = (len(tier_fffs) + stagger_n - 1) // stagger_n if stagger_n > 1 else len(tier_fffs)
            tiers_info.append({
                "fff_min":           fff_min,
                "max_age_h":         max_age_h,
                "stagger_n":         stagger_n,
                "refresh_per_day":   24 // max_age_h,
                "frame_count":       len(tier_fffs),
                "frames_per_cycle":  frames_per_cycle,
                "fff_range":         f"f{tier_fffs[0]:03d}–f{tier_fffs[-1]:03d}" if tier_fffs else None,
            })

        # Precompute stagger groups (needed for accurate per-cycle frame counts)
        has_stagger = any(len(t) > 2 and t[2] > 1 for t in spec.cold_tiers)
        frame_groups = tier_frame_groups(spec, fffs) if has_stagger else {}

        def _tier_fffs_for_idx(tier_idx: int) -> list:
            tier = spec.cold_tiers[tier_idx]
            fff_min = tier[0]
            next_min = spec.cold_tiers[tier_idx + 1][0] if tier_idx + 1 < len(spec.cold_tiers) else None
            return [f for f in fffs if f >= fff_min and (next_min is None or f < next_min)]

        def _cycle_detail(hour: int) -> dict:
            if not spec.cold_tiers:
                return {"action": "full", "frames_gen": len(fffs), "frames_link": 0}

            cycle_slot = hour // 6
            frames_gen = len(hot_fffs)
            frames_link = 0

            for tier_idx, tier in enumerate(spec.cold_tiers):
                max_age_h = tier[1]
                stagger_n = tier[2] if len(tier) > 2 else 1
                t_fffs = _tier_fffs_for_idx(tier_idx)

                if stagger_n > 1:
                    n_groups = stagger_n
                    group = cycle_slot % n_groups
                    n_in_group = sum(1 for f in t_fffs if frame_groups.get(f, 0) == group)
                    frames_gen += n_in_group
                    frames_link += len(t_fffs) - n_in_group
                else:
                    if hour % max_age_h == 0:
                        frames_gen += len(t_fffs)
                    else:
                        frames_link += len(t_fffs)

            if frames_link == 0:
                action = "full_gen"
            elif frames_gen == len(hot_fffs):
                action = "gen_and_link"
            else:
                action = "partial_refresh"

            detail: dict = {"action": action, "frames_gen": frames_gen, "frames_link": frames_link}
            for tier in spec.cold_tiers:
                if len(tier) > 2 and tier[2] > 1:
                    detail["stagger_group"] = (hour // 6) % tier[2]
                    detail["stagger_n"] = tier[2]
                    break
            return detail

        schedule_detail = {
            "00z": _cycle_detail(0),
            "06z": _cycle_detail(6),
            "12z": _cycle_detail(12),
            "18z": _cycle_detail(18),
        }

        result[map_type] = {
            "cold_tiers":             tiers_info,
            "cold_fff_min":           spec.cold_fff_min if spec.cold_tiers else None,
            "cold_max_age_h":         spec.cold_max_age_h if spec.cold_tiers else None,
            "total_frames":           len(fffs),
            "hot_frames":             len(hot_fffs),
            "cold_frames":            total_cold,
            "hot_range":              f"f{hot_fffs[0]:03d}–f{hot_fffs[-1]:03d}" if hot_fffs else None,
            "cold_range":             (f"f{spec.cold_fff_min:03d}–f{fffs[-1]:03d}"
                                       if spec.cold_tiers and total_cold > 0 else None),
            # backward-compat string schedule
            "cycle_schedule": {k: v["action"] for k, v in schedule_detail.items()},
            # rich schedule with frame counts and stagger info
            "cycle_schedule_detail":  schedule_detail,
        }
    return result


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Benchmark Mode
# ---------------------------------------------------------------------------

import subprocess
import sys
import json
from pathlib import Path

_benchmark_process = None
_last_benchmark_mode = "baseline"

_ALLOWED_BENCHMARK_MODES = frozenset(
    {"baseline", "stable-prod", "scheduler_realistic", "cold_zone", "predict"}
)


@router.post("/benchmark/start")
def benchmark_start(
    mode: str = Body(default="baseline"),
    multi_frame: bool = Body(default=False),
    cold_frames: int = Body(default=1),
) -> dict:
    global _benchmark_process, _last_benchmark_mode
    if mode not in _ALLOWED_BENCHMARK_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid mode {mode!r}; allowed: {sorted(_ALLOWED_BENCHMARK_MODES)}",
        )
    if _benchmark_process is not None and _benchmark_process.poll() is None:
        return {"status": "error", "message": "Benchmark is already running."}

    _last_benchmark_mode = mode
    cmd = [sys.executable, "-m", "app.benchmark", "--mode", mode]
    if multi_frame:
        cmd.append("--multi-frame")
    if mode == "cold_zone" and cold_frames > 1:
        cmd += ["--cold-frames", str(cold_frames)]

    _benchmark_process = subprocess.Popen(cmd)
    return {"status": "started", "pid": _benchmark_process.pid}

@router.get("/benchmark/status")
def benchmark_status() -> dict:
    global _benchmark_process, _last_benchmark_mode
    is_running = _benchmark_process is not None and _benchmark_process.poll() is None

    results = []
    mode_run = _last_benchmark_mode
    daily_projection = {}
    grand_total = {}
    generated_at = None

    if not is_running:
        try:
            if mode_run == "cold_zone":
                summary_file = Path("benchmark_cold_zone_report.json")
                if summary_file.exists():
                    data = json.loads(summary_file.read_text())
                    results          = data.get("results", [])
                    daily_projection = data.get("daily_projection", {})
                    grand_total      = data.get("grand_total", {})
                    generated_at     = data.get("generated_at")
            else:
                summary_file = Path(f"benchmark_first_frame_summary_{mode_run}.json")
                if not summary_file.exists():
                    summary_file = Path("benchmark_first_frame_summary.json")
                if summary_file.exists():
                    data = json.loads(summary_file.read_text())
                    results  = data.get("benchmark_results", [])
                    mode_run = data.get("benchmark_mode", mode_run)
        except Exception:
            pass

    status_str = "running" if is_running else ("idle" if _benchmark_process is None else "complete")

    return {
        "status":           status_str,
        "mode":             mode_run,
        "results":          results,
        "daily_projection": daily_projection,
        "grand_total":      grand_total,
        "generated_at":     generated_at,
    }


@router.get("/benchmark/cold-zone-log")
def cold_zone_log(lines: int = Query(default=200)) -> dict:
    """Return the most recent cold zone benchmark log file (last N lines)."""
    import glob as _glob
    log_files = sorted(_glob.glob("benchmark_cold_zone_*.log"), reverse=True)
    if not log_files:
        return {"log_file": None, "lines": []}
    log_path = Path(log_files[0])
    try:
        all_lines = log_path.read_text().splitlines()
        return {"log_file": log_path.name, "lines": all_lines[-lines:]}
    except Exception as e:
        return {"log_file": log_path.name, "lines": [f"Error reading log: {e}"]}


# ---------------------------------------------------------------------------
# Cycle history (cycle_metrics queries)
# ---------------------------------------------------------------------------

def _cycle_status(rows: list[dict]) -> str:
    """Derive overall cycle status from cycle_metrics rows of a single run."""
    if not rows:
        return "unknown"
    perm = sum((r.get("permanent_errors") or 0) for r in rows)
    if perm > 0:
        return "error"
    switches = [r.get("pointer_switch_ok") for r in rows]
    if any(s is False for s in switches):
        return "error"
    transient = sum((r.get("transient_errors") or 0) for r in rows)
    if transient > 0:
        return "warning"
    return "ok"


@router.get("/cycles")
def list_cycles(days: int = Query(default=7, ge=1, le=90)) -> list[dict]:
    """List cycles finished in the last N days, grouped by run_id."""
    from datetime import datetime, timezone, timedelta
    from ..core.db import get_cycle_metrics_between

    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=days)).isoformat()
    date_to = (now + timedelta(hours=1)).isoformat()

    rows = get_cycle_metrics_between(date_from, date_to)

    # Group by run_id
    by_run: dict[str, list[dict]] = {}
    for r in rows:
        by_run.setdefault(r["run_id"], []).append(r)

    cycles = []
    for run_id, group in by_run.items():
        starts = [g["started_at"] for g in group if g.get("started_at")]
        ends = [g["finished_at"] for g in group if g.get("finished_at")]
        wall = None
        if starts and ends:
            try:
                t0 = datetime.fromisoformat(min(starts).replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(max(ends).replace("Z", "+00:00"))
                wall = (t1 - t0).total_seconds()
            except (ValueError, AttributeError):
                pass
        cycles.append({
            "run_id": run_id,
            "started_at": min(starts) if starts else None,
            "finished_at": max(ends) if ends else None,
            "total_wall_seconds": wall,
            "maps_done": sum(1 for g in group if g.get("finished_at")),
            "maps_total": len(group),
            "total_bytes_uploaded": sum((g.get("bytes_uploaded") or 0) for g in group),
            "total_bytes_cold": sum(
                ((g.get("bytes_cold_get") or 0) + (g.get("bytes_cold_put") or 0))
                for g in group
            ),
            "peak_local_staging_bytes": max(
                (g.get("peak_local_staging_bytes") or 0) for g in group
            ),
            "bunny_storage_after_bytes": max(
                (g.get("bunny_storage_after_bytes") or 0) for g in group
            ),
            "transient_errors": sum((g.get("transient_errors") or 0) for g in group),
            "permanent_errors": sum((g.get("permanent_errors") or 0) for g in group),
            "status": _cycle_status(group),
        })
    cycles.sort(key=lambda c: c["finished_at"] or "", reverse=True)
    return cycles


@router.get("/cycles/{run_id}")
def get_cycle_detail(run_id: str) -> dict:
    """Detail of one cycle: aggregate summary + all map rows."""
    from datetime import datetime
    from ..core.db import get_cycle_metrics_by_run

    rows = get_cycle_metrics_by_run(run_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No cycle_metrics for run_id {run_id!r}")

    starts = [r["started_at"] for r in rows if r.get("started_at")]
    ends = [r["finished_at"] for r in rows if r.get("finished_at")]
    wall = None
    if starts and ends:
        try:
            t0 = datetime.fromisoformat(min(starts).replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(max(ends).replace("Z", "+00:00"))
            wall = (t1 - t0).total_seconds()
        except (ValueError, AttributeError):
            pass

    summary = {
        "run_id": run_id,
        "started_at": min(starts) if starts else None,
        "finished_at": max(ends) if ends else None,
        "total_wall_seconds": wall,
        "sum_cpu_seconds": sum((r.get("total_wall_seconds") or 0) for r in rows),
        "maps_done": sum(1 for r in rows if r.get("finished_at")),
        "maps_total": len(rows),
        "total_bytes_uploaded": sum((r.get("bytes_uploaded") or 0) for r in rows),
        "total_bytes_cold_get": sum((r.get("bytes_cold_get") or 0) for r in rows),
        "total_bytes_cold_put": sum((r.get("bytes_cold_put") or 0) for r in rows),
        "peak_local_staging_bytes": max(
            (r.get("peak_local_staging_bytes") or 0) for r in rows
        ),
        "bunny_storage_after_bytes": max(
            (r.get("bunny_storage_after_bytes") or 0) for r in rows
        ),
        "transient_errors": sum((r.get("transient_errors") or 0) for r in rows),
        "permanent_errors": sum((r.get("permanent_errors") or 0) for r in rows),
        "status": _cycle_status(rows),
    }
    return {"run_id": run_id, "summary": summary, "maps": rows}


@router.get("/cycles/{run_id}/{map_type}")
def get_cycle_map_detail(run_id: str, map_type: str) -> dict:
    """Single cycle_metrics row for (run_id, map_type)."""
    from ..core.db import get_cycle_metrics_by_run

    rows = get_cycle_metrics_by_run(run_id)
    for r in rows:
        if r["map_type"] == map_type:
            return r
    raise HTTPException(
        status_code=404,
        detail=f"No metric for map_type={map_type!r} in run {run_id!r}",
    )


# ---------------------------------------------------------------------------
# Bunny analytics + storage
# ---------------------------------------------------------------------------

@router.get("/bunny/analytics")
def bunny_analytics(days: int = Query(default=1, ge=1, le=90)) -> dict:
    """Hourly bunny_analytics_hourly rows + aggregated summary."""
    from datetime import datetime, timezone, timedelta
    from ..core.db import get_bunny_analytics_between
    from ..config import get_settings
    import json as _json

    cfg = get_settings()
    if not cfg.BUNNY_ACCOUNT_API_KEY:
        return {"enabled": False, "rows": [], "summary": {}}

    now = datetime.now(timezone.utc)
    date_from = (now - timedelta(days=days)).isoformat()
    date_to = now.isoformat()
    rows = get_bunny_analytics_between(date_from, date_to)

    if not rows:
        return {"enabled": True, "rows": [], "summary": {}}

    total_pulls = sum((r.get("pull_requests") or 0) for r in rows)
    total_bw = sum((r.get("bandwidth_bytes") or 0) for r in rows)
    total_4xx = sum((r.get("error_4xx") or 0) for r in rows)
    total_5xx = sum((r.get("error_5xx") or 0) for r in rows)
    if total_pulls > 0:
        weighted_hit = sum(
            (r.get("cache_hit_ratio") or 0) * (r.get("pull_requests") or 0)
            for r in rows
        )
        cache_hit_ratio = weighted_hit / total_pulls
    else:
        cache_hit_ratio = 0.0

    country_counter: dict[str, int] = {}
    for r in rows:
        try:
            cs = _json.loads(r.get("top_countries_json") or "[]")
            for entry in cs:
                code = entry.get("code", "?")
                country_counter[code] = country_counter.get(code, 0) + entry.get("requests", 0)
        except (ValueError, TypeError):
            continue
    top_countries = sorted(
        country_counter.items(), key=lambda kv: kv[1], reverse=True,
    )[:10]

    peak_row = max(rows, key=lambda r: r.get("pull_requests") or 0)
    error_rate = ((total_4xx + total_5xx) / total_pulls) if total_pulls else 0.0

    return {
        "enabled": True,
        "rows": rows,
        "summary": {
            "pulls": total_pulls,
            "bandwidth_bytes": total_bw,
            "cache_hit_ratio": cache_hit_ratio,
            "error_4xx": total_4xx,
            "error_5xx": total_5xx,
            "error_rate": error_rate,
            "top_countries": [
                {"code": c, "requests": n} for c, n in top_countries
            ],
            "peak_hour": peak_row.get("timestamp"),
            "peak_hour_pulls": peak_row.get("pull_requests"),
        },
    }


@router.get("/bunny/storage")
def bunny_storage_used() -> dict:
    """Live query Bunny Account API for current StorageUsed."""
    from datetime import datetime, timezone
    from ..services.bunny_analytics import get_bunny_analytics_client

    client = get_bunny_analytics_client()
    if client is None:
        return {"enabled": False, "bytes": 0, "gb": 0.0}
    try:
        b = client.get_storage_used()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Bunny API error: {exc}")
    return {
        "enabled": True,
        "bytes": b,
        "gb": round(b / 1e9, 3),
        "measured_at": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Telegram test + preview
# ---------------------------------------------------------------------------

@router.post("/telegram/test")
def telegram_test() -> dict:
    """Send a test message via Telegram bot to verify config."""
    from datetime import datetime, timezone
    from ..services.telegram_reporter import get_telegram_reporter

    reporter = get_telegram_reporter()
    if reporter is None:
        return {
            "sent": False,
            "error": "Telegram disabled or misconfigured (check TELEGRAM_ENABLED, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID).",
        }
    try:
        ok = reporter.send(
            f"🧪 *Admin test message*\n"
            f"Sent at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
        )
        return {"sent": bool(ok)}
    except Exception as exc:
        return {"sent": False, "error": str(exc)}


@router.get("/telegram/preview/{type}")
def telegram_preview(type: str) -> dict:
    """Render a sample Telegram alert (per_map | per_cycle | daily) without sending.

    Uses the most recent real cycle_metrics if available, otherwise fake data.
    """
    from datetime import datetime, timezone, timedelta
    from ..core.db import get_cycle_metrics_between, get_cycle_metrics_by_run
    from ..services.telegram_reporter import (
        format_per_map_report,
        format_per_cycle_report,
        format_daily_report,
    )

    if type not in ("per_map", "per_cycle", "daily"):
        raise HTTPException(
            status_code=400,
            detail="type must be one of: per_map, per_cycle, daily",
        )

    now = datetime.now(timezone.utc)
    recent = get_cycle_metrics_between(
        (now - timedelta(days=7)).isoformat(),
        (now + timedelta(hours=1)).isoformat(),
    )

    if type == "per_map":
        if recent:
            md = format_per_map_report(recent[-1])
        else:
            md = format_per_map_report({
                "map_type": "wind_surface", "run_id": "20260510_06z",
                "started_at": "2026-05-10T12:00:00+00:00",
                "finished_at": "2026-05-10T12:51:37+00:00",
                "total_wall_seconds": 3097,
                "frames_total": 231, "frames_generated": 117, "frames_cold_copied": 114,
                "chunks_uploaded_ok": 1366, "chunks_uploaded_failed": 0,
                "bytes_uploaded": 47_500_000_000,
                "peak_local_staging_bytes": 2_100_000_000,
                "bunny_storage_after_bytes": 90_300_000_000,
                "pointer_switch_ok": True,
                "pointer_switched_at": "2026-05-10T12:51:34+00:00",
            })
    elif type == "per_cycle":
        if recent:
            run_id = recent[-1]["run_id"]
            rows = get_cycle_metrics_by_run(run_id)
            md = format_per_cycle_report(rows)
        else:
            md = "(No recent cycle data — preview unavailable)"
    else:  # daily
        from ..services.cycle_tracker import _build_daily_report
        # Yesterday window
        today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        date_from = today_midnight - timedelta(days=1)
        date_to = today_midnight
        daily = _build_daily_report(date_from, date_to)
        md = format_daily_report(daily)

    return {"type": type, "markdown": md}


# ---------------------------------------------------------------------------
# Public config dump (read-only, no secrets)
# ---------------------------------------------------------------------------

def _mask(value: str, keep: int = 8) -> str:
    if not value:
        return ""
    return value[:keep] + "…" if len(value) > keep else value


@router.get("/config/public")
def config_public() -> dict:
    """Read-only dump of non-secret config knobs for the admin UI."""
    from ..config import get_settings

    cfg = get_settings()
    return {
        "server": {
            "HOST": cfg.HOST,
            "PORT": cfg.PORT,
            "APP_VERSION": getattr(cfg, "APP_VERSION", "?"),
        },
        "tile": {
            "TILE_WORKERS": cfg.TILE_WORKERS,
            "TILE_ZOOM_EAGER_MAX": cfg.TILE_ZOOM_EAGER_MAX,
            "TILE_ZOOM_LAZY_MAX": cfg.TILE_ZOOM_LAZY_MAX,
            "TILE_CACHE_MB": cfg.TILE_CACHE_MB,
        },
        "retention": {
            "KEEP_CYCLES": cfg.KEEP_CYCLES,
            "CLOUD_KEEP_CYCLES": cfg.CLOUD_KEEP_CYCLES,
        },
        "scheduler": {
            "SCHEDULER_ENABLED": cfg.SCHEDULER_ENABLED,
            "CHECK_INTERVAL_MINUTES": cfg.CHECK_INTERVAL_MINUTES,
        },
        "bunny": {
            "BUNNY_ENABLED": cfg.BUNNY_ENABLED,
            "BUNNY_STORAGE_ZONE": cfg.BUNNY_STORAGE_ZONE,
            "BUNNY_REGION": cfg.BUNNY_REGION,
            "BUNNY_PATH_PREFIX": cfg.BUNNY_PATH_PREFIX,
            "BUNNY_PULL_ZONE_URL": cfg.BUNNY_PULL_ZONE_URL,
            "BUNNY_MAX_PARALLEL": cfg.BUNNY_MAX_PARALLEL,
            "BUNNY_RETRY_ATTEMPTS": cfg.BUNNY_RETRY_ATTEMPTS,
            "BUNNY_TIMEOUT_S": cfg.BUNNY_TIMEOUT_S,
            "BUNNY_FAIL_FAST": cfg.BUNNY_FAIL_FAST,
            "BUNNY_DELETE_PREV_AFTER_SWITCH": cfg.BUNNY_DELETE_PREV_AFTER_SWITCH,
            "BUNNY_API_KEY_MASK": _mask(cfg.BUNNY_API_KEY),
            "BUNNY_ACCOUNT_API_KEY_MASK": _mask(cfg.BUNNY_ACCOUNT_API_KEY),
            "BUNNY_PULL_ZONE_ID": cfg.BUNNY_PULL_ZONE_ID,
            "BUNNY_STORAGE_ZONE_ID": cfg.BUNNY_STORAGE_ZONE_ID,
        },
        "telegram": {
            "TELEGRAM_ENABLED": cfg.TELEGRAM_ENABLED,
            "TELEGRAM_VERBOSITY": cfg.TELEGRAM_VERBOSITY,
            "TELEGRAM_CHAT_ID": cfg.TELEGRAM_CHAT_ID,
            "TELEGRAM_BOT_TOKEN_MASK": _mask(cfg.TELEGRAM_BOT_TOKEN),
        },
        "reporting": {
            "DAILY_REPORT_UTC_HOUR": cfg.DAILY_REPORT_UTC_HOUR,
            "DAILY_REPORT_UTC_MINUTE": cfg.DAILY_REPORT_UTC_MINUTE,
            "BUNNY_ANALYTICS_POLL_MIN": cfg.BUNNY_ANALYTICS_POLL_MIN,
        },
    }


# ---------------------------------------------------------------------------
# Scheduler control — enable/disable + status
# ---------------------------------------------------------------------------

@router.get("/scheduler")
def scheduler_status() -> dict:
    """Return scheduler state: enabled (persistent), running (in-process), jobs."""
    from ..services.scheduler_service import get_scheduler_info
    return get_scheduler_info()


@router.post("/scheduler/enable")
def scheduler_enable() -> dict:
    """Persist enabled=true + start scheduler if not already running.

    Persists the choice in DB so the next restart honours it.
    """
    from ..services.scheduler_service import enable_scheduler
    return enable_scheduler()


@router.post("/scheduler/disable")
def scheduler_disable() -> dict:
    """Persist enabled=false + stop scheduler.

    In-flight map jobs run to completion; only future cron fires are cancelled.
    """
    from ..services.scheduler_service import disable_scheduler
    return disable_scheduler()


# ---------------------------------------------------------------------------
# SSE stream — real-time job status + progress + logs
# ---------------------------------------------------------------------------

@router.get("/stream")
async def admin_stream(request: Request):
    """Server-Sent Events: push job_status, progress, logs in real time.

    Events emitted:
      - hello   (initial handshake, empty payload)
      - jobs    (when get_all_job_status output changes)
      - progress (when get_all_progress output changes)
      - logs    (incremental new log lines since last seq)
      - ping    (keepalive every ~20s of idle)
    """
    import asyncio
    import json as _json
    import time as _time
    from ..core.db import get_all_job_status, get_all_progress
    from ..services.log_buffer import get_lines, get_max_seq

    # Cap a single SSE connection at 1h to avoid permanently-held sockets
    # from forgotten browser tabs. Clients reconnect automatically.
    _SSE_MAX_DURATION_S = 3600.0

    async def gen():
        last_log_seq = max(0, get_max_seq() - 50)  # backfill last 50 lines
        last_jobs_hash = None
        last_prog_hash = None
        idle_ticks = 0
        started = _time.monotonic()

        yield "event: hello\ndata: {}\n\n"

        while True:
            if await request.is_disconnected():
                break
            if (_time.monotonic() - started) > _SSE_MAX_DURATION_S:
                yield "event: bye\ndata: {\"reason\":\"max_duration\"}\n\n"
                break

            emitted = False
            try:
                jobs = get_all_job_status()
                jh = hash(_json.dumps(jobs, sort_keys=True, default=str))
                if jh != last_jobs_hash:
                    yield f"event: jobs\ndata: {_json.dumps(jobs, default=str)}\n\n"
                    last_jobs_hash = jh
                    emitted = True

                prog = get_all_progress()
                ph = hash(_json.dumps(prog, sort_keys=True, default=str))
                if ph != last_prog_hash:
                    yield f"event: progress\ndata: {_json.dumps(prog, default=str)}\n\n"
                    last_prog_hash = ph
                    emitted = True

                new_lines = get_lines(since_seq=last_log_seq, limit=20)
                if new_lines:
                    last_log_seq = get_max_seq()
                    yield (
                        "event: logs\n"
                        f"data: {_json.dumps({'max_seq': last_log_seq, 'lines': new_lines}, default=str)}\n\n"
                    )
                    emitted = True
            except Exception as exc:
                yield f"event: error\ndata: {_json.dumps({'message': str(exc)})}\n\n"

            if emitted:
                idle_ticks = 0
            else:
                idle_ticks += 1
                if idle_ticks >= 10:  # ~20s with 2s sleep
                    yield "event: ping\ndata: {}\n\n"
                    idle_ticks = 0

            await asyncio.sleep(2)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ---------------------------------------------------------------------------
# Diagnostics — health snapshot + ZIP bundle export for offline review
# ---------------------------------------------------------------------------

def _collect_diagnostics(probe_noaa: bool = True) -> dict:
    """Health snapshot of all subsystems. Pure function; safe to call from
    /diagnostics and /diagnostics/export without re-entering FastAPI."""
    import time as _time
    from datetime import datetime, timezone, timedelta
    from ..config import get_settings

    cfg = get_settings()
    checks: dict[str, dict] = {}
    now = datetime.now(tz=timezone.utc)

    # 1. DB connectivity
    try:
        from ..core.db import get_cycle_metrics_between
        recent_24h = get_cycle_metrics_between(
            (now - timedelta(hours=24)).isoformat(),
            now.isoformat(),
        )
        checks["db"] = {"ok": True, "cycle_metrics_last_24h": len(recent_24h)}
    except Exception as exc:
        checks["db"] = {"ok": False, "error": str(exc)}

    # 2. Scheduler
    try:
        from ..services.scheduler_service import get_scheduler_info
        info = get_scheduler_info()
        checks["scheduler"] = {
            "ok": bool(info.get("running")),
            "enabled": info.get("enabled"),
            "running": info.get("running"),
            "active_slots": info.get("active_slots"),
            "queued_jobs": info.get("queued_jobs"),
            "next_fires": {
                j.get("id", j.get("name", "?")): j.get("next_run")
                for j in info.get("jobs", [])
            },
        }
    except Exception as exc:
        checks["scheduler"] = {"ok": False, "error": str(exc)}

    # 3. Pipeline orchestrator
    try:
        from ..services.pipeline_orchestrator import get_orchestrator
        orch = get_orchestrator()
        checks["orchestrator"] = {
            "ok": bool(orch.is_running),
            "is_running": bool(orch.is_running),
            "active_runs": sorted(list(orch.active_runs)),
            "publishing_runs": sorted(list(orch.publishing_runs)),
        }
    except Exception as exc:
        checks["orchestrator"] = {"ok": False, "error": str(exc)}

    # 4. Resources (disk / RAM / CPU / IOWait)
    try:
        from ..services.resource_guard import get_resource_metrics
        m = get_resource_metrics()
        ok = (
            m.get("disk_free_gb", 0) >= cfg.MIN_DISK_FREE_GB
            and m.get("ram_percent", 0) <= cfg.MAX_RAM_PERCENT
        )
        checks["resources"] = {"ok": ok, **m}
    except Exception as exc:
        checks["resources"] = {"ok": False, "error": str(exc)}

    # 5. Last cycle health
    try:
        from ..core.db import get_cycle_metrics_between
        last_48h = get_cycle_metrics_between(
            (now - timedelta(hours=48)).isoformat(),
            now.isoformat(),
        )
        if last_48h:
            last_run_id = max(
                (r.get("run_id", "") for r in last_48h if r.get("run_id")),
                default="",
            )
            same_run = [r for r in last_48h if r.get("run_id") == last_run_id]
            finished = sum(1 for r in same_run if r.get("finished_at"))
            pointer_ok = all(r.get("pointer_switch_ok") for r in same_run)
            age_hours: float | None = None
            try:
                from ..services.availability_service import parse_run_id
                d, h = parse_run_id(last_run_id)
                from datetime import datetime as _dt
                run_dt = _dt(d.year, d.month, d.day, h, tzinfo=timezone.utc)
                age_hours = round((now - run_dt).total_seconds() / 3600, 1)
            except Exception:
                pass
            checks["last_cycle"] = {
                "ok": (
                    finished >= 5
                    and pointer_ok
                    and (age_hours is None or age_hours <= 8)
                ),
                "run_id": last_run_id,
                "finished_maps": finished,
                "total_maps_in_run": len(same_run),
                "age_hours": age_hours,
                "pointer_switch_ok": pointer_ok,
            }
        else:
            checks["last_cycle"] = {"ok": False, "error": "No cycle in last 48h"}
    except Exception as exc:
        checks["last_cycle"] = {"ok": False, "error": str(exc)}

    # 6. Bunny CDN pointer
    if cfg.BUNNY_ENABLED:
        try:
            from ..services.bunny_storage import get_bunny_client
            bunny = get_bunny_client()
            ptr = bunny.read_pointer("rain_basic")
            checks["bunny"] = {
                "ok": ptr is not None,
                "enabled": True,
                "sample_map": "rain_basic",
                "current_run": (ptr or {}).get("current_run"),
                "previous_run": (ptr or {}).get("previous_run"),
            }
        except Exception as exc:
            checks["bunny"] = {"ok": False, "enabled": True, "error": str(exc)}
    else:
        checks["bunny"] = {"ok": True, "enabled": False, "note": "Bunny disabled by config"}

    # 7. NOAA reachability (HEAD request, ~5s timeout) — optional
    if probe_noaa:
        try:
            import urllib.request
            t0 = _time.perf_counter()
            req = urllib.request.Request(
                "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/",
                method="HEAD",
            )
            with urllib.request.urlopen(req, timeout=5) as resp:
                status = resp.status
            latency_ms = int((_time.perf_counter() - t0) * 1000)
            checks["noaa_probe"] = {
                "ok": status < 500,
                "reachable": True,
                "latency_ms": latency_ms,
                "status": status,
            }
        except Exception as exc:
            checks["noaa_probe"] = {"ok": False, "reachable": False, "error": str(exc)}

    overall_ok = all(c.get("ok", False) for c in checks.values())
    return {
        "ok": overall_ok,
        "checks": checks,
        "generated_at": now.isoformat(),
    }


@router.get("/diagnostics")
def diagnostics(probe_noaa: bool = Query(default=True)) -> dict:
    """Snapshot health check of all auto-pipeline subsystems.

    Pass `probe_noaa=false` to skip the 5s HTTP probe (useful for fast polling).
    Top-level `ok` is true only when every subsystem check is healthy.
    """
    return _collect_diagnostics(probe_noaa=probe_noaa)


def _redact_config(cfg) -> dict:
    """Return cfg's instance attributes with secret-looking values masked."""
    SECRET_SUBSTRS = ("token", "key", "password", "secret")
    out: dict = {}
    for name, val in vars(cfg).items():
        if name.startswith("_"):
            continue
        name_lower = name.lower()
        if isinstance(val, str) and val and any(s in name_lower for s in SECRET_SUBSTRS):
            out[name] = _mask(val)
            continue
        # Coerce non-JSON-native values
        if hasattr(val, "__fspath__"):
            out[name] = str(val)
        elif isinstance(val, (set, frozenset)):
            out[name] = sorted(val)
        else:
            out[name] = val
    return out


def _build_readme(now_iso: str, mode: str, date_or_days: str) -> str:
    coverage = (
        f"Single day: {date_or_days} (UTC)"
        if mode == "date"
        else f"Last {date_or_days} day(s)"
    )
    return (
        "NOAA BE Diagnostic Bundle\n"
        "=========================\n\n"
        f"Generated at: {now_iso}\n"
        f"Coverage:     {coverage}\n\n"
        "Files in this bundle\n"
        "--------------------\n"
        "  README.txt                  — this file\n"
        "  current_state.json          — health snapshot at export time (7 subsystem checks)\n"
        "  app.log                     — text logs (capped 50 MB)\n"
        "  events.jsonl                — structured JSON-per-line logs (capped 50 MB)\n"
        "  cycle_metrics.json          — DB dump: per-(map,cycle) metrics for the coverage window\n"
        "  pipeline_jobs_recent.json   — pipeline_jobs rows for the coverage window (up to 500)\n"
        "  config_redacted.json        — non-secret config values (API keys/tokens masked)\n\n"
        "How to use\n"
        "----------\n"
        "  1. Open current_state.json — look for ok=false in any check\n"
        "  2. Search events.jsonl for ERROR/WARNING around the timeframe of interest\n"
        "  3. Cross-reference cycle_metrics.json for failed run/map\n"
        "  4. Send this entire ZIP to Claude (or support) for analysis\n"
    )


@router.get("/diagnostics/log-dates")
def diagnostics_log_dates() -> dict:
    """List the UTC dates for which logs exist on disk (today + rotated days)."""
    try:
        from ..services.log_files import list_available_dates
        return {"dates": list_available_dates()}
    except Exception as exc:
        return {"dates": [], "error": str(exc)}


@router.get("/diagnostics/export")
def diagnostics_export(
    days: int = Query(default=7, ge=1, le=7,
                      description="Aggregate mode: include last N days of logs/metrics."),
    date: Optional[str] = Query(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description="Single-day mode: 'YYYY-MM-DD' (UTC). Overrides `days` when set.",
    ),
    include_app_log: bool = Query(default=True),
    include_events: bool = Query(default=True),
):
    """Download a ZIP bundle: logs + cycle metrics + current health snapshot.

    Two modes (mutually exclusive — `date` takes precedence if both are passed):
      - `days=N` (default 7) — aggregate logs/metrics for the last N days.
      - `date=YYYY-MM-DD`    — only that single UTC date (smaller, focused bundle).

    Secrets (API keys, tokens) are always masked.
    """
    import io
    import json as _json
    import sqlite3
    import zipfile
    from datetime import datetime, timezone, timedelta
    from ..config import get_settings

    cfg = get_settings()
    now = datetime.now(tz=timezone.utc)

    # Determine coverage window
    if date:
        # Single-day mode — strict UTC day boundaries
        try:
            day_start = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid date: {date}")
        day_end = day_start + timedelta(days=1)
        coverage_start_iso = day_start.isoformat()
        coverage_end_iso = day_end.isoformat()
        mode = "date"
        mode_label = date
        filename = f"noaa_be_diag_{date.replace('-', '')}.zip"
    else:
        coverage_start_iso = (now - timedelta(days=days)).isoformat()
        coverage_end_iso = now.isoformat()
        mode = "days"
        mode_label = str(days)
        filename = f"noaa_be_diag_{now.strftime('%Y%m%d_%H%M%S')}.zip"

    # Per-file caps (combined ~100MB worst case for ZIP-deflated text)
    MAX_LOG_BYTES = 50 * 1024 * 1024

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        # 1. README
        zf.writestr("README.txt", _build_readme(now.isoformat(), mode, mode_label))

        # 2. Current state snapshot (skip NOAA probe to keep it fast)
        try:
            state = _collect_diagnostics(probe_noaa=False)
            zf.writestr("current_state.json",
                        _json.dumps(state, indent=2, ensure_ascii=False, default=str))
        except Exception as exc:
            zf.writestr("current_state.json",
                        _json.dumps({"error": str(exc)}, indent=2))

        # 3. app.log
        if include_app_log:
            try:
                if mode == "date":
                    from ..services.log_files import collect_log_text_for_date
                    data = collect_log_text_for_date("app.log", date, max_bytes=MAX_LOG_BYTES)
                else:
                    from ..services.log_files import collect_log_text
                    data = collect_log_text("app.log", max_bytes=MAX_LOG_BYTES)
                zf.writestr("app.log", data if data else b"(no logs collected)")
            except Exception as exc:
                zf.writestr("app.log", f"(failed to collect: {exc})".encode("utf-8"))

        # 4. events.jsonl
        if include_events:
            try:
                if mode == "date":
                    from ..services.log_files import collect_log_text_for_date
                    data = collect_log_text_for_date("events.jsonl", date, max_bytes=MAX_LOG_BYTES)
                else:
                    from ..services.log_files import collect_log_text
                    data = collect_log_text("events.jsonl", max_bytes=MAX_LOG_BYTES)
                zf.writestr("events.jsonl", data if data else b"(no events collected)")
            except Exception as exc:
                zf.writestr("events.jsonl", f"(failed to collect: {exc})".encode("utf-8"))

        # 5. cycle_metrics.json — filtered to coverage window
        try:
            from ..core.db import get_cycle_metrics_between
            cycles = get_cycle_metrics_between(coverage_start_iso, coverage_end_iso)
            zf.writestr("cycle_metrics.json",
                        _json.dumps(cycles, indent=2, ensure_ascii=False, default=str))
        except Exception as exc:
            zf.writestr("cycle_metrics.json",
                        _json.dumps({"error": str(exc)}, indent=2))

        # 6. pipeline_jobs — filtered to coverage window by updated_at (unix ts)
        try:
            if mode == "date":
                ts_start = day_start.timestamp()
                ts_end = day_end.timestamp()
            else:
                ts_start = (now - timedelta(days=days)).timestamp()
                ts_end = now.timestamp()
            conn = sqlite3.connect(str(cfg.SHARED_DB_PATH))
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT id, map_type, run_id, fff, product, state, updated_at, error "
                "FROM pipeline_jobs "
                "WHERE updated_at >= ? AND updated_at < ? "
                "ORDER BY updated_at DESC LIMIT 500",
                (ts_start, ts_end),
            )
            rows = [dict(r) for r in cur.fetchall()]
            conn.close()
            zf.writestr("pipeline_jobs_recent.json",
                        _json.dumps(rows, indent=2, ensure_ascii=False, default=str))
        except Exception as exc:
            zf.writestr("pipeline_jobs_recent.json",
                        _json.dumps({"error": str(exc)}, indent=2))

        # 7. config_redacted.json
        try:
            redacted = _redact_config(cfg)
            zf.writestr("config_redacted.json",
                        _json.dumps(redacted, indent=2, ensure_ascii=False, default=str))
        except Exception as exc:
            zf.writestr("config_redacted.json",
                        _json.dumps({"error": str(exc)}, indent=2))

    buf.seek(0)
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )

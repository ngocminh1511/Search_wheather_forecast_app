from __future__ import annotations

"""
delete_service.py — background parallel deletion of map data.

Job flow:
  1. enqueue_delete(map_types, run_ids) → job_id
  2. Background thread starts _run_delete_job(job_id)
  3. Map types locked → scheduler jobs paused for those types
  4. _delete_map_type() runs in parallel across map_types
     (up to MAX_CONCURRENT_MAP_DELETES threads)
  5. _delete_run() runs in parallel per map_type
     (up to MAX_CONCURRENT_BATCHES_PER_MAP threads)
  6. On finish: map types unlocked, scheduler jobs resumed

Storage paths deleted per (map_type, run_id):
  DATA_DIR      / {map_type} / {run_id}                          (raw GRIB2)
  TILES_DIR     / {map_type} / {run_id}                          (live tiles)
  STAGING_DIR   / {map_type} / {run_id}                          (staging tiles)
  JSON_GRIDS_DIR/ {map_type} / {run_id}                          (JSON grids)
  AVAILABLE_DIR / {map_type} / availability_{run_id}_{map_type}.json
  AVAILABLE_DIR / availability_{run_id}.json  (entry removed; file deleted if empty)

All operations are idempotent (missing_ok / ignore_errors=True).
"""

import json
import logging
import re
import shutil
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..config import get_settings

log = logging.getLogger(__name__)

MAX_CONCURRENT_MAP_DELETES = 4
MAX_CONCURRENT_BATCHES_PER_MAP = 8

# ---------------------------------------------------------------------------
# In-memory state
# ---------------------------------------------------------------------------

DELETE_JOBS: dict[str, "DeleteJob"] = {}
_LOCKED_MAP_TYPES: set[str] = set()
_state_lock = threading.Lock()


# ---------------------------------------------------------------------------
# DeleteJob dataclass
# ---------------------------------------------------------------------------

@dataclass
class DeleteJob:
    job_id: str
    map_types: list[str]
    run_ids: list[str] | None   # None → delete ALL runs for each map_type
    status: str = "pending"     # pending | running | done | failed | partial
    created_at: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat())
    started_at: str | None = None
    finished_at: str | None = None
    progress: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "map_types": self.map_types,
            "run_ids": self.run_ids,
            "status": self.status,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "progress": self.progress,
            "errors": self.errors,
        }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def is_map_locked(map_type: str) -> bool:
    """Return True if this map_type is currently being deleted."""
    with _state_lock:
        return map_type in _LOCKED_MAP_TYPES


def enqueue_delete(
    map_types: list[str],
    run_ids: list[str] | None = None,
) -> str:
    """Create and start a background delete job. Returns job_id."""
    job_id = str(uuid.uuid4())
    job = DeleteJob(job_id=job_id, map_types=list(map_types), run_ids=run_ids)
    DELETE_JOBS[job_id] = job
    t = threading.Thread(target=_run_delete_job, args=(job_id,), daemon=True)
    t.start()
    return job_id


def get_job(job_id: str) -> DeleteJob | None:
    return DELETE_JOBS.get(job_id)


def list_jobs() -> list[dict]:
    return [j.to_dict() for j in DELETE_JOBS.values()]


# ---------------------------------------------------------------------------
# Internal job runner
# ---------------------------------------------------------------------------

def _run_delete_job(job_id: str) -> None:
    job = DELETE_JOBS[job_id]
    cfg = get_settings()

    # Acquire locks — reject if any map_type is already being deleted
    with _state_lock:
        conflicts = [mt for mt in job.map_types if mt in _LOCKED_MAP_TYPES]
        if conflicts:
            job.status = "failed"
            job.errors.append(f"Map types already being deleted: {conflicts}")
            job.finished_at = datetime.now(tz=timezone.utc).isoformat()
            return
        for mt in job.map_types:
            _LOCKED_MAP_TYPES.add(mt)

    job.status = "running"
    job.started_at = datetime.now(tz=timezone.utc).isoformat()
    log.info(
        "Delete job %s started: map_types=%s run_ids=%s",
        job_id, job.map_types, job.run_ids,
    )

    # Pause scheduler so ingest doesn't race with deletion
    _pause_scheduler_jobs(job.map_types)

    # Initialise per-map progress
    for mt in job.map_types:
        job.progress[mt] = {
            "status": "pending",
            "runs_total": 0,
            "runs_done": 0,
            "errors": 0,
        }

    try:
        with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_MAP_DELETES) as pool:
            futures = {
                pool.submit(_delete_map_type, job, mt, cfg): mt
                for mt in job.map_types
            }
            for fut in as_completed(futures):
                mt = futures[fut]
                try:
                    fut.result()
                except Exception as exc:
                    log.exception("Delete failed for map_type=%s", mt)
                    job.errors.append(f"{mt}: {exc}")
                    job.progress[mt]["status"] = "failed"

        job.status = "done" if not job.errors else "partial"
    except Exception as exc:
        job.status = "failed"
        job.errors.append(str(exc))
        log.exception("Delete job %s failed", job_id)
    finally:
        with _state_lock:
            for mt in job.map_types:
                _LOCKED_MAP_TYPES.discard(mt)
        _resume_scheduler_jobs(job.map_types)
        job.finished_at = datetime.now(tz=timezone.utc).isoformat()
        log.info(
            "Delete job %s finished: status=%s errors=%d",
            job_id, job.status, len(job.errors),
        )


def _delete_map_type(job: DeleteJob, map_type: str, cfg: Any) -> None:
    prog = job.progress[map_type]
    prog["status"] = "running"

    run_ids = job.run_ids or _discover_run_ids(map_type, cfg)
    prog["runs_total"] = len(run_ids)
    log.info("Deleting map_type=%s (%d run(s))", map_type, len(run_ids))

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_BATCHES_PER_MAP) as pool:
        futures = {
            pool.submit(_delete_run, map_type, rid, cfg): rid
            for rid in run_ids
        }
        for fut in as_completed(futures):
            rid = futures[fut]
            try:
                fut.result()
                prog["runs_done"] += 1
            except Exception as exc:
                prog["errors"] += 1
                job.errors.append(f"{map_type}/{rid}: {exc}")
                log.warning("_delete_run failed %s/%s: %s", map_type, rid, exc)

    prog["status"] = (
        "failed" if prog["errors"] > 0 and prog["runs_done"] == 0
        else "partial" if prog["errors"] > 0
        else "done"
    )


def _discover_run_ids(map_type: str, cfg: Any) -> list[str]:
    """Gather all run IDs that have data under any storage directory."""
    run_ids: set[str] = set()
    for base in (cfg.DATA_DIR, cfg.TILES_DIR, cfg.STAGING_DIR, cfg.JSON_GRIDS_DIR):
        mt_dir = base / map_type
        if mt_dir.exists():
            for child in mt_dir.iterdir():
                if child.is_dir():
                    run_ids.add(child.name)
    # Also scan per-map availability directory
    avail_dir = cfg.AVAILABLE_DIR / map_type
    if avail_dir.exists():
        _pat = re.compile(
            rf"^availability_(\d{{8}}_\d{{2}}z)_{re.escape(map_type)}\.json$"
        )
        for f in avail_dir.glob("availability_*.json"):
            m = _pat.match(f.name)
            if m:
                run_ids.add(m.group(1))
    return sorted(run_ids, reverse=True)


def _delete_run(map_type: str, run_id: str, cfg: Any) -> None:
    """Delete all data for (map_type, run_id). Fully idempotent."""
    _rm(cfg.DATA_DIR / map_type / run_id)
    _rm(cfg.TILES_DIR / map_type / run_id)
    _rm(cfg.STAGING_DIR / map_type / run_id)
    _rm(cfg.JSON_GRIDS_DIR / map_type / run_id)

    avail_file = (
        cfg.AVAILABLE_DIR / map_type
        / f"availability_{run_id}_{map_type}.json"
    )
    avail_file.unlink(missing_ok=True)

    _prune_master_availability(run_id, map_type, cfg)
    log.debug("_delete_run done: %s/%s", map_type, run_id)


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

import os
import subprocess

def _rm(path: Path) -> None:
    """Remove a directory tree, using fast OS commands on Windows."""
    if not path.exists():
        return
        
    try:
        # Move to a temp _trash folder first so it vanishes instantly from the live app
        trash_path = path.with_name(path.name + f"_trash_{uuid.uuid4().hex[:8]}")
        try:
            path.rename(trash_path)
            target_to_delete = trash_path
        except OSError:
            # If rename fails (locked file), fallback to deleting in-place
            target_to_delete = path

        if os.name == "nt":
            # Windows 'rd' is 10x-50x faster than shutil.rmtree for millions of tiny files
            subprocess.run(
                ["cmd", "/c", "rd", "/s", "/q", str(target_to_delete.resolve())],
                check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        else:
            shutil.rmtree(target_to_delete, ignore_errors=True)
            
        # Clean up in case 'rd' missed locked files
        if target_to_delete.exists():
            shutil.rmtree(target_to_delete, ignore_errors=True)
            
    except Exception as exc:
        log.warning(f"Fast _rm failed for {path}: {exc}")
        shutil.rmtree(path, ignore_errors=True)


def _prune_master_availability(run_id: str, deleted_map_type: str, cfg: Any) -> None:
    """Remove deleted_map_type from master availability JSON.
    Deletes the file entirely if no map types remain."""
    master = cfg.AVAILABLE_DIR / f"availability_{run_id}.json"
    if not master.exists():
        return
    try:
        data = json.loads(master.read_text())
        map_types = data.get("map_types", {})
        if deleted_map_type not in map_types:
            return
        map_types.pop(deleted_map_type)
        if not map_types:
            master.unlink(missing_ok=True)
        else:
            data["map_types"] = map_types
            master.write_text(json.dumps(data, indent=2))
    except Exception as exc:
        log.warning("_prune_master_availability %s/%s: %s",
                    run_id, deleted_map_type, exc)


# ---------------------------------------------------------------------------
# Scheduler interaction
# ---------------------------------------------------------------------------

def _pause_scheduler_jobs(map_types: list[str]) -> None:
    """Pause APScheduler jobs for the given map_types to prevent race with deletion."""
    try:
        from . import scheduler_service as _svc
        sc = _svc._scheduler
        if sc and sc.running:
            for mt in map_types:
                try:
                    sc.pause_job(f"job_{mt}")
                    log.info("Paused scheduler job: %s", mt)
                except Exception:
                    pass  # Job may not exist if scheduler is disabled
    except Exception as exc:
        log.warning("Could not pause scheduler jobs: %s", exc)


def _resume_scheduler_jobs(map_types: list[str]) -> None:
    """Resume APScheduler jobs for the given map_types."""
    try:
        from . import scheduler_service as _svc
        sc = _svc._scheduler
        if sc and sc.running:
            for mt in map_types:
                try:
                    sc.resume_job(f"job_{mt}")
                    log.info("Resumed scheduler job: %s", mt)
                except Exception:
                    pass
    except Exception as exc:
        log.warning("Could not resume scheduler jobs: %s", exc)

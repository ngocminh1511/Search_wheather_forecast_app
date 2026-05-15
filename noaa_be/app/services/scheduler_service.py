from __future__ import annotations

"""
scheduler_service.py — APScheduler-based background jobs.

noaa_be is fully self-contained: all imports come from noaa_be/app/core/ and
noaa_be/app/services/. No reference to scripts/ or any external path.

One job per map_type runs every SCHEDULER_INTERVAL_MINUTES.
Each job independently:
  1. Probes NOAA for the latest GFS cycle via core.discovery.
  2. Downloads GRIB2 files via core.downloader.
  3. Generates PNG tiles (staging → atomic swap) via services.tile_generator.
  4. Generates JSON grids via services.grid_service (wind + rain_advanced).
  5. Cloud maps: circular buffer (KEEP_CYCLES kept, oldest pruned).

Job state is kept in-memory in JOB_STATUS dict for the /admin/jobs endpoint.
"""

import logging
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor as ApsThreadPoolExecutor

from ..config import get_settings
from ..core.discovery import discover_cycle, find_latest_accessible_cycle, latest_available_run, load_available_fff
from ..core.downloader import download_map, run_id_from_date
from ..core.map_specs import MAP_SPECS, segment_fff
from ..services.availability_service import (
    all_run_ids,
    run_id_to_datetime,
)
from ..core.db import (
    init_db as db_init,
    update_job_status as db_update_job_status,
    get_job_status as db_get_job_status,
    get_all_job_status as db_get_all_job_status,
    reset_cancel_requested as db_reset_cancel,
    check_cancel_requested,
    get_setting as db_get_setting,
    set_setting as db_set_setting,
    JobCancelledError,
)

log = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None
_scheduler_lock = threading.Lock()

# Hard cap on concurrent map jobs. Two heavy maps in parallel is the sweet
# spot: wall-clock stays close to "all parallel" while peak CPU/RAM/NOAA-RPM
# stays bounded. Adjust via SCHEDULER_CONCURRENCY env var.
_CONCURRENCY_LIMIT: int = max(1, int(os.getenv("SCHEDULER_CONCURRENCY", "2")))

# FIFO-ordered worker pool. Using ThreadPoolExecutor instead of bare
# Semaphore so that "trigger-job-all" preserves MAP_SPECS submit order:
# threading.Semaphore.acquire() is NOT FIFO on CPython — when 5 threads
# race for 2 slots the OS scheduler picks arbitrarily, which made manual
# runs occasionally start wind_surface before temperature_feels_like.
# Executor.submit() is documented FIFO: first 2 submissions run immediately,
# remaining queue up and run in submit order as workers free.
from concurrent.futures import ThreadPoolExecutor as _Cf_ThreadPoolExecutor

_job_executor: _Cf_ThreadPoolExecutor = _Cf_ThreadPoolExecutor(
    max_workers=_CONCURRENCY_LIMIT,
    thread_name_prefix="map_job",
)

# Persistence key for the runtime on/off toggle.
_SETTING_KEY_ENABLED = "scheduler_enabled"


# ---------------------------------------------------------------------------
# FFF helpers
# ---------------------------------------------------------------------------

def _compute_needed_fffs(map_type: str, cfg: Any) -> list[int]:
    """
    Return the full fff list we need to download/generate for this map type.

    The fff_segments_full in map_specs.py already encodes the correct per-map
    rolling window including the buffer beyond the user window
    (buffer_h = cycle_interval(6h) + noaa_upload(5h) + proc_buffer(1h) = 12h).
    """
    return segment_fff(MAP_SPECS[map_type].fff_segments_full)


def coverage_sufficient(map_type: str, run_id: str, cfg: Any) -> tuple[bool, str]:
    """
    Check whether existing tiles/grids already cover the needed fff window.

    Returns (True, reason_str) if nothing needs to be downloaded/generated,
    or (False, reason_str) if we still need to do work.
    """
    needed = _compute_needed_fffs(map_type, cfg)
    if not needed:
        return True, "No frames required for this map type"
    if _all_output_ready(map_type, run_id, needed, cfg):
        max_h = needed[-1]
        return True, f"Tiles already cover until f{max_h:03d} ({max_h}h ahead) — sufficient for 24h user window"
    return False, f"Missing output for {map_type}/{run_id} up to f{needed[-1]:03d}"


def _spec_max_fff(map_type: str) -> int:
    """Max fff in the full MAP_SPECS segments (for discovery probing)."""
    spec = MAP_SPECS[map_type]
    return max(end for _, end, _ in spec.fff_segments_full)


# ---------------------------------------------------------------------------
# Post-finalize local cleanup (Bunny mode)
# ---------------------------------------------------------------------------

def _cleanup_local_after_bunny_finalize(map_type: str, run_id: str, cfg: Any) -> None:
    """Remove ALL local data for (map_type, run_id) after successful Bunny finalize.

    When Bunny is the canonical store, local data is only needed during
    processing. Once the pointer has been switched on Bunny, these files
    are dead weight and should be purged to free disk space:
      - DATA_DIR:       raw GRIB2 files
      - TILES_DIR:      live tiles (not served locally in Bunny mode)
      - STAGING_DIR:    staging residuals (most already cleaned per-frame)
      - JSON_GRIDS_DIR: JSON grids (rain_advanced)
      - AVAILABLE_DIR:  per-map availability metadata file
      - AVAILABLE_DIR:  master availability entry (file removed if empty)

    Uses delete_service._rm() for fast Windows-aware deletion (rename-to-trash
    + native `rd /s /q`). After deletion, verifies nothing was left behind and
    logs a warning if cleanup was incomplete (e.g. Windows file lock).
    """
    # Reuse the proven helpers from delete_service so cleanup paths stay in sync.
    from .delete_service import _rm, _prune_master_availability

    dirs_to_remove = [
        cfg.DATA_DIR / map_type / run_id,
        cfg.TILES_DIR / map_type / run_id,
        cfg.STAGING_DIR / map_type / run_id,
        cfg.JSON_GRIDS_DIR / map_type / run_id,
    ]
    removed = 0
    for d in dirs_to_remove:
        if d.exists():
            _rm(d)
            removed += 1
            log.debug("Local cleanup: removed %s", d)

    # Per-map availability metadata
    avail_file = (
        cfg.AVAILABLE_DIR / map_type
        / f"availability_{run_id}_{map_type}.json"
    )
    if avail_file.exists():
        avail_file.unlink(missing_ok=True)
        removed += 1

    # Master availability entry — prune this map_type's slot; the file is
    # deleted entirely once all maps have been finalized for this run_id.
    try:
        _prune_master_availability(run_id, map_type, cfg)
    except Exception as exc:
        log.warning(
            "Cleanup: master availability prune failed %s/%s: %s",
            map_type, run_id, exc,
        )

    # Verification: anything left behind is a red flag (likely Windows lock).
    residuals = [d for d in dirs_to_remove if d.exists()]
    if avail_file.exists():
        residuals.append(avail_file)
    if residuals:
        log.warning(
            "Local cleanup INCOMPLETE for %s/%s — %d item(s) remain: %s",
            map_type, run_id, len(residuals),
            ", ".join(str(p) for p in residuals[:5]),
        )

    if removed:
        log.info(
            "Local cleanup after Bunny finalize: %s/%s — removed %d items (residuals=%d)",
            map_type, run_id, removed, len(residuals),
        )


# ---------------------------------------------------------------------------
# Concurrency throttle
# ---------------------------------------------------------------------------

def _job_for_map_type_throttled(
    map_type: str,
    max_fff_override: int | None = None,
    run_id_override: str | None = None,
) -> None:
    """Direct wrapper kept for APScheduler compatibility.

    The pool (`_job_executor`) already bounds concurrency at `_CONCURRENCY_LIMIT`
    and preserves FIFO submit order — no semaphore needed here.
    """
    _job_for_map_type(
        map_type,
        max_fff_override=max_fff_override,
        run_id_override=run_id_override,
    )


# ---------------------------------------------------------------------------
# Individual map-type job
# ---------------------------------------------------------------------------

def _job_for_map_type(
    map_type: str,
    max_fff_override: int | None = None,
    run_id_override: str | None = None,
) -> None:
    # Guard: skip if this map_type is locked for deletion
    try:
        from ..services.delete_service import is_map_locked
        if is_map_locked(map_type):
            log.info("Skipping scheduler job for %s — locked for deletion", map_type)
            return
    except ImportError:
        pass

    from ..services import progress_tracker
    now = datetime.now(tz=timezone.utc)

    status = db_get_job_status(map_type)
    if status.get("status") == "running":
        # Stale-lock threshold: 30 minutes. Any healthy map job pushes a
        # progress update at least every few seconds, so 30m of silence is a
        # strong signal that the worker died without resetting status.
        # `reset_zombie_jobs()` cleans up after a server restart; this guard
        # handles intra-session hangs.
        last_started = status.get("last_started")
        if last_started:
            try:
                last_dt = datetime.fromisoformat(last_started)
                elapsed = (now - last_dt).total_seconds()
                if elapsed < 1800:
                    log.warning("Job for %s is already running, skipping trigger.", map_type)
                    return
                log.warning(
                    "Job for %s has been 'running' for %.0fs (>30m) — treating as stale, "
                    "re-acquiring lock.", map_type, elapsed,
                )
            except Exception:
                pass

    # --- Reset stale cancel flag BEFORE marking as running ---
    db_reset_cancel(map_type)

    status["last_started"] = now.isoformat()
    status["status"] = "running"
    status.pop("cancel_requested", None)
    db_update_job_status(map_type, status)

    progress_tracker.reset(map_type)
    progress_tracker.update(
        map_type,
        step="checking",
        step_detail="Khởi tạo job…",
        run_id=None,
        frames_total=0,
        frames_done=0,
        tiles_saved=0,
        tiles_skipped=0,
        started_at=now.isoformat(),
    )
    try:
        _run_map_job(
            map_type,
            max_fff_override=max_fff_override,
            run_id_override=run_id_override,
        )
        status = db_get_job_status(map_type)
        status["status"] = "ok"
        status["last_success"] = datetime.now(tz=timezone.utc).isoformat()
        status.pop("last_error", None)
        status.pop("cancel_requested", None)
        db_update_job_status(map_type, status)
        progress_tracker.update(map_type, step="done", step_detail="Hoàn thành ✓")
        # Map recovered → reset pause fingerprint để lần lỗi kế tiếp gửi lại
        try:
            from ..services.pause_notifier import clear_pause
            clear_pause(map_type)
        except Exception:
            pass
    except JobCancelledError:
        log.info("Job cancelled by user: map_type=%s", map_type)
        status = db_get_job_status(map_type)
        status["status"] = "cancelled"
        status["cancel_requested"] = False
        status.pop("last_error", None)
        db_update_job_status(map_type, status)
        progress_tracker.update(map_type, step="cancelled", step_detail="Đã hủy bởi người dùng ✗")
        try:
            from ..services.pause_notifier import notify_pause
            notify_pause(
                map_type,
                reason="cancelled",
                title=f"⏹ {map_type}: Job bị hủy",
                detail=f"Bản đồ `{map_type}` bị hủy thủ công bởi người dùng.",
                level="warning",
            )
        except Exception:
            pass
    except Exception as exc:
        log.exception("Job failed for map_type=%s: %s", map_type, exc)
        status = db_get_job_status(map_type)
        status["status"] = "error"
        status["last_error"] = str(exc)
        status.pop("cancel_requested", None)
        db_update_job_status(map_type, status)
        progress_tracker.update(map_type, step="error", step_detail=str(exc)[:120])
        try:
            from ..services.pause_notifier import notify_pause
            exc_type = type(exc).__name__
            notify_pause(
                map_type,
                reason=f"error::{exc_type}",
                title=f"🚨 {map_type}: Job thất bại",
                detail=(
                    f"*Lỗi*: `{exc_type}`\n"
                    f"```\n{str(exc)[:300]}\n```"
                ),
                level="critical",
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Cold-zone optimisation helpers
# ---------------------------------------------------------------------------

def _cold_fffs_covered(map_type: str, current_run_id: str, fffs: list[int], cfg: Any) -> set[int]:
    """
    Return the subset of fffs that should be hardlinked from a previous cycle
    rather than regenerated this cycle.

    Per-frame tier logic:
      - Hot frames (below first tier) → always regenerate.
      - Standard tier (stagger_n=1): regenerate when run_hour % max_age_h == 0.
      - Staggered tier (stagger_n>1): frame belongs to group = sorted_index % stagger_n;
        regenerate when (run_hour // 6) % stagger_n == group (one group per cycle slot).
    """
    from ..core.map_specs import tier_info_for_fff, tier_frame_groups
    spec = MAP_SPECS[map_type]
    if not spec.cold_tiers:
        return set()

    try:
        run_hour = run_id_to_datetime(current_run_id).hour
    except ValueError:
        return set()

    cycle_slot = run_hour // 6  # 0=00z, 1=06z, 2=12z, 3=18z

    # Precompute stagger groups using full spec fff list so indices are stable
    all_spec_fffs = segment_fff(spec.fff_segments_full)
    frame_groups = tier_frame_groups(spec, all_spec_fffs)

    # Per-frame: determine which frames should be hardlinked at this cycle
    cold_to_hardlink: set[int] = set()
    for f in fffs:
        info = tier_info_for_fff(spec, f)
        if info is None:
            continue  # hot frame — always regenerate
        max_age_h, stagger_n = info
        if stagger_n > 1:
            group = frame_groups.get(f, 0)
            if cycle_slot % stagger_n != group:
                cold_to_hardlink.add(f)
        else:
            if run_hour % max_age_h != 0:
                cold_to_hardlink.add(f)

    if not cold_to_hardlink:
        return set()

    # ── Bunny mode: trust prev_run on Bunny ────────────────────────────────
    # In Bunny mode, LIVE local does not exist (Bunny is canonical store).
    # prev_run on Bunny is guaranteed complete because:
    #   - Atomic pointer switch only happens AFTER all frames pushed
    #   - cold_copy in finalize_map_to_bunny ensures cold frames are present
    # Return all cold-skip frames directly; finalize_map_to_bunny will
    # `copy_run_subset` them from Bunny prev_run → current_run server-side.
    if cfg.BUNNY_ENABLED:
        return cold_to_hardlink

    # ── Legacy mode: verify source files exist in local LIVE ──────────────
    # Use the widest tier window as the source search horizon
    max_search_age = max((t[1] for t in spec.cold_tiers), default=0)

    is_json_only = map_type in cfg.JSON_ONLY_MAP_TYPES
    base_dir = cfg.JSON_GRIDS_DIR if is_json_only else cfg.TILES_DIR
    now = datetime.now(tz=timezone.utc)

    covered: set[int] = set()
    for rid in all_run_ids(cfg.AVAILABLE_DIR):
        if rid == current_run_id:
            continue
        try:
            rid_dt = run_id_to_datetime(rid)
        except ValueError:
            continue
        if (now - rid_dt).total_seconds() / 3600 > max_search_age:
            break  # all_run_ids is newest-first; older cycles won't help

        for fff in cold_to_hardlink - covered:
            fff_dir = base_dir / map_type / rid / f"{fff:03d}"
            if fff_dir.is_dir() and next(fff_dir.rglob("*"), None) is not None:
                covered.add(fff)

        if covered >= cold_to_hardlink:
            break

    return covered


def _hardlink_cold_output(map_type: str, run_id: str, cold_covered: set[int], cfg: Any) -> int:
    """
    Hardlink (or copy on cross-device) cold-zone output from a recent previous
    cycle into the current run_id directory tree.  Works for both tile
    directories and JSON grid files.  Returns the number of files linked.
    """
    if not cold_covered:
        return 0

    import shutil as _shutil

    spec = MAP_SPECS[map_type]
    is_json_only = map_type in cfg.JSON_ONLY_MAP_TYPES
    base_dir = cfg.JSON_GRIDS_DIR if is_json_only else cfg.TILES_DIR
    now = datetime.now(tz=timezone.utc)

    max_search_age = max((t[1] for t in spec.cold_tiers), default=spec.cold_max_age_h)
    recent_run_ids: list[str] = []
    for rid in all_run_ids(cfg.AVAILABLE_DIR):
        if rid == run_id:
            continue
        try:
            rid_dt = run_id_to_datetime(rid)
        except ValueError:
            continue
        age_h = (now - rid_dt).total_seconds() / 3600
        if 0.0 <= age_h <= max_search_age:
            recent_run_ids.append(rid)

    if not recent_run_ids:
        return 0

    linked = 0
    for fff in sorted(cold_covered):
        src_fff_dir: Path | None = None
        for rid in recent_run_ids:
            candidate = base_dir / map_type / rid / f"{fff:03d}"
            if candidate.is_dir() and next(candidate.rglob("*"), None) is not None:
                src_fff_dir = candidate
                break

        if src_fff_dir is None:
            continue

        dst_fff_dir = base_dir / map_type / run_id / f"{fff:03d}"
        for src_file in src_fff_dir.rglob("*"):
            if not src_file.is_file():
                continue
            dst_file = dst_fff_dir / src_file.relative_to(src_fff_dir)
            if dst_file.exists():
                continue
            dst_file.parent.mkdir(parents=True, exist_ok=True)
            try:
                os.link(src_file, dst_file)
            except OSError:
                _shutil.copy2(src_file, dst_file)
            linked += 1

    return linked


def _run_map_job(
    map_type: str,
    max_fff_override: int | None = None,
    run_id_override: str | None = None,
) -> None:
    from ..services import progress_tracker
    from ..services.availability_service import parse_run_id

    cfg = get_settings()

    # ── Reporting metrics: capture stage timestamps as we go ─────────────
    # Populated incrementally; passed to record_map_done() at the end.
    job_started_iso = datetime.now(timezone.utc).isoformat()
    job_t0 = time.perf_counter()
    metrics: dict = {
        "started_at": job_started_iso,
        "transient_errors": 0,
        "permanent_errors": 0,
    }

    # 1. Determine which GFS cycle to probe.
    #    Strategy: probe backward from current UTC time to find the most recent cycle
    #    that is actually accessible on NOAA.  This avoids the scheduler targeting a
    #    stale cycle from the on-disk availability files whose data has already been
    #    purged from NOAA (causing 404 on every download attempt).
    max_fff = _spec_max_fff(map_type)
    if run_id_override:
        # Manual override path — caller specified an exact cycle. Validate
        # format (`YYYYMMDD_HHz`) and skip NOAA cycle probing entirely.
        try:
            date_str, run_hour = parse_run_id(run_id_override)
        except ValueError as exc:
            raise ValueError(
                f"Invalid run_id_override {run_id_override!r}: {exc}"
            )
        from datetime import date as _date
        probe_date = _date(
            int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])
        )
        probe_hour = run_hour
        progress_tracker.update(
            map_type, step="checking",
            step_detail=f"Manual run_id={run_id_override} (bỏ qua auto-discover)",
        )
        log.info("Manual override: %s using run_id=%s", map_type, run_id_override)
    else:
        progress_tracker.update(map_type, step="checking",
                                step_detail="Tìm chu kỳ GFS mới nhất trên NOAA…")
        probe_date, probe_hour = find_latest_accessible_cycle(max_fff)
        if probe_date is None:
            # NOAA unreachable — fall back to the latest we have on disk
            probe_date, probe_hour = latest_available_run(cfg.AVAILABLE_DIR)
        if probe_date is None or probe_hour is None:
            utc_now = datetime.now(tz=timezone.utc)
            probe_date = utc_now.date()
            probe_hour = (utc_now.hour // 6) * 6

    run_id = run_id_from_date(probe_date, probe_hour)
    progress_tracker.update(map_type, run_id=run_id)

    # 2. Check if we already have all expected tiles/grids
    expected = _compute_needed_fffs(map_type, cfg)
    if max_fff_override is not None:
        expected = [f for f in expected if f <= max_fff_override]
        if not expected:
            log.warning(
                "max_fff=%d clamped %s spec to empty — nothing to do",
                max_fff_override, map_type,
            )
            progress_tracker.update(
                map_type, step="done",
                step_detail=f"max_fff={max_fff_override} loại hết frame",
            )
            return
        log.info(
            "Manual run: %s clamped to %d frames (max_fff=%d, last f%03d)",
            map_type, len(expected), max_fff_override, expected[-1],
        )
    progress_tracker.update(map_type, step="checking",
                            step_detail=f"Kiểm tra output hiện có cho {run_id}…")

    # Bunny mode: pointer already at run_id → all tiles uploaded, nothing to do.
    if cfg.BUNNY_ENABLED:
        try:
            from .bunny_storage import get_bunny_client as _get_bunny
            _bunny = _get_bunny()
            if _bunny is not None:
                _ptr = _bunny.read_pointer(map_type)
                if _ptr and _ptr.get("current_run") == run_id:
                    log.info("Bunny pointer already at %s/%s — skip", map_type, run_id)
                    progress_tracker.update(map_type, step="done",
                                            step_detail="Bunny CDN đã có data ✓")
                    return
        except Exception as _e:
            log.debug("Bunny pointer check failed (non-fatal): %s", _e)

    if _all_output_ready(map_type, run_id, expected, cfg):
        log.info("All output ready for %s/%s — skip", map_type, run_id)
        progress_tracker.update(map_type, step="done",
                                step_detail="Dữ liệu đầy đủ, bỏ qua ✓")
        return

    # 3. Discover available fff on NOAA
    progress_tracker.update(
        map_type, step="discovering",
        step_detail=f"Probing NOAA — {run_id} (max f{max_fff:03d})…",
    )
    discovery_failed = False
    try:
        discover_cycle(
            run_date=probe_date,
            run_hour=probe_hour,
            max_fff=max_fff,
            available_dir=cfg.AVAILABLE_DIR,
            rpm_limit=cfg.RPM_LIMIT,
        )
    except Exception as exc:
        discovery_failed = True
        log.warning("Discovery failed for %s: %s", map_type, exc)
        progress_tracker.update(
            map_type, step_detail=f"Discover thất bại: {exc}")

    # 4. Filter expected fff to those confirmed available on NOAA.
    #    Safety net: if the availability data has no overlap with 'expected' (e.g.
    #    only contains f048 while expected = [3..24]), download all expected frames
    #    rather than silently skipping everything.
    available = load_available_fff(
        cfg.AVAILABLE_DIR, map_type, probe_date, probe_hour)
    if available and any(f in available for f in expected):
        to_download = [f for f in expected if f in available]
    elif discovery_failed and not available:
        # Discovery threw AND no cached availability for this run — bail out
        # rather than hammer NOAA with a blind download that's likely to fail
        # on every frame. The next scheduler tick will retry discovery.
        raise RuntimeError(
            f"Discovery failed and no cached availability for {map_type} {run_id}; "
            f"refusing to blind-download {len(expected)} frames."
        )
    else:
        # Cached availability exists (or empty) but no overlap — download all expected
        to_download = list(expected)

    # 4b. Cold-zone optimisation: frames beyond cold_fff_min that are already
    #     covered by a recent previous cycle are hardlinked rather than
    #     re-downloaded and re-generated.
    cold_covered = _cold_fffs_covered(map_type, run_id, expected, cfg)
    if cold_covered:
        log.info(
            "Cold zone: reusing %d/%d fffs from previous cycle for %s/%s",
            len(cold_covered), len(expected), map_type, run_id,
        )
        progress_tracker.update(
            map_type,
            step_detail=f"Cold zone: {len(cold_covered)} frames dùng lại từ chu kỳ trước…",
        )
        to_download = [f for f in to_download if f not in cold_covered]

    # 5. Download
    log.info("Downloading %s/%s (%d frames)",
             map_type, run_id, len(to_download))
    progress_tracker.update(
        map_type,
        step="downloading",
        step_detail=f"Tải {len(to_download)} frames từ NOAA…",
        frames_total=len(to_download),
        frames_done=0,
    )
    _dl_start = time.perf_counter()
    metrics["download_started_at"] = datetime.now(timezone.utc).isoformat()

    # Check cancel before starting download
    if check_cancel_requested(map_type):
        raise JobCancelledError("Job cancelled by user before download.")

    download_map(
        map_type=map_type,
        run_date=probe_date,
        run_hour=probe_hour,
        data_dir=cfg.DATA_DIR,
        fff_values=to_download,
        rpm_limit=cfg.RPM_LIMIT,
        skip_existing=True,
    )
    _dl_elapsed = round(time.perf_counter() - _dl_start, 1)
    metrics["download_finished_at"] = datetime.now(timezone.utc).isoformat()
    metrics["download_seconds"] = _dl_elapsed

    # Check cancel again after download, before tile generation
    if check_cancel_requested(map_type):
        raise JobCancelledError("Job cancelled by user after download.")
    status = db_get_job_status(map_type)
    status["download_duration_s"] = _dl_elapsed
    db_update_job_status(map_type, status)
    
    progress_tracker.update(map_type, frames_done=len(to_download), download_duration_s=_dl_elapsed, step_detail=f"Tải xong {len(to_download)} files GRIB2")

    # 6. Generate output
    from ..services.tile_generator import _MAP_PRODUCTS
    products = _MAP_PRODUCTS.get(map_type, [])
    total_frames = len(to_download) * max(len(products), 1)
    progress_tracker.update(
        map_type,
        step="generating",
        step_detail=f"Tạo output — {len(to_download)} frames × {max(len(products), 1)} products…",
        frames_total=total_frames,
        frames_done=0,
        tiles_saved=0,
        tiles_skipped=0,
    )
    _gen_start = time.perf_counter()
    metrics["generate_started_at"] = datetime.now(timezone.utc).isoformat()
    # Measure peak STAGING checkpoint 1: BEFORE generate
    from .cycle_tracker import measure_staging_size
    staging_peak = measure_staging_size(map_type, run_id)
    staging_peak_at = datetime.now(timezone.utc).isoformat()

    _generate_output(map_type, run_id, to_download, cfg)
    _gen_elapsed = round(time.perf_counter() - _gen_start, 1)
    metrics["generate_finished_at"] = datetime.now(timezone.utc).isoformat()
    metrics["generate_seconds"] = _gen_elapsed
    status = db_get_job_status(map_type)
    status["tile_duration_s"] = _gen_elapsed
    db_update_job_status(map_type, status)

    progress_tracker.update(map_type, tile_duration_s=_gen_elapsed)

    # Measure peak STAGING checkpoint 2: AFTER generate (peak is here usually)
    p2 = measure_staging_size(map_type, run_id)
    if p2 > staging_peak:
        staging_peak = p2
        staging_peak_at = datetime.now(timezone.utc).isoformat()

    # 6b. Cold-zone "hardlink" handling
    #     - Bunny mode: cold frames copied on Bunny-side (GET+PUT) inside finalize_map_to_bunny
    #     - Legacy mode: local hardlink LIVE/{old}/{fff} → LIVE/{new}/{fff}
    if not cfg.BUNNY_ENABLED:
        if cold_covered:
            n_linked = _hardlink_cold_output(map_type, run_id, cold_covered, cfg)
            if n_linked > 0:
                log.info(
                    "Cold zone: hardlinked %d files from previous cycle for %s/%s (legacy local mode)",
                    n_linked, map_type, run_id,
                )

    # 6c. Finalize map on Bunny: copy cold frames Bunny-side + atomic pointer switch
    _bunny_finalize_ok = False
    if cfg.BUNNY_ENABLED:
        try:
            _bunny_finalize_ok = finalize_map_to_bunny(map_type, run_id, cold_covered, cfg, metrics_out=metrics)
        except Exception as exc:
            log.error(
                "finalize_map_to_bunny raised for %s/%s: %s",
                map_type, run_id, exc,
            )
            # Always surface as a job-level failure: if the pointer never
            # switched, FE keeps serving the previous run and "non-fatal"
            # is misleading. BUNNY_FAIL_FAST is preserved for callers that
            # rely on the old non-fatal semantics but is no longer the
            # only way to escalate.
            metrics["finalize_error"] = str(exc)
            raise
        if not _bunny_finalize_ok:
            # finalize_map_to_bunny returned False (pointer switch failed) —
            # treat as hard error so retry path is open.
            metrics["finalize_error"] = "pointer_switch_failed"
            raise RuntimeError(
                f"Bunny pointer switch failed for {map_type}/{run_id}"
            )

    # 7. Clean up ALL local data after successful Bunny finalize.
    #    Bunny is the canonical store — local GRIB2, tiles, grids, availability
    #    are no longer needed and would just waste disk space.
    #    Set BUNNY_KEEP_LOCAL_AFTER_FINALIZE=1 to retain them for debugging.
    if cfg.BUNNY_ENABLED and _bunny_finalize_ok:
        if getattr(cfg, "BUNNY_KEEP_LOCAL_AFTER_FINALIZE", False):
            log.info(
                "BUNNY_KEEP_LOCAL_AFTER_FINALIZE=1 — skipping local cleanup for %s/%s",
                map_type, run_id,
            )
        else:
            _cleanup_local_after_bunny_finalize(map_type, run_id, cfg)

    # 8. Reporting hook (per-map alert + cycle complete check + DB write)
    try:
        # Measure peak STAGING checkpoint 3: AFTER finalize (should be near 0)
        p3 = measure_staging_size(map_type, run_id)
        if p3 > staging_peak:
            staging_peak = p3
            staging_peak_at = datetime.now(timezone.utc).isoformat()

        # Pull push metrics from orchestrator
        try:
            from .pipeline_orchestrator import get_orchestrator
            orchestrator = get_orchestrator()
            push_m = orchestrator.get_push_metrics(map_type, run_id)
            if push_m:
                metrics["push_started_at"] = push_m.get("first_push_at")
                metrics["push_finished_at"] = push_m.get("last_push_at")
                metrics["push_seconds"] = push_m.get("accumulated_seconds")
                metrics["chunks_uploaded_ok"] = push_m.get("ok", 0)
                metrics["chunks_uploaded_failed"] = push_m.get("failed", 0)
                metrics["bytes_uploaded"] = push_m.get("bytes", 0)
                metrics["transient_errors"] = push_m.get("transient_errors", 0)
                metrics["permanent_errors"] = push_m.get("permanent_errors", 0)
                # Clear after consumed (next cycle starts fresh)
                orchestrator.reset_push_metrics(map_type, run_id)
        except Exception as e:
            log.debug("Could not pull push metrics from orchestrator: %s", e)

        # Frame counts
        metrics["frames_total"] = len(expected)
        metrics["frames_generated"] = len(to_download)
        metrics["frames_cold_copied"] = len(cold_covered)

        # Storage snapshots
        metrics["peak_local_staging_bytes"] = staging_peak
        metrics["peak_local_staging_at"] = staging_peak_at

        # Bunny storage size (via Account API if configured)
        try:
            from .bunny_analytics import get_bunny_analytics_client
            ba = get_bunny_analytics_client()
            if ba is not None:
                metrics["bunny_storage_after_bytes"] = ba.get_storage_used()
                metrics["bunny_storage_measured_at"] = datetime.now(timezone.utc).isoformat()
        except Exception as e:
            log.debug("Could not query Bunny storage size: %s", e)

        # Job-overall timestamps
        metrics["finished_at"] = datetime.now(timezone.utc).isoformat()
        metrics["total_wall_seconds"] = time.perf_counter() - job_t0

        # Send report + insert DB row
        from .cycle_tracker import record_map_done
        record_map_done(map_type, run_id, metrics)
    except Exception as exc:
        log.error("Reporting hook failed (non-fatal) %s/%s: %s", map_type, run_id, exc)

    log.info("Job done: %s/%s", map_type, run_id)


def finalize_map_to_bunny(
    map_type: str,
    run_id: str,
    cold_covered: set[int],
    cfg: Any,
    metrics_out: dict | None = None,
) -> bool:
    """Bunny-side cold frame transfer + atomic pointer switch + delete previous run.

    Called after `_generate_output` finished (which pushed hot frames per-frame
    from STAGING). Cold frames `cold_covered` were NOT regenerated this cycle —
    they need to be transferred from previous run to new run on Bunny side
    (no local LIVE involved).

    Steps:
      1. Read previous pointer from Bunny → identify prev_run.
      2. For each fff in cold_covered: copy Bunny prev_run/{fff} → run_id/{fff}
         using GET+PUT (no local file needed).
      3. PUT new pointer → atomic switch. Frontend now serves new run.
      4. DELETE previous Bunny run (if BUNNY_DELETE_PREV_AFTER_SWITCH).

    Args:
        metrics_out: optional dict to populate with timing/byte metrics for reporting.
                     Mutated in-place. Keys set:
                       cold_copy_started_at, cold_copy_finished_at, cold_copy_seconds
                       bytes_cold_get, bytes_cold_put
                       finalize_started_at, finalize_finished_at, finalize_seconds
                       pointer_switched_at, pointer_switch_ok

    Returns True on success (or noop), False if pointer switch failed.
    """
    from ..services.bunny_storage import get_bunny_client
    import time as _time
    bunny = get_bunny_client()
    if bunny is None:
        return True  # noop, Bunny disabled or misconfigured

    # 1. Read previous pointer to find source for cold-frame copies
    prev = bunny.read_pointer(map_type)
    prev_run = prev.get("current_run") if prev else None

    # Idempotent: skip everything if pointer already at this run (re-run protection)
    if prev_run == run_id:
        log.info(
            "Bunny pointer already at %s/%s, skipping finalize",
            map_type, run_id,
        )
        return True

    finalize_t0 = _time.perf_counter()
    finalize_start_iso = datetime.now(timezone.utc).isoformat()

    # 2. Bunny-side copy of cold frames (no local file needed)
    cold_t0 = None
    cold_start_iso = None
    cold_end_iso = None
    cold_bytes = 0
    if cold_covered and prev_run:
        labels = sorted(f"{fff:03d}" for fff in cold_covered)
        log.info(
            "Bunny cold-frame copy %s | %s → %s: %d frames",
            map_type, prev_run, run_id, len(labels),
        )
        cold_t0 = _time.perf_counter()
        cold_start_iso = datetime.now(timezone.utc).isoformat()
        try:
            stats = bunny.copy_run_subset(map_type, prev_run, run_id, labels)
            cold_bytes = stats.get("bytes", 0) or 0
            log.info(
                "Bunny cold copy %s/%s done: frames=%d files ok=%d failed=%d",
                map_type, run_id, stats["frames"], stats["ok"], stats["failed"],
            )
            if stats["failed"] > 0:
                # Any file-level failure means cold frames are incomplete on
                # the destination → refuse to publish a partial run.
                raise RuntimeError(
                    f"Bunny cold copy had {stats['failed']} failures "
                    f"for {map_type}/{run_id}"
                )
        except Exception as exc:
            log.error(
                "Bunny cold copy failed %s/%s: %s — aborting finalize",
                map_type, run_id, exc,
            )
            # Always abort: a pointer switch with missing cold frames means
            # the FE renders empty / 404 tiles. This is never acceptable.
            raise
        cold_end_iso = datetime.now(timezone.utc).isoformat()

        if metrics_out is not None:
            metrics_out["cold_copy_started_at"] = cold_start_iso
            metrics_out["cold_copy_finished_at"] = cold_end_iso
            metrics_out["cold_copy_seconds"] = _time.perf_counter() - cold_t0
            # GET+PUT: bytes count once (transit) — split equally for clarity
            metrics_out["bytes_cold_get"] = cold_bytes
            metrics_out["bytes_cold_put"] = cold_bytes
    elif cold_covered and not prev_run:
        log.warning(
            "Bunny finalize %s/%s: cold_covered=%d but no previous run → "
            "cold frames will be missing on Bunny",
            map_type, run_id, len(cold_covered),
        )

    # 3. Atomic switch (single PUT call)
    pointer_switched_at = datetime.now(timezone.utc).isoformat()
    pointer_switch_ok = bunny.write_pointer(
        map_type, current_run=run_id, previous_run=prev_run,
    )
    if metrics_out is not None:
        metrics_out["pointer_switch_ok"] = bool(pointer_switch_ok)
        metrics_out["pointer_switched_at"] = pointer_switched_at

    if not pointer_switch_ok:
        log.error(
            "Bunny pointer switch FAILED for %s/%s (previous=%s)",
            map_type, run_id, prev_run,
        )
        # Still record finalize timing for reporting
        if metrics_out is not None:
            metrics_out["finalize_started_at"] = finalize_start_iso
            metrics_out["finalize_finished_at"] = datetime.now(timezone.utc).isoformat()
            metrics_out["finalize_seconds"] = _time.perf_counter() - finalize_t0
        return False

    log.info(
        "Bunny atomic switch ✓ %s | %s → %s",
        map_type, prev_run or "(none)", run_id,
    )

    # 3b. Upload _timeline.json so FE can render timeline without hitting BE API
    try:
        from .timeline_builder import build_timeline_static
        # bunny_run_ready=True because we just pointed at this run
        timeline_doc = build_timeline_static(map_type, run_id, cfg, bunny_run_ready=True)
        if timeline_doc["frames"]:
            timeline_ok = bunny.write_timeline_metadata(map_type, timeline_doc)
            if metrics_out is not None:
                metrics_out["timeline_metadata_ok"] = bool(timeline_ok)
            if not timeline_ok:
                log.warning(
                    "Bunny _timeline.json upload failed (non-fatal) for %s/%s",
                    map_type, run_id,
                )
    except Exception as exc:
        log.warning(
            "Bunny _timeline.json build/upload error (non-fatal) for %s/%s: %s",
            map_type, run_id, exc,
        )

    # 4. Delete previous run from Bunny
    if cfg.BUNNY_DELETE_PREV_AFTER_SWITCH and prev_run and prev_run != run_id:
        if bunny.delete_run(map_type, prev_run):
            log.info("Bunny deleted previous run: %s/%s", map_type, prev_run)
        else:
            log.warning(
                "Bunny delete previous run failed (non-fatal): %s/%s",
                map_type, prev_run,
            )

    # Final timing for finalize stage
    if metrics_out is not None:
        metrics_out["finalize_started_at"] = finalize_start_iso
        metrics_out["finalize_finished_at"] = datetime.now(timezone.utc).isoformat()
        metrics_out["finalize_seconds"] = _time.perf_counter() - finalize_t0

    return True


def _all_output_ready(map_type: str, run_id: str, fffs: list[int], cfg: Any) -> bool:
    from ..services.availability_service import json_grid_ready, tiles_ready
    from ..services.tile_generator import _MAP_PRODUCTS

    is_rain_adv = (map_type == "rain_advanced")

    for fff in fffs:
        for product in _MAP_PRODUCTS.get(map_type, []):
            if not tiles_ready(map_type, run_id, fff, product, cfg.TILES_DIR):
                return False
        if is_rain_adv:
            if not json_grid_ready(map_type, run_id, fff, "rain_advanced", cfg.JSON_GRIDS_DIR):
                return False
    return True


def _generate_output(map_type: str, run_id: str, fffs: list[int], cfg: Any) -> None:
    from ..services.tile_generator import generate_run

    is_rain_adv = (map_type == "rain_advanced")

    if False:  # JSON_ONLY_MAP_TYPES is currently empty — placeholder for future use
        pass
    else:
        # PNG tiles (and optionally JSON grids for rain_advanced)
        generate_run(
            map_type=map_type,
            run_id=run_id,
            fff_values=fffs,
            data_dir=cfg.DATA_DIR,
        )
        if is_rain_adv:
            # Generate 15-min interpolated frames sliding "now" (006_15 to 014_45)
            try:
                from ..core.precip_pipeline import generate_precip_interp_frames
                interp_result = generate_precip_interp_frames(
                    run_id=run_id,
                    data_dir=cfg.DATA_DIR,
                    output_dir=cfg.STAGING_DIR / map_type / run_id,
                )
                if interp_result.get("frames_generated", 0) > 0:
                    log.info(
                        "rain_advanced interp: %d frames in %.1fs",
                        interp_result["frames_generated"],
                        interp_result.get("duration_s", 0),
                    )
                    # Per-sub-frame Bunny push: walk each interp dir and push it.
                    # Sub-frames live at STAGING/rain_advanced/{run}/{label}/precip_base/...
                    # where label = 'NNN_MM' (e.g. '006_15', '014_45').
                    if cfg.BUNNY_ENABLED:
                        try:
                            from .pipeline_tasks import push_frame_to_bunny
                            staging_run = cfg.STAGING_DIR / map_type / run_id
                            # Identify interp sub-frame dirs (name has underscore)
                            interp_dirs = [
                                d for d in staging_run.iterdir()
                                if d.is_dir() and "_" in d.name
                            ]
                            for d in sorted(interp_dirs):
                                ok = push_frame_to_bunny(
                                    map_type=map_type,
                                    run_id=run_id,
                                    fff=0,            # ignored when fff_label given
                                    fff_label=d.name, # '006_15', '014_45', ...
                                )
                                if not ok:
                                    log.error(
                                        "Bunny push failed for interp sub-frame %s/%s/%s",
                                        map_type, run_id, d.name,
                                    )
                        except Exception as exc:
                            log.error(
                                "Bunny interp push exception (non-fatal): %s", exc,
                            )
                            if cfg.BUNNY_FAIL_FAST:
                                raise
            except Exception as exc:
                log.warning("rain_advanced interp frames failed (non-fatal): %s", exc)

            from ..services.grid_service import generate_grid
            for fff in fffs:
                try:
                    generate_grid(
                        map_type="rain_advanced",
                        run_id=run_id,
                        fff=fff,
                        product="rain_advanced",
                        data_dir=cfg.DATA_DIR,
                        grids_dir=cfg.JSON_GRIDS_DIR,
                    )
                except Exception as exc:
                    log.warning("Rain grid failed %s/f%03d: %s",
                                run_id, fff, exc)


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

# GFS cycles arrive 4×/day. Each cycle is fully uploaded to NOAA ~5h after
# its label hour, so we trigger ~5h after each cycle: 05/11/17/23 UTC.
_CYCLE_FIRE_HOURS = "5,11,17,23"

# Stagger map-job start times across the cycle window so all maps don't pile
# into the same minute. With 5 maps × 3-min step we span 5:05 → 5:17 (well
# inside the available 6-hour window before the next cycle).
_STAGGER_STEP_MIN = 3
_STAGGER_FIRST_MIN = 5


# ---------------------------------------------------------------------------
# Persistent on/off toggle
# ---------------------------------------------------------------------------

def is_scheduler_enabled() -> bool:
    """Source of truth: DB setting `scheduler_enabled` (falls back to env once)."""
    val = db_get_setting(_SETTING_KEY_ENABLED, None)
    if val is None:
        env_val = bool(get_settings().SCHEDULER_ENABLED)
        db_set_setting(_SETTING_KEY_ENABLED, env_val)
        return env_val
    return bool(val)


def _set_scheduler_enabled(enabled: bool) -> None:
    db_set_setting(_SETTING_KEY_ENABLED, bool(enabled))


# ---------------------------------------------------------------------------
# Retention safety-net — enforces KEEP_CYCLES by deleting runs older than the
# newest N cycles. Catches the rare case where per-run auto-cleanup failed
# (e.g. Windows file lock) or BUNNY_KEEP_LOCAL_AFTER_FINALIZE was left on.
# Never deletes a run that is current_run / previous_run on Bunny.
# ---------------------------------------------------------------------------

def _retention_check() -> None:
    """Delete local + Bunny data for runs older than the newest KEEP_CYCLES.

    Safety guards (in order):
      1. Skip map_types currently locked for deletion (in-flight delete job).
      2. Skip map_types currently `running` (don't yank disk from active job).
      3. Skip run_ids equal to Bunny `current_run` or `previous_run` (live).
      4. Only delete what's beyond the newest `cfg.KEEP_CYCLES` runs.
    """
    cfg = get_settings()
    keep = max(1, int(getattr(cfg, "KEEP_CYCLES", 4)))

    # Snapshot Bunny pointers once (current + previous = always-protect set).
    bunny_protected: dict[str, set[str]] = {}
    if cfg.BUNNY_ENABLED:
        try:
            from .bunny_storage import get_bunny_client
            bunny = get_bunny_client()
            if bunny is not None:
                for mt in MAP_SPECS:
                    try:
                        ptr = bunny.read_pointer(mt)
                        protect = set()
                        if ptr:
                            cur = ptr.get("current_run")
                            prev = ptr.get("previous_run")
                            if cur:
                                protect.add(cur)
                            if prev:
                                protect.add(prev)
                        bunny_protected[mt] = protect
                    except Exception as exc:
                        # If we can't read pointer, protect everything for this
                        # map_type — better keep junk than risk deleting live data.
                        log.warning(
                            "Retention: cannot read Bunny pointer for %s (%s) — "
                            "skipping retention for this map this round",
                            mt, exc,
                        )
                        bunny_protected[mt] = None  # type: ignore[assignment]
        except Exception as exc:
            log.warning("Retention: Bunny client unavailable (%s) — proceeding without pointer guard", exc)

    from .delete_service import _discover_run_ids, is_map_locked, enqueue_delete

    to_delete_by_map: dict[str, list[str]] = {}

    for map_type in MAP_SPECS:
        if is_map_locked(map_type):
            log.debug("Retention: skip %s (locked for deletion)", map_type)
            continue
        try:
            status = db_get_job_status(map_type) or {}
            if status.get("status") == "running":
                log.debug("Retention: skip %s (job running)", map_type)
                continue
        except Exception:
            pass

        protect = bunny_protected.get(map_type)
        # Sentinel: pointer-unreadable → treat as "skip this map this round"
        if protect is None and cfg.BUNNY_ENABLED:
            continue
        protect = protect or set()

        all_runs = _discover_run_ids(map_type, cfg)  # newest-first
        if len(all_runs) <= keep:
            continue

        # Everything beyond the newest `keep` runs is a deletion candidate,
        # minus anything currently pointed-to on Bunny.
        for rid in all_runs[keep:]:
            if rid in protect:
                log.debug("Retention: protect %s/%s (Bunny pointer)", map_type, rid)
                continue
            to_delete_by_map.setdefault(map_type, []).append(rid)

    if not to_delete_by_map:
        log.debug("Retention: nothing to delete (KEEP_CYCLES=%d)", keep)
        return

    total_runs = sum(len(v) for v in to_delete_by_map.values())
    log.info(
        "Retention: deleting %d run(s) across %d map(s) (KEEP_CYCLES=%d)",
        total_runs, len(to_delete_by_map), keep,
    )

    # enqueue_delete throttles concurrency internally (MAX_CONCURRENT_MAP_DELETES).
    # We submit one job per map_type so each gets its own run_ids list.
    for map_type, rids in to_delete_by_map.items():
        try:
            job_id = enqueue_delete([map_type], rids)
            log.info(
                "Retention: enqueued delete job %s for %s (%d runs: %s)",
                job_id, map_type, len(rids), ", ".join(rids[:3]) + (" …" if len(rids) > 3 else ""),
            )
        except Exception as exc:
            log.warning("Retention: enqueue_delete failed for %s: %s", map_type, exc)


# ---------------------------------------------------------------------------
# Catchup safety-net job — runs every 2h, fires any map whose Bunny pointer
# has fallen behind the latest accessible NOAA cycle.
# ---------------------------------------------------------------------------

def _catchup_check() -> None:
    """Probe the latest accessible GFS cycle once, then fire any out-of-date map."""
    cfg = get_settings()

    # Single probe shared by all maps — replaces the 5× duplicate probes
    # that the per-map _run_map_job would do on each fire.
    try:
        max_fff_all = max(_spec_max_fff(mt) for mt in MAP_SPECS)
    except ValueError:
        return
    probe_date, probe_hour = find_latest_accessible_cycle(max_fff_all)
    if probe_date is None or probe_hour is None:
        log.debug("Catchup: NOAA unreachable, skipping")
        return
    run_id = run_id_from_date(probe_date, probe_hour)

    bunny = None
    if cfg.BUNNY_ENABLED:
        try:
            from .bunny_storage import get_bunny_client
            bunny = get_bunny_client()
        except Exception as exc:
            log.debug("Catchup: Bunny client unavailable: %s", exc)

    fired = 0
    for map_type in MAP_SPECS:
        try:
            status = db_get_job_status(map_type)
            if status.get("status") == "running":
                continue

            # Skip if Bunny pointer already at run_id (cheap GET cached at CDN edge)
            if bunny is not None:
                try:
                    ptr = bunny.read_pointer(map_type)
                    if ptr and ptr.get("current_run") == run_id:
                        continue
                except Exception as exc:
                    # Cannot confirm pointer state — skip this map rather than
                    # blindly re-firing it. Firing on Bunny API failure would
                    # redundantly reprocess all maps every 2h during an outage.
                    log.debug("Catchup: cannot read Bunny pointer for %s (%s) — skipping", map_type, exc)
                    continue

            log.info("Catchup: firing %s (target run %s)", map_type, run_id)
            trigger_job(map_type)
            fired += 1
        except Exception as exc:
            log.warning("Catchup probe failed for %s: %s", map_type, exc)

    if fired:
        log.info("Catchup: triggered %d/%d maps", fired, len(MAP_SPECS))


# ---------------------------------------------------------------------------
# Scheduler lifecycle (idempotent; safe to call multiple times)
# ---------------------------------------------------------------------------

def _build_scheduler() -> BackgroundScheduler:
    """Construct (but do not start) a BackgroundScheduler wired with all jobs."""
    cfg = get_settings()

    sched = BackgroundScheduler(
        timezone="UTC",
        daemon=True,
        executors={
            # max_workers ≥ concurrency cap + 2 small overhead jobs
            'default': ApsThreadPoolExecutor(max_workers=_CONCURRENCY_LIMIT + 2),
        },
        job_defaults={
            'coalesce': True,         # collapse missed fires into one
            'misfire_grace_time': 1800,
            'max_instances': 1,       # one instance per job_id at any time
        },
    )

    # Per-map jobs: cron-aligned to GFS availability (T+5h), staggered by 3 min.
    # Note: cron handler calls `trigger_job` (which submits to the FIFO pool)
    # instead of running the work directly, so cron + manual fires share the
    # same _CONCURRENCY_LIMIT and the same submit-order guarantee.
    map_types = list(MAP_SPECS.keys())
    for i, map_type in enumerate(map_types):
        stagger_min = _STAGGER_FIRST_MIN + i * _STAGGER_STEP_MIN
        sched.add_job(
            func=trigger_job,
            args=[map_type],
            trigger=CronTrigger(
                hour=_CYCLE_FIRE_HOURS,
                minute=stagger_min,
                timezone='UTC',
            ),
            id=f"job_{map_type}",
            name=f"Map job: {map_type}",
            replace_existing=True,
        )

        status = db_get_job_status(map_type)
        if not status:
            db_update_job_status(map_type, {
                "status": "idle",
                "last_started": None,
                "last_success": None,
            })

    # Catchup every 2h on the half-hour (off-cycle so it doesn't clash with map fires)
    sched.add_job(
        func=_catchup_check,
        trigger=CronTrigger(hour='*/2', minute=30, timezone='UTC'),
        id="catchup_check",
        name="Catchup coverage check",
        replace_existing=True,
        misfire_grace_time=600,
    )

    # Retention enforcement every 6h at :45 (offset from cron fires at xx:05-17
    # and catchup at xx:30). Deletes runs beyond KEEP_CYCLES if not pointed-to
    # by Bunny. Safe-by-default: skips locked/running maps, skips on pointer
    # read failure.
    sched.add_job(
        func=_retention_check,
        trigger=CronTrigger(hour='2,8,14,20', minute=45, timezone='UTC'),
        id="retention_check",
        name="Retention enforcement (KEEP_CYCLES)",
        replace_existing=True,
        misfire_grace_time=900,
    )

    # Daily aggregation report
    if cfg.TELEGRAM_ENABLED:
        try:
            from .cycle_tracker import daily_aggregation_job
            sched.add_job(
                func=daily_aggregation_job,
                trigger=CronTrigger(
                    hour=cfg.DAILY_REPORT_UTC_HOUR,
                    minute=cfg.DAILY_REPORT_UTC_MINUTE,
                    timezone="UTC",
                ),
                id="daily_report",
                name="Daily aggregated report (Telegram)",
                replace_existing=True,
                misfire_grace_time=600,
            )
        except Exception as e:
            log.error("Failed to schedule daily_aggregation_job: %s", e)

    # Bunny analytics hourly polling
    if cfg.BUNNY_ENABLED and cfg.BUNNY_ACCOUNT_API_KEY:
        try:
            from .bunny_analytics import hourly_poll_job
            sched.add_job(
                func=hourly_poll_job,
                trigger=IntervalTrigger(minutes=cfg.BUNNY_ANALYTICS_POLL_MIN),
                id="bunny_analytics_poll",
                name="Bunny Statistics API polling",
                replace_existing=True,
                misfire_grace_time=300,
            )
        except Exception as e:
            log.error("Failed to schedule bunny_analytics hourly_poll_job: %s", e)

    return sched


def reset_zombie_jobs() -> int:
    """Reset any `status=running` job in DB to `idle` — these are leftovers from
    a previous server process that died without updating DB.

    Returns the number of map_types reset. Called once at lifespan startup so
    that a fresh server can re-fire jobs without hitting the stale-lock guard
    in `_job_for_map_type`.
    """
    from ..services import progress_tracker as _pt
    now_iso = datetime.now(tz=timezone.utc).isoformat()
    n = 0
    for map_type, status in db_get_all_job_status().items():
        if status.get("status") == "running":
            status["status"] = "idle"
            # Preserve any prior `last_error` so diagnostics survive the reset.
            # Only set a synthetic note if no real error was already captured.
            if not status.get("last_error"):
                status["last_error"] = (
                    "Reset on server restart (was 'running' from previous session)"
                )
            status["last_zombie_reset_at"] = now_iso
            status.pop("cancel_requested", None)
            db_update_job_status(map_type, status)
            _pt.update(
                map_type, step="idle",
                step_detail="Reset on server restart",
            )
            n += 1
    if n:
        log.info("Zombie reset: %d map(s) flipped 'running' → 'idle' on startup", n)
    return n


def start_scheduler() -> None:
    """Start the scheduler if (a) not already running and (b) persistently enabled.

    Called once at FastAPI lifespan startup. Also called by `enable_scheduler()`
    when the admin flips the toggle on.

    Note: zombie-job cleanup is NOT done here — it would clobber genuinely
    running jobs if called via the admin enable/disable cycle. The lifespan
    handler calls `reset_zombie_jobs()` once at process startup instead.
    """
    global _scheduler
    db_init()

    with _scheduler_lock:
        if _scheduler is not None and _scheduler.running:
            log.info("Scheduler already running — no-op")
            return

        if not is_scheduler_enabled():
            log.info(
                "Scheduler disabled (persistent state) — not starting. "
                "Toggle via POST /api/v1/admin/scheduler/enable.",
            )
            return

        _scheduler = _build_scheduler()
        _scheduler.start()
        log.info(
            "Scheduler started: %d map jobs cron-aligned %s UTC, stagger=%dmin, "
            "concurrency_cap=%d, catchup=*/2h:30",
            len(MAP_SPECS), _CYCLE_FIRE_HOURS, _STAGGER_STEP_MIN, _CONCURRENCY_LIMIT,
        )


def stop_scheduler() -> None:
    """Shut down the scheduler. Idempotent."""
    global _scheduler
    with _scheduler_lock:
        if _scheduler is not None and _scheduler.running:
            _scheduler.shutdown(wait=False)
            log.info("Scheduler stopped")
        _scheduler = None


def enable_scheduler() -> dict:
    """Persist enabled=true + start scheduler. Returns current info dict."""
    _set_scheduler_enabled(True)
    start_scheduler()
    return get_scheduler_info()


def disable_scheduler() -> dict:
    """Persist enabled=false + stop scheduler. Returns current info dict.

    In-flight map jobs continue to completion (we don't kill threads), but no
    new cron fires will occur.
    """
    _set_scheduler_enabled(False)
    stop_scheduler()
    return get_scheduler_info()


def get_scheduler_info() -> dict:
    """Snapshot of scheduler state for UI / monitoring."""
    enabled = is_scheduler_enabled()
    running = _scheduler is not None and _scheduler.running
    jobs_info: list[dict] = []
    if running and _scheduler is not None:
        for j in _scheduler.get_jobs():
            jobs_info.append({
                "id": j.id,
                "name": j.name,
                "next_run": j.next_run_time.isoformat() if j.next_run_time else None,
                "trigger": str(j.trigger),
            })
    # in-flight slot occupancy via ThreadPoolExecutor internals (CPython detail).
    try:
        # _work_queue holds queued (not-yet-running) tasks; _threads holds workers.
        # An "active" slot ≈ worker thread that is currently running a task. We
        # approximate as: total workers - idle workers. The executor doesn't
        # expose "idle" directly, so just report queue size + workers count.
        active_slots = min(
            _CONCURRENCY_LIMIT,
            len(getattr(_job_executor, "_threads", [])),
        )
        queued_jobs = _job_executor._work_queue.qsize()  # type: ignore[attr-defined]
    except Exception:
        active_slots = None
        queued_jobs = None

    return {
        "enabled": enabled,
        "running": running,
        "concurrency_limit": _CONCURRENCY_LIMIT,
        "active_slots": active_slots,
        "queued_jobs": queued_jobs,
        "cycle_fire_hours_utc": _CYCLE_FIRE_HOURS,
        "stagger_minutes": _STAGGER_STEP_MIN,
        "jobs": jobs_info,
    }


def trigger_job(
    map_type: str,
    max_fff_override: int | None = None,
    run_id_override: str | None = None,
) -> None:
    """Submit a map job to the shared FIFO executor (admin endpoint + cron helper).

    Calls go to `_job_executor` which preserves submit order: the first
    `_CONCURRENCY_LIMIT` submissions start immediately, the rest queue
    and run in submit order as workers free. This guarantees that
    `trigger-job-all` honours MAP_SPECS order — fixes the bug where
    wind_surface occasionally started before temperature_feels_like.
    """
    # Reset UI state immediately so the admin log doesn't keep displaying
    # the previous cycle's error / step_detail for maps that are still
    # waiting their concurrency slot. Without this, queued maps appear to
    # be "errored on <old run_id>" until they actually start, even though
    # the new submit is for a different cycle.
    #
    # IMPORTANT: do NOT set status="running" here. `_job_for_map_type`
    # treats status=="running" as a stale-lock signal and skips the job
    # if it sees one when actually starting — so setting it eagerly would
    # cause every just-submitted job to short-circuit and never run.
    # Setting `status="idle"` clears any prior error badge and lets the
    # job worker flip idle → running when it actually picks the task.
    try:
        from ..services import progress_tracker as _pt
        _pt.reset(map_type)
        detail = "Đang xếp hàng…"
        if run_id_override:
            detail = f"Xếp hàng (run_id={run_id_override})"
        _pt.update(
            map_type,
            step="queued",
            step_detail=detail,
            run_id=run_id_override,
            frames_total=0,
            frames_done=0,
            tiles_saved=0,
            tiles_skipped=0,
        )
        status = db_get_job_status(map_type)
        status["status"] = "idle"  # clear stale error/running so UI is sane
        status.pop("last_error", None)
        status.pop("cancel_requested", None)
        db_update_job_status(map_type, status)
    except Exception as exc:
        log.warning("trigger_job pre-reset failed for %s: %s", map_type, exc)

    _job_executor.submit(
        _job_for_map_type, map_type, max_fff_override, run_id_override,
    )


def get_all_job_status() -> dict[str, dict]:
    return db_get_all_job_status()


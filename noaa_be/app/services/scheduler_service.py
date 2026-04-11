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
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

from ..config import get_settings
from ..core.discovery import discover_cycle, latest_available_run, load_available_fff
from ..core.downloader import download_map, run_id_from_date
from ..core.map_specs import MAP_SPECS, segment_fff
from ..services.availability_service import (
    all_run_ids,
    prune_old_cloud_runs,
    run_id_to_datetime,
)

log = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None
JOB_STATUS: dict[str, dict[str, Any]] = {}


# ---------------------------------------------------------------------------
# FFF helpers
# ---------------------------------------------------------------------------

def _compute_needed_fffs(map_type: str, cfg: Any) -> list[int]:
    """
    Return the fff list we need to download/generate for this map type.

    Logic:
    - Archive maps (cloud_total, cloud_layered): just f000 per cycle snapshot.
    - All other maps: take the full spec segment list, keep only values
      <= cfg.NEEDED_FORECAST_H (= cycle_interval + noaa_upload + proc_buffer + user_window).
      This guarantees 24h forecast coverage for users at all times between two
      consecutive cycle updates.
    """
    if map_type in cfg.ARCHIVE_MAP_TYPES:
        return [0]
    all_fff = segment_fff(MAP_SPECS[map_type].fff_segments_full)
    return [f for f in all_fff if f <= cfg.NEEDED_FORECAST_H]


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
# Individual map-type job
# ---------------------------------------------------------------------------

def _job_for_map_type(map_type: str) -> None:
    from ..services import progress_tracker
    now = datetime.now(tz=timezone.utc)
    JOB_STATUS.setdefault(map_type, {})["last_started"] = now.isoformat()
    JOB_STATUS[map_type]["status"] = "running"
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
    )
    try:
        _run_map_job(map_type)
        JOB_STATUS[map_type]["status"] = "ok"
        JOB_STATUS[map_type]["last_success"] = datetime.now(tz=timezone.utc).isoformat()
        JOB_STATUS[map_type].pop("last_error", None)
        progress_tracker.update(map_type, step="done", step_detail="Hoàn thành ✓")
    except Exception as exc:
        log.exception("Job failed for map_type=%s: %s", map_type, exc)
        JOB_STATUS[map_type]["status"] = "error"
        JOB_STATUS[map_type]["last_error"] = str(exc)
        progress_tracker.update(map_type, step="error", step_detail=str(exc)[:120])


def _run_map_job(map_type: str) -> None:
    from datetime import date as _date
    from ..services import progress_tracker

    cfg = get_settings()

    # 1. Determine which GFS cycle to probe
    probe_date, probe_hour = latest_available_run(cfg.AVAILABLE_DIR)
    if probe_date is None:
        utc_now = datetime.now(tz=timezone.utc)
        probe_date = utc_now.date()
        probe_hour = (utc_now.hour // 6) * 6

    run_id = run_id_from_date(probe_date, probe_hour)
    progress_tracker.update(map_type, run_id=run_id)

    # 2. Check if we already have all expected tiles/grids
    expected = _compute_needed_fffs(map_type, cfg)
    progress_tracker.update(map_type, step="checking", step_detail=f"Kiểm tra output hiện có cho {run_id}…")
    if _all_output_ready(map_type, run_id, expected, cfg):
        log.info("All output ready for %s/%s — skip", map_type, run_id)
        progress_tracker.update(map_type, step="done", step_detail="Dữ liệu đầy đủ, bỏ qua ✓")
        return

    # 3. Discover available fff on NOAA
    max_fff = _spec_max_fff(map_type)
    progress_tracker.update(
        map_type, step="discovering",
        step_detail=f"Probing NOAA — {run_id} (max f{max_fff:03d})…",
    )
    try:
        discover_cycle(
            run_date=probe_date,
            run_hour=probe_hour,
            max_fff=max_fff,
            available_dir=cfg.AVAILABLE_DIR,
            rpm_limit=cfg.RPM_LIMIT,
        )
    except Exception as exc:
        log.warning("Discovery failed for %s: %s — will try downloading anyway", map_type, exc)
        progress_tracker.update(map_type, step_detail=f"Discover thất bại: {exc} — thử tải anyway")

    # 4. Filter expected fff to those confirmed available on NOAA
    available = load_available_fff(cfg.AVAILABLE_DIR, map_type, probe_date, probe_hour)
    to_download = [f for f in expected if (not available or f in available)]

    # 5. Download
    log.info("Downloading %s/%s (%d frames)", map_type, run_id, len(to_download))
    progress_tracker.update(
        map_type,
        step="downloading",
        step_detail=f"Tải {len(to_download)} frames từ NOAA…",
        frames_total=len(to_download),
        frames_done=0,
    )
    download_map(
        map_type=map_type,
        run_date=probe_date,
        run_hour=probe_hour,
        data_dir=cfg.DATA_DIR,
        fff_values=to_download,
        rpm_limit=cfg.RPM_LIMIT,
        skip_existing=True,
    )
    progress_tracker.update(map_type, frames_done=len(to_download), step_detail=f"Tải xong {len(to_download)} files GRIB2")

    # 6. Generate output
    from ..services.tile_generator import _MAP_PRODUCTS
    products = _MAP_PRODUCTS.get(map_type, [])
    total_frames = len(to_download) * max(len(products), 1)
    progress_tracker.update(
        map_type,
        step="generating",
        step_detail=f"Tạo output — {len(to_download)} frames × {max(len(products),1)} products…",
        frames_total=total_frames,
        frames_done=0,
        tiles_saved=0,
        tiles_skipped=0,
    )
    _generate_output(map_type, run_id, to_download, cfg)

    # 7. Prune cloud circular buffer
    if map_type in cfg.ARCHIVE_MAP_TYPES:
        pruned = prune_old_cloud_runs(map_type, cfg.AVAILABLE_DIR, cfg.TILES_DIR)
        if pruned:
            log.info("Pruned old runs for %s: %s", map_type, pruned)

    log.info("Job done: %s/%s", map_type, run_id)


def _all_output_ready(map_type: str, run_id: str, fffs: list[int], cfg: Any) -> bool:
    from ..services.availability_service import json_grid_ready, tiles_ready
    from ..services.tile_generator import _MAP_PRODUCTS

    is_json_only = map_type in cfg.JSON_ONLY_MAP_TYPES
    is_rain_adv = (map_type == "rain_advanced")

    for fff in fffs:
        if is_json_only:
            if not json_grid_ready(map_type, run_id, fff, "wind_30m", cfg.JSON_GRIDS_DIR):
                return False
        else:
            for product in _MAP_PRODUCTS.get(map_type, []):
                if not tiles_ready(map_type, run_id, fff, product, cfg.TILES_DIR):
                    return False
            if is_rain_adv:
                if not json_grid_ready(map_type, run_id, fff, "rain_advanced", cfg.JSON_GRIDS_DIR):
                    return False
    return True


def _generate_output(map_type: str, run_id: str, fffs: list[int], cfg: Any) -> None:
    from ..services.tile_generator import generate_run
    from ..services.grid_service import generate_grid, _WIND_PRODUCTS

    is_json_only = map_type in cfg.JSON_ONLY_MAP_TYPES
    is_rain_adv = (map_type == "rain_advanced")

    if is_json_only:
        # Wind: generate JSON grid for each product × fff
        for product_name in _WIND_PRODUCTS:
            for fff in fffs:
                try:
                    generate_grid(
                        map_type=map_type,
                        run_id=run_id,
                        fff=fff,
                        product=product_name,
                        data_dir=cfg.DATA_DIR,
                        grids_dir=cfg.JSON_GRIDS_DIR,
                    )
                except Exception as exc:
                    log.warning("Grid gen failed %s/%s/f%03d/%s: %s", map_type, run_id, fff, product_name, exc)
    else:
        # PNG tiles (and optionally JSON grids for rain_advanced)
        generate_run(
            map_type=map_type,
            run_id=run_id,
            fff_values=fffs,
            data_dir=cfg.DATA_DIR,
        )
        if is_rain_adv:
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
                    log.warning("Rain grid failed %s/f%03d: %s", run_id, fff, exc)


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def start_scheduler() -> None:
    global _scheduler
    cfg = get_settings()
    if not cfg.SCHEDULER_ENABLED:
        log.info("Scheduler disabled via SCHEDULER_ENABLED=false")
        return

    _scheduler = BackgroundScheduler(timezone="UTC", daemon=True)
    interval_min = cfg.SCHEDULER_INTERVAL_MINUTES

    for map_type in MAP_SPECS:
        _scheduler.add_job(
            func=_job_for_map_type,
            args=[map_type],
            trigger=IntervalTrigger(minutes=interval_min),
            id=f"job_{map_type}",
            name=f"Map job: {map_type}",
            replace_existing=True,
            misfire_grace_time=300,
        )
        JOB_STATUS[map_type] = {
            "status": "idle",
            "last_started": None,
            "last_success": None,
        }

    _scheduler.start()
    log.info("Scheduler started: %d jobs, interval=%d min", len(MAP_SPECS), interval_min)


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        log.info("Scheduler stopped")


def trigger_job(map_type: str) -> None:
    """Fire a job immediately in a daemon thread (for admin endpoint)."""
    t = threading.Thread(target=_job_for_map_type, args=[map_type], daemon=True)
    t.start()


def get_all_job_status() -> dict[str, dict]:
    return dict(JOB_STATUS)

"""cycle_tracker.py — cycle-level completion detection + daily aggregation.

Workflow:
  1. After each _run_map_job(map) completes → record_map_done(map, run_id, metrics)
     - INSERT/UPDATE cycle_metrics row (idempotent via UNIQUE(map_type, run_id))
     - Send per-map Telegram alert (if TELEGRAM_VERBOSITY >= 2)
     - Check if cycle complete (all ACTIVE_MAPS done) → trigger per-cycle alert
  2. Daily 00:30 UTC cron → daily_aggregation_job()
     - Aggregate yesterday's cycle_metrics + bunny_analytics_hourly
     - Send daily Telegram report

Active maps: read from MAP_SPECS.
"""
from __future__ import annotations

import logging
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from ..config import get_settings
from ..core.map_specs import MAP_SPECS

log = logging.getLogger(__name__)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cycle_hour_from_run_id(run_id: str) -> Optional[int]:
    """Extract cycle hour (0/6/12/18) from run_id like '20260510_06z'."""
    try:
        h_part = run_id.split("_")[1].rstrip("z")
        return int(h_part)
    except (IndexError, ValueError):
        return None


def get_active_maps() -> set[str]:
    """Return the set of maps currently being scheduled."""
    return set(MAP_SPECS.keys())


def measure_staging_size(map_type: str, run_id: str) -> int:
    """Measure current STAGING/{map}/{run_id}/ size in bytes.

    Returns 0 if dir doesn't exist (e.g. already cleaned up by Bunny mode).
    """
    cfg = get_settings()
    p = cfg.STAGING_DIR / map_type / run_id
    if not p.exists():
        return 0
    try:
        return sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
    except OSError:
        return 0


def get_disk_free_gb() -> float:
    """Return free disk space on the DB volume, in GB."""
    cfg = get_settings()
    try:
        usage = shutil.disk_usage(str(cfg.DB_DIR))
        return usage.free / (1024 ** 3)
    except Exception:
        return -1


# ── Main entry: record map done + check cycle complete ────────────────

def record_map_done(map_type: str, run_id: str, metrics: dict) -> None:
    """Record map completion in DB; send Telegram alerts as appropriate.

    metrics dict should contain stage timestamps + counts + bytes (see db.py schema).
    All exceptions are caught — reporting NEVER blocks pipeline.
    """
    cfg = get_settings()

    # Inject derived fields
    metrics.setdefault("cycle_hour", _cycle_hour_from_run_id(run_id))

    # Compute total_wall_seconds from started_at + finished_at if not provided
    if metrics.get("total_wall_seconds") is None:
        sa, fa = metrics.get("started_at"), metrics.get("finished_at")
        if sa and fa:
            try:
                t0 = datetime.fromisoformat(sa.replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(fa.replace("Z", "+00:00"))
                metrics["total_wall_seconds"] = (t1 - t0).total_seconds()
            except (ValueError, AttributeError):
                pass

    # 1. Insert cycle_metrics row (idempotent)
    try:
        from ..core.db import upsert_cycle_metric
        upsert_cycle_metric(map_type, run_id, dict(metrics))
    except Exception as e:
        log.error("cycle_tracker: upsert_cycle_metric failed: %s", e)
        return  # bail — can't send report without DB row

    # 2. Per-map Telegram alert
    try:
        from .telegram_reporter import send_per_map
        # Re-fetch row with normalized values (boolean True/False as 1/0 in SQLite, etc.)
        from ..core.db import get_cycle_metrics_by_run
        rows = get_cycle_metrics_by_run(run_id)
        this_row = next((r for r in rows if r.get("map_type") == map_type), None)
        if this_row:
            ok = send_per_map(this_row)
            if ok and cfg.TELEGRAM_VERBOSITY >= 2:
                from ..core.db import mark_map_alert_sent
                mark_map_alert_sent(map_type, run_id, _utc_now_iso())
    except Exception as e:
        log.error("cycle_tracker: per-map alert send failed: %s", e)

    # 3. Critical alerts (pointer fail, permanent errors)
    try:
        if metrics.get("pointer_switch_ok") is False:
            from .telegram_reporter import send_critical
            send_critical(
                "Pointer switch FAILED",
                f"Map: `{map_type}`\nRun: `{run_id}`\n"
                f"Frontend may be stuck on previous run.",
            )
        if (metrics.get("permanent_errors") or 0) > 0:
            from .telegram_reporter import send_critical
            send_critical(
                "Permanent push errors",
                f"Map: `{map_type}`\nRun: `{run_id}`\n"
                f"Errors: {metrics['permanent_errors']} chunks failed permanently",
            )
    except Exception as e:
        log.error("cycle_tracker: critical alert send failed: %s", e)

    # 4. Check disk space alert
    try:
        free_gb = get_disk_free_gb()
        if 0 < free_gb < 5:
            from .telegram_reporter import send_critical
            send_critical(
                "Low disk space",
                f"Free: {free_gb:.1f} GB on DB volume\n"
                f"Pipeline may fail if disk fills.",
            )
    except Exception as e:
        log.error("cycle_tracker: disk check failed: %s", e)

    # 5. Check if cycle is complete → per-cycle alert
    try:
        if _is_cycle_complete(run_id):
            _trigger_cycle_complete_report(run_id)
    except Exception as e:
        log.error("cycle_tracker: cycle complete check failed: %s", e)


def _is_cycle_complete(run_id: str) -> bool:
    """Return True if all ACTIVE_MAPS have finished_at set for this run_id."""
    from ..core.db import get_cycle_metrics_by_run
    rows = get_cycle_metrics_by_run(run_id)
    done_maps = {
        r["map_type"]
        for r in rows
        if r.get("finished_at") is not None
    }
    return done_maps >= get_active_maps()


def _trigger_cycle_complete_report(run_id: str) -> None:
    """Build aggregated cycle report and send Telegram. Idempotent."""
    from ..core.db import (
        get_cycle_metrics_by_run,
        is_cycle_alert_already_sent,
        mark_cycle_alert_sent,
    )

    if is_cycle_alert_already_sent(run_id):
        log.debug("cycle_tracker: cycle alert already sent for %s", run_id)
        return

    rows = get_cycle_metrics_by_run(run_id)
    try:
        from .telegram_reporter import send_per_cycle
        ok = send_per_cycle(rows)
        if ok:
            mark_cycle_alert_sent(run_id, _utc_now_iso())
            log.info("cycle_tracker: per-cycle alert sent for %s", run_id)
    except Exception as e:
        log.error("cycle_tracker: per-cycle alert failed: %s", e)


# ── Daily aggregation ────────────────────────────────────────────────

def daily_aggregation_job() -> None:
    """APScheduler cron job at DAILY_REPORT_UTC_HOUR:MINUTE.

    Aggregate yesterday's metrics → send Telegram daily report.
    """
    cfg = get_settings()
    now = datetime.now(timezone.utc)
    # "Yesterday" = the 24h ending at midnight UTC of today
    today_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if now.hour < cfg.DAILY_REPORT_UTC_HOUR or (
        now.hour == cfg.DAILY_REPORT_UTC_HOUR and now.minute < cfg.DAILY_REPORT_UTC_MINUTE
    ):
        # Edge case: cron fires before midnight — use today's start
        date_to = today_midnight
    else:
        date_to = today_midnight  # same — yesterday is fixed
    date_from = date_to - timedelta(days=1)

    log.info(
        "Daily aggregation: %s → %s",
        date_from.isoformat(), date_to.isoformat(),
    )

    try:
        daily = _build_daily_report(date_from, date_to)
        from .telegram_reporter import send_daily
        send_daily(daily)
    except Exception as e:
        log.error("daily_aggregation_job failed: %s", e, exc_info=True)


def _build_daily_report(date_from: datetime, date_to: datetime) -> dict:
    """Aggregate cycle_metrics + bunny_analytics_hourly into report dict."""
    from ..core.db import get_cycle_metrics_between
    rows = get_cycle_metrics_between(date_from.isoformat(), date_to.isoformat())

    # Group cycles by run_id
    by_run: dict[str, list[dict]] = {}
    for r in rows:
        by_run.setdefault(r["run_id"], []).append(r)

    cycles = []
    pipe_frames_gen = 0
    pipe_frames_cold = 0
    pipe_push_bytes = 0
    pipe_cold_get = 0
    pipe_cold_put = 0
    pipe_peak_staging = 0
    pipe_bunny_storage_latest = 0

    for run_id, run_rows in sorted(by_run.items()):
        # Cycle wall: max(finished_at) - min(started_at)
        starts = [r["started_at"] for r in run_rows if r.get("started_at")]
        ends = [r["finished_at"] for r in run_rows if r.get("finished_at")]
        wall_s = None
        cycle_started = None
        cycle_finished = None
        if starts and ends:
            cycle_started = min(starts)
            cycle_finished = max(ends)
            try:
                t0 = datetime.fromisoformat(cycle_started.replace("Z", "+00:00"))
                t1 = datetime.fromisoformat(cycle_finished.replace("Z", "+00:00"))
                wall_s = (t1 - t0).total_seconds()
            except (ValueError, AttributeError):
                pass

        cycle_hour = run_rows[0].get("cycle_hour", "?")
        cycles.append({
            "run_id": run_id,
            "cycle_label": f"{cycle_hour:02d}z" if isinstance(cycle_hour, int) else "?",
            "started_at": cycle_started,
            "finished_at": cycle_finished,
            "wall_seconds": wall_s,
        })

        for r in run_rows:
            pipe_frames_gen += r.get("frames_generated") or 0
            pipe_frames_cold += r.get("frames_cold_copied") or 0
            pipe_push_bytes += r.get("bytes_uploaded") or 0
            pipe_cold_get += r.get("bytes_cold_get") or 0
            pipe_cold_put += r.get("bytes_cold_put") or 0
            pipe_peak_staging = max(pipe_peak_staging, r.get("peak_local_staging_bytes") or 0)
            bs = r.get("bunny_storage_after_bytes") or 0
            if bs > pipe_bunny_storage_latest:
                pipe_bunny_storage_latest = bs

    pipeline_summary = {
        "frames_generated": pipe_frames_gen,
        "frames_cold_copied": pipe_frames_cold,
        "frames_total": pipe_frames_gen + pipe_frames_cold,
        "push_bytes": pipe_push_bytes,
        "cold_get_bytes": pipe_cold_get,
        "cold_put_bytes": pipe_cold_put,
        "peak_staging": pipe_peak_staging,
        "bunny_storage": pipe_bunny_storage_latest,
        # bunny_storage_delta computed by comparing to day-before-yesterday
    }

    # Try to compute Bunny storage delta vs day before
    try:
        prev_rows = get_cycle_metrics_between(
            (date_from - timedelta(days=1)).isoformat(),
            date_from.isoformat(),
        )
        prev_max_bunny = max(
            (r.get("bunny_storage_after_bytes") or 0) for r in prev_rows
        ) if prev_rows else 0
        if prev_max_bunny > 0:
            pipeline_summary["bunny_storage_delta"] = pipe_bunny_storage_latest - prev_max_bunny
    except Exception:
        pass

    # Bunny user analytics
    bunny_summary = {}
    try:
        from .bunny_analytics import daily_summarize
        bunny_summary = daily_summarize(date_from, date_to)
    except Exception as e:
        log.warning("daily_summarize Bunny analytics failed: %s", e)

    # Tuning signals
    tuning = {}
    if cycles:
        slowest = max(cycles, key=lambda c: c.get("wall_seconds") or 0)
        tuning["slowest_cycle_label"] = slowest["cycle_label"]
    tuning["disk_free_gb"] = get_disk_free_gb()

    return {
        "date_iso": date_from.strftime("%Y-%m-%d"),
        "cycles": cycles,
        "pipeline": pipeline_summary,
        "bunny": bunny_summary,
        "tuning": tuning,
    }

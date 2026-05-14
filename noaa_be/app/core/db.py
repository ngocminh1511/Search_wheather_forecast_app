import sqlite3
import json
import logging
from typing import Any
from ..config import get_settings

log = logging.getLogger(__name__)

def get_db_connection() -> sqlite3.Connection:
    cfg = get_settings()
    # connect to the shared sqlite DB
    conn = sqlite3.connect(cfg.SHARED_DB_PATH, timeout=30.0)
    # WAL mode for better concurrency between Pipeline writer and API reader.
    # busy_timeout (ms) widens the window before SQLITE_BUSY is raised so that
    # scheduler + worker writes don't fight each other under load.
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

def init_db() -> None:
    """Initialize tables if they don't exist."""
    with get_db_connection() as conn:
        conn.execute('''
            CREATE TABLE IF NOT EXISTS job_status (
                map_type TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS progress (
                map_type TEXT PRIMARY KEY,
                data TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_jobs (
                id TEXT PRIMARY KEY,
                map_type TEXT NOT NULL,
                run_id TEXT NOT NULL,
                fff INTEGER NOT NULL,
                product TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at REAL NOT NULL,
                error TEXT
            )
        ''')

        # ── Cycle metrics (per-map-per-cycle aggregate) ───────────────────
        # Inserted at end of _run_map_job by cycle_tracker.record_map_done().
        # Stage timestamps stored as ISO 8601 UTC strings.
        conn.execute('''
            CREATE TABLE IF NOT EXISTS cycle_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                map_type TEXT NOT NULL,
                run_id TEXT NOT NULL,
                cycle_hour INTEGER,

                -- Overall map timestamps
                started_at TEXT,
                finished_at TEXT,
                total_wall_seconds REAL,

                -- Per-stage timestamps (ISO 8601 UTC) + duration (seconds)
                download_started_at TEXT,
                download_finished_at TEXT,
                download_seconds REAL,

                generate_started_at TEXT,
                generate_finished_at TEXT,
                generate_seconds REAL,

                push_started_at TEXT,
                push_finished_at TEXT,
                push_seconds REAL,

                cold_copy_started_at TEXT,
                cold_copy_finished_at TEXT,
                cold_copy_seconds REAL,

                finalize_started_at TEXT,
                finalize_finished_at TEXT,
                finalize_seconds REAL,

                -- Frame counts
                frames_total INTEGER,
                frames_generated INTEGER,
                frames_cold_copied INTEGER,

                -- Chunk + bytes
                chunks_uploaded_ok INTEGER,
                chunks_uploaded_failed INTEGER,
                bytes_uploaded INTEGER,
                bytes_cold_get INTEGER,
                bytes_cold_put INTEGER,

                -- Storage snapshots
                peak_local_staging_bytes INTEGER,
                peak_local_staging_at TEXT,
                bunny_storage_after_bytes INTEGER,
                bunny_storage_measured_at TEXT,

                -- Quality
                pointer_switch_ok INTEGER,
                pointer_switched_at TEXT,
                transient_errors INTEGER,
                permanent_errors INTEGER,

                -- Reporting state
                map_alert_sent_at TEXT,
                cycle_alert_sent_at TEXT,

                UNIQUE(map_type, run_id)
            )
        ''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cycle_metrics_run ON cycle_metrics(run_id)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_cycle_metrics_finished ON cycle_metrics(finished_at)')

        # ── Bunny Statistics API hourly snapshots ─────────────────────────
        conn.execute('''
            CREATE TABLE IF NOT EXISTS bunny_analytics_hourly (
                timestamp TEXT PRIMARY KEY,
                pull_requests INTEGER,
                bandwidth_bytes INTEGER,
                cache_hit_ratio REAL,
                error_4xx INTEGER,
                error_5xx INTEGER,
                top_countries_json TEXT,
                raw_json TEXT
            )
        ''')

        # ── System settings (runtime-toggleable knobs persisted across restarts) ──
        conn.execute('''
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        ''')

        conn.commit()


# --- System settings ---

def get_setting(key: str, default: Any = None) -> Any:
    """Read a system_settings value (JSON-decoded). Returns default if missing."""
    with get_db_connection() as conn:
        cursor = conn.execute('SELECT value FROM system_settings WHERE key = ?', (key,))
        row = cursor.fetchone()
        if not row:
            return default
        try:
            return json.loads(row[0])
        except (ValueError, TypeError):
            return row[0]


def set_setting(key: str, value: Any) -> None:
    """Persist a system_settings value (JSON-encoded)."""
    val_str = json.dumps(value)
    with get_db_connection() as conn:
        conn.execute('''
            INSERT INTO system_settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        ''', (key, val_str))
        conn.commit()


# --- Cycle metrics ---

def upsert_cycle_metric(map_type: str, run_id: str, metrics: dict[str, Any]) -> None:
    """Insert or replace a cycle_metrics row.

    Idempotent: re-running same (map_type, run_id) overrides the row.
    Caller passes a dict matching the schema columns; missing keys → NULL.
    """
    columns = [
        "map_type", "run_id", "cycle_hour",
        "started_at", "finished_at", "total_wall_seconds",
        "download_started_at", "download_finished_at", "download_seconds",
        "generate_started_at", "generate_finished_at", "generate_seconds",
        "push_started_at", "push_finished_at", "push_seconds",
        "cold_copy_started_at", "cold_copy_finished_at", "cold_copy_seconds",
        "finalize_started_at", "finalize_finished_at", "finalize_seconds",
        "frames_total", "frames_generated", "frames_cold_copied",
        "chunks_uploaded_ok", "chunks_uploaded_failed",
        "bytes_uploaded", "bytes_cold_get", "bytes_cold_put",
        "peak_local_staging_bytes", "peak_local_staging_at",
        "bunny_storage_after_bytes", "bunny_storage_measured_at",
        "pointer_switch_ok", "pointer_switched_at",
        "transient_errors", "permanent_errors",
        "map_alert_sent_at", "cycle_alert_sent_at",
    ]
    metrics["map_type"] = map_type
    metrics["run_id"] = run_id
    values = [metrics.get(c) for c in columns]
    placeholders = ",".join("?" * len(columns))
    cols_sql = ",".join(columns)
    update_cols = ",".join(f"{c}=excluded.{c}" for c in columns
                            if c not in ("map_type", "run_id"))
    sql = f'''
        INSERT INTO cycle_metrics ({cols_sql}) VALUES ({placeholders})
        ON CONFLICT(map_type, run_id) DO UPDATE SET {update_cols}
    '''
    with get_db_connection() as conn:
        conn.execute(sql, values)
        conn.commit()


def get_cycle_metrics_by_run(run_id: str) -> list[dict[str, Any]]:
    """Return all cycle_metrics rows for a given run_id, sorted by map_type."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            'SELECT * FROM cycle_metrics WHERE run_id = ? ORDER BY map_type',
            (run_id,),
        )
        return [dict(row) for row in cursor.fetchall()]


def get_cycle_metrics_between(date_from_iso: str, date_to_iso: str) -> list[dict[str, Any]]:
    """Return cycle_metrics rows with finished_at in [date_from, date_to)."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            '''SELECT * FROM cycle_metrics
               WHERE finished_at >= ? AND finished_at < ?
               ORDER BY finished_at''',
            (date_from_iso, date_to_iso),
        )
        return [dict(row) for row in cursor.fetchall()]


def mark_map_alert_sent(map_type: str, run_id: str, ts_iso: str) -> None:
    with get_db_connection() as conn:
        conn.execute(
            'UPDATE cycle_metrics SET map_alert_sent_at = ? WHERE map_type = ? AND run_id = ?',
            (ts_iso, map_type, run_id),
        )
        conn.commit()


def mark_cycle_alert_sent(run_id: str, ts_iso: str) -> None:
    """Mark the cycle alert as sent on ALL maps of this run_id."""
    with get_db_connection() as conn:
        conn.execute(
            'UPDATE cycle_metrics SET cycle_alert_sent_at = ? WHERE run_id = ?',
            (ts_iso, run_id),
        )
        conn.commit()


def is_cycle_alert_already_sent(run_id: str) -> bool:
    """Check if any row for this run_id has cycle_alert_sent_at set."""
    with get_db_connection() as conn:
        cursor = conn.execute(
            '''SELECT 1 FROM cycle_metrics
               WHERE run_id = ? AND cycle_alert_sent_at IS NOT NULL LIMIT 1''',
            (run_id,),
        )
        return cursor.fetchone() is not None


# --- Bunny analytics hourly ---

def insert_bunny_analytics_hourly(
    timestamp_iso: str,
    pull_requests: int,
    bandwidth_bytes: int,
    cache_hit_ratio: float,
    error_4xx: int,
    error_5xx: int,
    top_countries_json: str,
    raw_json: str,
) -> None:
    with get_db_connection() as conn:
        conn.execute(
            '''INSERT OR REPLACE INTO bunny_analytics_hourly
               (timestamp, pull_requests, bandwidth_bytes, cache_hit_ratio,
                error_4xx, error_5xx, top_countries_json, raw_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
            (timestamp_iso, pull_requests, bandwidth_bytes, cache_hit_ratio,
             error_4xx, error_5xx, top_countries_json, raw_json),
        )
        conn.commit()


def get_bunny_analytics_between(date_from_iso: str, date_to_iso: str) -> list[dict[str, Any]]:
    """Return bunny_analytics_hourly rows in [date_from, date_to)."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            '''SELECT * FROM bunny_analytics_hourly
               WHERE timestamp >= ? AND timestamp < ?
               ORDER BY timestamp''',
            (date_from_iso, date_to_iso),
        )
        return [dict(row) for row in cursor.fetchall()]

# --- Job Status ---

def update_job_status(map_type: str, status_dict: dict[str, Any]) -> None:
    data_str = json.dumps(status_dict)
    with get_db_connection() as conn:
        conn.execute('''
            INSERT INTO job_status (map_type, data)
            VALUES (?, ?)
            ON CONFLICT(map_type) DO UPDATE SET data = excluded.data
        ''', (map_type, data_str))
        conn.commit()

def get_job_status(map_type: str) -> dict[str, Any]:
    with get_db_connection() as conn:
        cursor = conn.execute('SELECT data FROM job_status WHERE map_type = ?', (map_type,))
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
        return {}

def get_all_job_status() -> dict[str, dict[str, Any]]:
    with get_db_connection() as conn:
        cursor = conn.execute('SELECT map_type, data FROM job_status')
        return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}

def set_cancel_requested(map_type: str, cancel: bool = True) -> None:
    status = get_job_status(map_type)
    status["cancel_requested"] = cancel
    update_job_status(map_type, status)

def reset_cancel_requested(map_type: str) -> None:
    """Clear cancel flag — call at the START of every new job so stale flags don't kill the next run."""
    status = get_job_status(map_type)
    if status.get("cancel_requested"):
        status["cancel_requested"] = False
        update_job_status(map_type, status)

def check_cancel_requested(map_type: str) -> bool:
    status = get_job_status(map_type)
    return status.get("cancel_requested", False)

class JobCancelledError(Exception):
    """Exception raised when a job is cancelled by the user."""
    pass

# --- Progress ---

def update_progress(map_type: str, progress_dict: dict[str, Any]) -> None:
    data_str = json.dumps(progress_dict)
    with get_db_connection() as conn:
        conn.execute('''
            INSERT INTO progress (map_type, data)
            VALUES (?, ?)
            ON CONFLICT(map_type) DO UPDATE SET data = excluded.data
        ''', (map_type, data_str))
        conn.commit()

def get_progress(map_type: str) -> dict[str, Any]:
    with get_db_connection() as conn:
        cursor = conn.execute('SELECT data FROM progress WHERE map_type = ?', (map_type,))
        row = cursor.fetchone()
        if row:
            return json.loads(row[0])
        return {}

def get_all_progress() -> dict[str, dict[str, Any]]:
    with get_db_connection() as conn:
        cursor = conn.execute('SELECT map_type, data FROM progress')
        return {row[0]: json.loads(row[1]) for row in cursor.fetchall()}

def clear_progress(map_type: str) -> None:
    with get_db_connection() as conn:
        conn.execute('DELETE FROM progress WHERE map_type = ?', (map_type,))
        conn.commit()

# --- Pipeline Jobs ---

def upsert_pipeline_job(job_id: str, map_type: str, run_id: str, fff: int, product: str, state: str, error: str = None) -> None:
    import time
    with get_db_connection() as conn:
        conn.execute('''
            INSERT INTO pipeline_jobs (id, map_type, run_id, fff, product, state, updated_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET 
                state = excluded.state,
                updated_at = excluded.updated_at,
                error = excluded.error
        ''', (job_id, map_type, run_id, fff, product, state, time.time(), error))
        conn.commit()

def get_pipeline_jobs_by_run(map_type: str, run_id: str) -> list[dict]:
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute('SELECT * FROM pipeline_jobs WHERE map_type = ? AND run_id = ?', (map_type, run_id))
        return [dict(row) for row in cursor.fetchall()]

def clear_pipeline_jobs(map_type: str, run_id: str) -> None:
    with get_db_connection() as conn:
        conn.execute('DELETE FROM pipeline_jobs WHERE map_type = ? AND run_id = ?', (map_type, run_id))
        conn.commit()

import sqlite3
import json
import logging
from typing import Any
from ..config import get_settings

log = logging.getLogger(__name__)

def get_db_connection() -> sqlite3.Connection:
    cfg = get_settings()
    # connect to the shared sqlite DB
    conn = sqlite3.connect(cfg.SHARED_DB_PATH, timeout=10.0)
    # WAL mode for better concurrency between Pipeline writer and API reader
    conn.execute("PRAGMA journal_mode=WAL")
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
        conn.commit()

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

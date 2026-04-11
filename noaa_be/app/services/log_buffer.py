from __future__ import annotations

"""
log_buffer.py — in-memory circular log buffer exposed to the admin UI.

A custom logging.Handler captures all log records from the root logger
(installed in main.py at startup) and keeps the last MAX_LINES lines.
The admin endpoint calls get_lines() to return recent logs for polling.
"""

import logging
import threading
from collections import deque
from typing import Any

MAX_LINES = 500

_lock = threading.Lock()
_buffer: deque[dict[str, Any]] = deque(maxlen=MAX_LINES)
_seq = 0  # monotonic counter so the frontend can detect new lines


def get_lines(since_seq: int = 0, limit: int = 200) -> list[dict[str, Any]]:
    """Return log entries with seq > since_seq (most recent first), up to limit."""
    with _lock:
        result = [e for e in _buffer if e["seq"] > since_seq]
    # Most recent first
    result.sort(key=lambda e: e["seq"], reverse=True)
    return result[:limit]


def get_max_seq() -> int:
    with _lock:
        return _seq


class _BufferHandler(logging.Handler):
    """Logging handler that appends records to the in-memory deque."""

    # Map Python log levels → frontend severity strings
    _LEVEL_MAP = {
        "DEBUG":    "info",
        "INFO":     "info",
        "WARNING":  "warn",
        "ERROR":    "err",
        "CRITICAL": "err",
    }

    def emit(self, record: logging.LogRecord) -> None:
        global _seq
        try:
            msg = self.format(record)
            level = self._LEVEL_MAP.get(record.levelname, "info")
            with _lock:
                _seq += 1
                _buffer.append({
                    "seq":   _seq,
                    "ts":    record.created,
                    "level": level,
                    "name":  record.name,
                    "msg":   msg,
                })
        except Exception:
            pass  # never crash the application from a log handler


# Single handler instance — installed once by install_handler()
_handler = _BufferHandler()
_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(name)s — %(message)s"))
_handler.setLevel(logging.DEBUG)

_installed = False


def install_handler() -> None:
    """Install the buffer handler on the root logger (idempotent)."""
    global _installed
    if _installed:
        return
    logging.getLogger().addHandler(_handler)
    _installed = True

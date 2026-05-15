"""log_files.py — persistent rotating log files for offline review.

Attaches two handlers to the root logger:
  - app.log         (text, identical to stdout format)
  - events.jsonl    (one JSON object per line — easy to grep / parse)

Both rotate daily at 00:00 UTC and keep `retention_days` backups. Idempotent:
calling install_file_handlers() twice is a no-op.
"""

from __future__ import annotations

import json
import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_TEXT_FORMAT = "%(asctime)s %(levelname)-8s %(name)s — %(message)s"

_installed = False
_text_handler: TimedRotatingFileHandler | None = None
_json_handler: TimedRotatingFileHandler | None = None


class _JsonLineFormatter(logging.Formatter):
    """Format each LogRecord as a single JSON object on one line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts":     time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
                      + f".{int(record.msecs):03d}Z",
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
        }
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def install_file_handlers(log_dir: Path, retention_days: int = 7, level: int = logging.INFO) -> None:
    """Attach the rotating text + json handlers to the root logger.

    Safe to call multiple times — only installs once.
    """
    global _installed, _text_handler, _json_handler
    if _installed:
        return

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    text_path = log_dir / "app.log"
    json_path = log_dir / "events.jsonl"

    # Rotate daily at midnight UTC, keep N backup files.
    text_handler = TimedRotatingFileHandler(
        filename=str(text_path),
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
        utc=True,
        delay=True,
    )
    text_handler.setLevel(level)
    text_handler.setFormatter(logging.Formatter(_TEXT_FORMAT))

    json_handler = TimedRotatingFileHandler(
        filename=str(json_path),
        when="midnight",
        interval=1,
        backupCount=retention_days,
        encoding="utf-8",
        utc=True,
        delay=True,
    )
    json_handler.setLevel(level)
    json_handler.setFormatter(_JsonLineFormatter())

    root = logging.getLogger()
    root.addHandler(text_handler)
    root.addHandler(json_handler)

    _text_handler = text_handler
    _json_handler = json_handler
    _installed = True

    logging.getLogger(__name__).info(
        "File log handlers installed: dir=%s retention_days=%d (app.log + events.jsonl)",
        log_dir, retention_days,
    )


def get_log_dir() -> Path | None:
    """Return the directory currently being written to, or None if not installed."""
    if _text_handler is None:
        return None
    return Path(_text_handler.baseFilename).parent


def list_log_files() -> list[Path]:
    """Return all existing log files (current + rotated backups) in date-sorted order."""
    d = get_log_dir()
    if d is None or not d.exists():
        return []
    files = []
    for name in ("app.log", "events.jsonl"):
        for p in sorted(d.glob(f"{name}*")):
            if p.is_file():
                files.append(p)
    return files


def list_available_dates() -> list[str]:
    """Return sorted YYYY-MM-DD strings for every day with log data on disk.

    Today's date is included only if the active (non-rotated) file exists.
    """
    d = get_log_dir()
    if d is None or not d.exists():
        return []

    dates: set[str] = set()
    today = time.strftime("%Y-%m-%d", time.gmtime())
    for prefix in ("app.log", "events.jsonl"):
        for p in d.glob(f"{prefix}*"):
            if not p.is_file():
                continue
            if p.name == prefix:
                # Active file = today
                dates.add(today)
            else:
                # Rotated: 'app.log.YYYY-MM-DD' → suffix is the date
                suffix = p.name[len(prefix) + 1:]
                if len(suffix) == 10 and suffix[4] == "-" and suffix[7] == "-":
                    dates.add(suffix)
    return sorted(dates)


def collect_log_text_for_date(prefix: str, date_str: str, max_bytes: int) -> bytes:
    """Return contents of the log file for a specific UTC date 'YYYY-MM-DD'.

    Today's logs live in the active file (no suffix); older days have suffix.
    Returns b'' if no file exists for that date.
    """
    d = get_log_dir()
    if d is None or not d.exists():
        return b""

    today = time.strftime("%Y-%m-%d", time.gmtime())
    if date_str == today:
        path = d / prefix
    else:
        path = d / f"{prefix}.{date_str}"

    if not path.exists():
        return b""

    try:
        data = path.read_bytes()
    except (OSError, PermissionError):
        return b""

    if len(data) > max_bytes:
        return data[-max_bytes:]
    return data


def collect_log_text(prefix: str, max_bytes: int) -> bytes:
    """Concatenate all rotated files for a given prefix ('app.log' or 'events.jsonl'),
    oldest → newest. Cap total output at `max_bytes` (tail-truncate to keep most recent).
    """
    d = get_log_dir()
    if d is None or not d.exists():
        return b""

    candidates = sorted(d.glob(f"{prefix}*"))
    # TimedRotatingFileHandler names rotated files with .YYYY-MM-DD suffix; the
    # base file (no suffix) is the newest. Read all, then concat oldest-first.
    rotated = [p for p in candidates if p.name != prefix]
    base = d / prefix
    ordered = sorted(rotated) + ([base] if base.exists() else [])

    chunks: list[bytes] = []
    total = 0
    for p in ordered:
        try:
            data = p.read_bytes()
        except (OSError, PermissionError):
            continue
        chunks.append(data)
        total += len(data)

    if total <= max_bytes:
        return b"".join(chunks)

    # Tail-truncate: keep the most recent max_bytes worth.
    combined = b"".join(chunks)
    return combined[-max_bytes:]

from __future__ import annotations

"""
progress_tracker.py — lightweight in-process progress registry.

Each map-type job writes structured progress here so the admin API
can expose it in real-time without a database or message queue.

Thread-safe: a single lock guards all reads and writes.
"""

import threading
from typing import Any

_lock = threading.Lock()

# Map-type → progress dict.  Never deleted between jobs, just reset+updated.
PROGRESS: dict[str, dict[str, Any]] = {}


def update(map_type: str, **kwargs: Any) -> None:
    """Merge kwargs into the progress dict for map_type."""
    with _lock:
        PROGRESS.setdefault(map_type, {}).update(kwargs)


def get(map_type: str) -> dict[str, Any]:
    """Return a shallow copy of the progress dict for map_type."""
    with _lock:
        return dict(PROGRESS.get(map_type, {}))


def get_all() -> dict[str, dict[str, Any]]:
    """Return shallow copies of all map-type progress dicts."""
    with _lock:
        return {k: dict(v) for k, v in PROGRESS.items()}


def reset(map_type: str) -> None:
    """Clear progress for map_type (call at job start)."""
    with _lock:
        PROGRESS[map_type] = {}

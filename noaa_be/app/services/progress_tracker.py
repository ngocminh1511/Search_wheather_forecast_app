from __future__ import annotations

"""
progress_tracker.py — lightweight progress registry.

Each map-type job writes structured progress to the shared SQLite DB
so the API can expose it in real-time.
"""

from typing import Any
from ..core.db import update_progress as db_update_progress
from ..core.db import get_progress as db_get_progress
from ..core.db import get_all_progress as db_get_all_progress
from ..core.db import clear_progress as db_clear_progress


def update(map_type: str, **kwargs: Any) -> None:
    """Merge kwargs into the progress dict for map_type."""
    current = db_get_progress(map_type)
    current.update(kwargs)
    db_update_progress(map_type, current)


def get(map_type: str) -> dict[str, Any]:
    """Return the progress dict for map_type."""
    return db_get_progress(map_type)


def get_all() -> dict[str, dict[str, Any]]:
    """Return all map-type progress dicts."""
    return db_get_all_progress()


def reset(map_type: str) -> None:
    """Clear progress for map_type (call at job start)."""
    db_clear_progress(map_type)
    db_update_progress(map_type, {})

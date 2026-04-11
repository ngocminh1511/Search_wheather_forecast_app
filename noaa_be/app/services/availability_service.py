from __future__ import annotations

"""
availability_service.py — read/write availability JSON files and derive run IDs.

Run ID format: "20260406_00z"  (yyyymmdd_HHz)
FFF (forecast hour offset): integer, e.g. 0, 3, 6, ..., 48.

On disk layout (under AVAILABLE_DIR):
  availability_{run_id}.json        — master file
  {map_type}/availability_{run_id}_{map_type}.json  — per-map split

A "run" is available if the f000 .idx file exists on NOAA remote.
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

from ..config import get_settings


# ---------------------------------------------------------------------------
# Run ID helpers
# ---------------------------------------------------------------------------

def run_id_from_parts(run_date_str: str, run_hour: int) -> str:
    """'20260406', 0 → '20260406_00z'"""
    return f"{run_date_str}_{run_hour:02d}z"


def parse_run_id(run_id: str) -> tuple[str, int]:
    """'20260406_00z' → ('20260406', 0)"""
    m = re.match(r"^(\d{8})_(\d{2})z$", run_id)
    if not m:
        raise ValueError(f"Invalid run_id format: {run_id!r}")
    return m.group(1), int(m.group(2))


def run_id_to_datetime(run_id: str) -> datetime:
    date_str, hour = parse_run_id(run_id)
    return datetime(
        year=int(date_str[:4]),
        month=int(date_str[4:6]),
        day=int(date_str[6:8]),
        hour=hour,
        tzinfo=timezone.utc,
    )


def all_run_ids(available_dir: Path | None = None) -> list[str]:
    """Return all run IDs found in the availability directory, newest first."""
    cfg = get_settings()
    d = available_dir or cfg.AVAILABLE_DIR
    pattern = re.compile(r"^availability_(\d{8}_\d{2}z)\.json$")
    ids = []
    for f in d.glob("availability_*.json"):
        m = pattern.match(f.name)
        if m:
            ids.append(m.group(1))
    ids.sort(reverse=True)
    return ids


def latest_run_id(available_dir: Path | None = None) -> str | None:
    ids = all_run_ids(available_dir)
    return ids[0] if ids else None


# ---------------------------------------------------------------------------
# Availability JSON helpers
# ---------------------------------------------------------------------------

def _avail_master_path(run_id: str, available_dir: Path) -> Path:
    return available_dir / f"availability_{run_id}.json"


def _avail_map_path(run_id: str, map_type: str, available_dir: Path) -> Path:
    return available_dir / map_type / f"availability_{run_id}_{map_type}.json"


def load_master_availability(run_id: str, available_dir: Path | None = None) -> dict:
    cfg = get_settings()
    d = available_dir or cfg.AVAILABLE_DIR
    path = _avail_master_path(run_id, d)
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def load_map_availability(run_id: str, map_type: str, available_dir: Path | None = None) -> list[int]:
    """Return sorted list of fff integers available for this run + map_type."""
    cfg = get_settings()
    d = available_dir or cfg.AVAILABLE_DIR
    path = _avail_map_path(run_id, map_type, d)

    if path.exists():
        raw = json.loads(path.read_text())
        # Per-map file: {"map_info": {"union_fff": [...], ...}, ...}
        if isinstance(raw, dict):
            map_info = raw.get("map_info", raw)
            union = map_info.get("union_fff")
            if isinstance(union, list):
                return sorted(int(v) for v in union)
            # Fallback: try top-level union_fff
            union = raw.get("union_fff")
            if isinstance(union, list):
                return sorted(int(v) for v in union)
        return []

    # Fallback: scan master availability file
    master = load_master_availability(run_id, d)
    # Master structure: {"map_types": {"temperature_feels_like": {"union_fff": [...], ...}, ...}}
    map_types = master.get("map_types", {})
    map_data = map_types.get(map_type, {})
    union = map_data.get("union_fff")
    if isinstance(union, list):
        return sorted(int(v) for v in union)
    return []


def save_master_availability(run_id: str, payload: dict, available_dir: Path | None = None) -> None:
    cfg = get_settings()
    d = available_dir or cfg.AVAILABLE_DIR
    d.mkdir(parents=True, exist_ok=True)
    path = _avail_master_path(run_id, d)
    path.write_text(json.dumps(payload, indent=2))


# ---------------------------------------------------------------------------
# Tile readiness checks
# ---------------------------------------------------------------------------

def tiles_ready(map_type: str, run_id: str, fff: int, product: str, tiles_dir: Path | None = None) -> bool:
    """Check if at least zoom z=0 tile exists for this frame."""
    cfg = get_settings()
    d = tiles_dir or cfg.TILES_DIR
    tile_0_0_0 = d / map_type / run_id / f"{fff:03d}" / product / "0" / "0" / "0.png"
    return tile_0_0_0.exists()


def json_grid_ready(map_type: str, run_id: str, fff: int, product: str, grids_dir: Path | None = None) -> bool:
    cfg = get_settings()
    d = grids_dir or cfg.JSON_GRIDS_DIR
    path = d / map_type / run_id / f"{fff:03d}" / f"{product}.json"
    return path.exists()


# ---------------------------------------------------------------------------
# Cloud circular buffer helpers
# ---------------------------------------------------------------------------

def cloud_run_ids_for_24h(available_dir: Path | None = None) -> list[str]:
    """
    For cloud maps (archive direction), return up to KEEP_CYCLES latest run IDs.
    These together cover the past 24h window.
    """
    cfg = get_settings()
    ids = all_run_ids(available_dir)
    return ids[: cfg.KEEP_CYCLES]


def prune_old_cloud_runs(
    map_type: str,
    available_dir: Path | None = None,
    tiles_dir: Path | None = None,
) -> list[str]:
    """
    Keep only the latest KEEP_CYCLES runs for cloud map types.
    Delete tiles for older runs. Returns list of pruned run_ids.
    """
    import shutil
    cfg = get_settings()
    d = available_dir or cfg.AVAILABLE_DIR
    td = tiles_dir or cfg.TILES_DIR

    # Only operate on cloud map types
    if map_type not in cfg.ARCHIVE_MAP_TYPES:
        return []

    ids = all_run_ids(d)
    to_prune = ids[cfg.KEEP_CYCLES:]
    pruned = []
    for run_id in to_prune:
        run_tile_dir = td / map_type / run_id
        if run_tile_dir.exists():
            shutil.rmtree(run_tile_dir, ignore_errors=True)
            pruned.append(run_id)
    return pruned

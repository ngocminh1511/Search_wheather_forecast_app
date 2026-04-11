from __future__ import annotations

"""
pipeline_adapter.py — unified internal facade over core modules.

noaa_be is fully self-contained. All logic lives inside noaa_be/app/core/:
  - map_specs.py   → MAP_SPECS, Product, MapSpec
  - downloader.py  → download_map, list_local_files
  - discovery.py   → discover_cycle, load_available_fff, latest_available_run

This file provides a single import surface for services/ and routers/.
"""

from datetime import date
from pathlib import Path

from .map_specs import MAP_SPECS, MapSpec, Product, segment_fff, resolve_fff  # noqa: F401
from .downloader import (
    download_map,
    download_product,
    grib_path,
    list_local_files,
    run_id_from_date,
)
from .discovery import (
    discover_cycle,
    load_available_fff,
    latest_available_run,
)


# ---------------------------------------------------------------------------
# Convenience wrappers (keep the same call signatures services/ expect)
# ---------------------------------------------------------------------------

def get_map_specs() -> dict:
    """Return MAP_SPECS dict (source of truth inside noaa_be)."""
    return MAP_SPECS


def download_map_dataset(
    map_type: str,
    run_date: date,
    run_hour: int,
    data_dir: Path,
    fff_values: list[int] | None = None,
    rpm_limit: int = 60,
    skip_existing: bool = True,
) -> dict:
    return download_map(
        map_type=map_type,
        run_date=run_date,
        run_hour=run_hour,
        data_dir=data_dir,
        fff_values=fff_values,
        rpm_limit=rpm_limit,
        skip_existing=skip_existing,
    )


def discover_update_times(
    run_date: date,
    run_hour: int,
    max_fff: int,
    available_dir: Path,
    rpm_limit: int = 60,
) -> dict:
    return discover_cycle(
        run_date=run_date,
        run_hour=run_hour,
        max_fff=max_fff,
        available_dir=available_dir,
        rpm_limit=rpm_limit,
    )

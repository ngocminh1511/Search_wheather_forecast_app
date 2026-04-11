from __future__ import annotations

"""
discovery.py — probe NOAA NOMADS inventory to discover which GFS cycles and
forecast hours (fff) are available, then save per-map availability JSON.

Ported from scripts/discover_update_times.py and scripts/split_availability_by_map.py
— noaa_be is fully self-contained, no dependency on scripts/.

Output files (under available_dir):
  <map_type>/availability_<yyyymmdd>_<hh>z_<map_type>.json
  availability_<yyyymmdd>_<hh>z.json   ← master (all map types combined)
"""

import json
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .map_specs import MAP_SPECS, Product


# ---------------------------------------------------------------------------
# NOAA inventory URL
# ---------------------------------------------------------------------------

_INVENTORY_BASE = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"


def _idx_url(run_date: date, run_hour: int, fff: int) -> str:
    ymd = run_date.strftime("%Y%m%d")
    hh = f"{run_hour:02d}"
    return (
        f"{_INVENTORY_BASE}/gfs.{ymd}/{hh}/atmos/"
        f"gfs.t{hh}z.pgrb2.0p25.f{fff:03d}.idx"
    )


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

def _fetch_idx(url: str, retries: int = 3) -> tuple[bool, str]:
    """Fetch .idx file. Returns (exists, body_text)."""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "noaa-be-discovery/1.0"})
            with urlopen(req, timeout=60) as resp:
                return True, resp.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            if exc.code == 404:
                return False, ""
            retry_after = exc.headers.get("Retry-After")
            time.sleep(float(retry_after) if retry_after else min(30, 2 ** attempt))
        except (URLError, Exception):
            time.sleep(min(30, 2 ** attempt))
    return False, ""


# ---------------------------------------------------------------------------
# .idx parsing helpers
# ---------------------------------------------------------------------------

def _normalize_level(level_key: str) -> str:
    """'lev_2_m_above_ground' → '2 m above ground'"""
    return level_key.replace("lev_", "").replace("_", " ").strip().lower()


def _product_requirements(product: Product) -> tuple[list[str], list[str]]:
    vars_req = [k.replace("var_", "") for k, v in product.query.items() if k.startswith("var_") and v == "on"]
    levels_req = [_normalize_level(k) for k, v in product.query.items() if k.startswith("lev_") and v == "on"]
    return vars_req, levels_req


def _idx_has_product(idx_text: str, vars_req: list[str], levels_req: list[str]) -> bool:
    idx_lower = idx_text.lower()
    for var in vars_req:
        if f":{var}:" not in idx_text:
            return False
    for level in levels_req:
        if level not in idx_lower:
            return False
    return True


def _infer_segments(values: list[int]) -> list[tuple[int, int, int]]:
    if not values:
        return []
    if len(values) == 1:
        return [(values[0], values[0], 1)]
    segments: list[tuple[int, int, int]] = []
    start = prev = values[0]
    step = values[1] - values[0]
    for cur in values[1:]:
        cur_step = cur - prev
        if cur_step != step:
            segments.append((start, prev, step))
            start = prev
            step = cur_step
        prev = cur
    segments.append((start, prev, step))
    return segments


# ---------------------------------------------------------------------------
# Core discovery function
# ---------------------------------------------------------------------------

def discover_cycle(
    run_date: date,
    run_hour: int,
    max_fff: int,
    available_dir: Path,
    rpm_limit: int = 60,
) -> dict:
    """
    Probe NOAA inventory for fff=0..max_fff of the given GFS cycle.

    Fast-path (2-step strategy):
      1. Check only max_fff first. If it exists → assume ALL frames f000..max_fff
         are present (NOAA publishes sequentially; if the last frame is up, all
         earlier ones are too). Skip the full sequential scan — saves ~36 requests.
      2. If max_fff is NOT yet available → scan f000..max_fff one-by-one so we
         know exactly which partial frames exist and can download them now.

    Saves per-map availability JSON files under available_dir/<map_type>/.
    Saves master availability JSON under available_dir/.
    Returns the full discovery result dict.
    """
    import logging
    log = logging.getLogger(__name__)

    min_interval = 60.0 / max(1, rpm_limit)

    # ── Fast-path: check the last frame first ────────────────────────────
    last_url = _idx_url(run_date, run_hour, max_fff)
    last_exists, last_body = _fetch_idx(last_url)

    idx_cache: dict[int, str] = {}
    fff_existing: list[int] = []

    if last_exists and last_body:
        # Full cycle is ready — assume f000..max_fff all exist.
        # Fetch f000 body so we can inspect variable availability (products),
        # but skip fetching all intermediate frames.
        log.info(
            "discover_cycle fast-path: f%03d exists → assuming f000..f%03d all present",
            max_fff, max_fff,
        )
        idx_cache[max_fff] = last_body
        fff_existing = list(range(0, max_fff + 1))

        # Grab f000 idx body for product-matching (only costs 1 extra request)
        f0_url = _idx_url(run_date, run_hour, 0)
        f0_exists, f0_body = _fetch_idx(f0_url)
        if f0_exists and f0_body:
            idx_cache[0] = f0_body
        # For product matching we use f000 body; all other frames assumed identical structure
        _body_for_matching = f0_body if f0_body else last_body

        # Build idx_cache with the same body for all frames (variables don't vary by frame)
        for fff in fff_existing:
            if fff not in idx_cache:
                idx_cache[fff] = _body_for_matching

    else:
        # Cycle not complete yet — scan frame-by-frame to find what's available
        log.info(
            "discover_cycle full-scan: f%03d not yet on NOAA, scanning f000..f%03d",
            max_fff, max_fff,
        )
        last_req = 0.0
        for fff in range(0, max_fff + 1):
            elapsed = time.time() - last_req
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)

            url = _idx_url(run_date, run_hour, fff)
            exists, body = _fetch_idx(url)
            last_req = time.time()

            if exists and body:
                idx_cache[fff] = body
                fff_existing.append(fff)

    # ── Phase 2: match products per map type ─────────────────────────────
    map_types_result: dict[str, dict] = {}

    for map_type, spec in MAP_SPECS.items():
        union: set[int] = set()
        products_info: dict[str, dict] = {}

        for product in spec.products:
            vars_req, levels_req = _product_requirements(product)
            matched = sorted(
                fff for fff, idx in idx_cache.items()
                if _idx_has_product(idx, vars_req, levels_req)
            )
            union.update(matched)
            products_info[product.name] = {
                "required_vars": vars_req,
                "required_levels": levels_req,
                "fff_available": matched,
                "segments": _infer_segments(matched),
            }

        union_sorted = sorted(union)
        map_types_result[map_type] = {
            "products": products_info,
            "union_fff": union_sorted,
            "union_segments": _infer_segments(union_sorted),
        }

    result = {
        "run_date": run_date.isoformat(),
        "run_hour": run_hour,
        "max_fff": max_fff,
        "fff_existing": fff_existing,
        "fff_existing_segments": _infer_segments(fff_existing),
        "map_types": map_types_result,
    }

    # Save files
    _save_availability(result, run_date, run_hour, available_dir)
    return result


# ---------------------------------------------------------------------------
# File saving helpers
# ---------------------------------------------------------------------------

def _save_availability(result: dict, run_date: date, run_hour: int, available_dir: Path) -> None:
    ymd = run_date.strftime("%Y%m%d")
    hh = f"{run_hour:02d}"
    available_dir.mkdir(parents=True, exist_ok=True)

    # Master file
    master = available_dir / f"availability_{ymd}_{hh}z.json"
    master.write_text(json.dumps(result, indent=2), encoding="utf-8")

    # Per-map files
    for map_type, map_info in result["map_types"].items():
        map_dir = available_dir / map_type
        map_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "run_date": result["run_date"],
            "run_hour": run_hour,
            "max_fff": result["max_fff"],
            "fff_existing": result["fff_existing"],
            "fff_existing_segments": result["fff_existing_segments"],
            "map_type": map_type,
            "map_info": map_info,
        }
        out = map_dir / f"availability_{ymd}_{hh}z_{map_type}.json"
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Reading availability from disk
# ---------------------------------------------------------------------------

def load_available_fff(available_dir: Path, map_type: str, run_date: date, run_hour: int) -> list[int]:
    """
    Return sorted list of fff values available for (map_type, run).
    Reads the per-map JSON file written by discover_cycle().
    """
    ymd = run_date.strftime("%Y%m%d")
    hh = f"{run_hour:02d}"
    per_map = available_dir / map_type / f"availability_{ymd}_{hh}z_{map_type}.json"
    master = available_dir / f"availability_{ymd}_{hh}z.json"

    for path in (per_map, master):
        if not path.exists():
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            # per-map format
            map_info = data.get("map_info", {})
            if map_info:
                return sorted(map_info.get("union_fff", []))
            # master format
            mt_data = data.get("map_types", {}).get(map_type, {})
            if mt_data:
                return sorted(mt_data.get("union_fff", []))
        except Exception:
            continue
    return []


def latest_available_run(available_dir: Path) -> tuple[date | None, int | None]:
    """
    Scan available_dir for the newest master availability JSON.
    Returns (run_date, run_hour) or (None, None).
    """
    import re
    pattern = re.compile(r"availability_(\d{4})(\d{2})(\d{2})_(\d{2})z\.json$")
    best: tuple[date, int] | None = None

    for f in available_dir.glob("availability_*.json"):
        m = pattern.match(f.name)
        if not m:
            continue
        run_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        run_hour = int(m.group(4))
        if best is None or (run_date, run_hour) > best:
            best = (run_date, run_hour)

    if best is None:
        return None, None
    return best

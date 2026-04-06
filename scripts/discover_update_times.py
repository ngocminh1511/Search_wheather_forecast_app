from __future__ import annotations

import json
import os
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from noaa_map_pipeline import MAP_SPECS


BASE_INVENTORY_URL = "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod"
DEFAULT_RUN_DATE = "2026-04-06"
DEFAULT_RUN_HOUR = 0
DEFAULT_MAX_FFF = 384
DEFAULT_RPM_LIMIT = 100


def format_eta(seconds: float) -> str:
    sec = max(0, int(seconds))
    hours = sec // 3600
    minutes = (sec % 3600) // 60
    secs = sec % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def print_progress(
    prefix: str,
    current: int,
    total: int,
    started_at: float,
    detail: str = "",
) -> None:
    total_safe = max(1, total)
    pct = (current / total_safe) * 100
    elapsed = max(0.001, time.time() - started_at)
    rate = current / elapsed
    remaining_items = max(0, total_safe - current)
    eta_seconds = (remaining_items / rate) if rate > 0 else 0

    message = (
        f"\r{prefix}: {current}/{total_safe} ({pct:6.2f}%)"
        f" | ETA {format_eta(eta_seconds)}"
    )
    if detail:
        message += f" | {detail}"
    print(message, end="", flush=True)
    if current >= total_safe:
        print()


def normalize_level_token(level_key: str) -> str:
    token = level_key.replace("lev_", "").replace("_", " ").strip().lower()
    replacements = {
        "entire atmosphere": "entire atmosphere",
        "surface": "surface",
    }
    return replacements.get(token, token)


def extract_requirements(query: Dict[str, str]) -> tuple[List[str], List[str]]:
    vars_required = [k.replace("var_", "") for k, v in query.items() if k.startswith("var_") and v == "on"]
    levels_required = [
        normalize_level_token(k)
        for k, v in query.items()
        if k.startswith("lev_") and v == "on"
    ]
    return vars_required, levels_required


def build_idx_url(run_date: date, run_hour: int, fff: int) -> str:
    ymd = run_date.strftime("%Y%m%d")
    hh = f"{run_hour:02d}"
    return (
        f"{BASE_INVENTORY_URL}/gfs.{ymd}/{hh}/atmos/"
        f"gfs.t{hh}z.pgrb2.0p25.f{fff:03d}.idx"
    )


def fetch_idx_text(url: str, retries: int = 3) -> tuple[bool, str]:
    last_error = ""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "noaa-fff-discovery/1.0"})
            with urlopen(req, timeout=60) as response:
                return True, response.read().decode("utf-8", errors="replace")
        except HTTPError as exc:
            if exc.code == 404:
                return False, ""
            last_error = f"HTTP {exc.code}"
            retry_after = exc.headers.get("Retry-After")
            sleep_s = float(retry_after) if retry_after else min(30, 2 ** attempt)
            time.sleep(sleep_s)
        except URLError as exc:
            last_error = f"URL error: {exc.reason}"
            time.sleep(min(30, 2 ** attempt))
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            time.sleep(min(30, 2 ** attempt))
    return False, f"failed:{last_error}"


def has_product_in_idx(idx_text: str, vars_required: List[str], levels_required: List[str]) -> bool:
    idx_lower = idx_text.lower()
    for var in vars_required:
        if f":{var}:" not in idx_text:
            return False
    for level in levels_required:
        if level not in idx_lower:
            return False
    return True


def infer_segments(values: List[int]) -> List[Tuple[int, int, int]]:
    if not values:
        return []
    if len(values) == 1:
        return [(values[0], values[0], 1)]

    segments: List[Tuple[int, int, int]] = []
    start = values[0]
    prev = values[0]
    step = values[1] - values[0]

    for i in range(1, len(values)):
        cur = values[i]
        cur_step = cur - prev
        if cur_step != step:
            segments.append((start, prev, step))
            start = prev
            step = cur_step
        prev = cur

    segments.append((start, prev, step))
    return segments


def discover_update_times(
    run_date: date,
    run_hour: int,
    max_fff: int,
    rpm_limit: int,
    output_root: Path | None = None,
) -> Dict:
    min_interval = 60.0 / max(1, rpm_limit)
    last_request_time = 0.0
    scan_started_at = time.time()

    idx_cache: Dict[int, str] = {}
    fff_existing: List[int] = []
    total_fff = max_fff + 1

    for fff in range(0, max_fff + 1):
        elapsed = time.time() - last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)

        url = build_idx_url(run_date=run_date, run_hour=run_hour, fff=fff)
        exists, body = fetch_idx_text(url)
        last_request_time = time.time()

        if exists and body:
            idx_cache[fff] = body
            fff_existing.append(fff)

        print_progress(
            prefix="IDX scan",
            current=fff + 1,
            total=total_fff,
            started_at=scan_started_at,
            detail=f"fff={fff:03d} found={len(fff_existing)}",
        )

    result: Dict = {
        "run_date": run_date.isoformat(),
        "run_hour": run_hour,
        "max_fff": max_fff,
        "fff_existing": fff_existing,
        "fff_existing_segments": infer_segments(fff_existing),
        "map_types": {},
    }

    total_products = sum(len(spec.products) for spec in MAP_SPECS.values())
    processed_products = 0
    analysis_started_at = time.time()

    if output_root is not None:
        output_root.mkdir(parents=True, exist_ok=True)

    for map_type, spec in MAP_SPECS.items():
        map_info = {
            "products": {},
            "union_fff": [],
            "union_segments": [],
        }

        union_set = set()
        for product in spec.products:
            vars_required, levels_required = extract_requirements(product.query)
            matched = []
            for fff, idx_text in idx_cache.items():
                if has_product_in_idx(idx_text, vars_required, levels_required):
                    matched.append(fff)

            matched = sorted(matched)
            union_set.update(matched)
            map_info["products"][product.name] = {
                "required_vars": vars_required,
                "required_levels": levels_required,
                "fff_available": matched,
                "segments": infer_segments(matched),
            }

            processed_products += 1
            print_progress(
                prefix="Map/Product analysis",
                current=processed_products,
                total=total_products,
                started_at=analysis_started_at,
                detail=f"{map_type}:{product.name} matched={len(matched)}",
            )

        union_values = sorted(union_set)
        map_info["union_fff"] = union_values
        map_info["union_segments"] = infer_segments(union_values)
        result["map_types"][map_type] = map_info

        if output_root is not None:
            map_dir = output_root / map_type
            map_dir.mkdir(parents=True, exist_ok=True)
            map_file = map_dir / f"availability_{run_date.strftime('%Y%m%d')}_{run_hour:02d}z.json"
            map_payload = {
                "run_date": run_date.isoformat(),
                "run_hour": run_hour,
                "map_type": map_type,
                "fff_existing": fff_existing,
                "fff_existing_segments": infer_segments(fff_existing),
                "map_info": map_info,
            }
            map_file.write_text(json.dumps(map_payload, indent=2), encoding="utf-8")
            print(f"Saved per-map availability: {map_file}")

    return result


def resolve_runtime_config() -> tuple[date, int, int, int]:
    run_date_str = os.getenv("NOAA_RUN_DATE", DEFAULT_RUN_DATE)
    run_hour = int(os.getenv("NOAA_RUN_HOUR", str(DEFAULT_RUN_HOUR)))
    max_fff = int(os.getenv("NOAA_MAX_FFF", str(DEFAULT_MAX_FFF)))
    rpm_limit = int(os.getenv("NOAA_RPM_LIMIT", str(DEFAULT_RPM_LIMIT)))

    run_date = date.fromisoformat(run_date_str)
    if run_hour < 0 or run_hour > 23:
        raise ValueError("NOAA_RUN_HOUR must be between 0 and 23")
    if max_fff < 0:
        raise ValueError("NOAA_MAX_FFF must be >= 0")
    if rpm_limit <= 0:
        raise ValueError("NOAA_RPM_LIMIT must be > 0")

    return run_date, run_hour, max_fff, rpm_limit


def main() -> None:
    run_date, run_hour, max_fff, rpm_limit = resolve_runtime_config()
    root = Path(__file__).resolve().parents[1]

    print(
        f"Discover config => date={run_date.isoformat()}, hour={run_hour:02d}, "
        f"max_fff={max_fff}, rpm_limit={rpm_limit}"
    )

    result = discover_update_times(
        run_date=run_date,
        run_hour=run_hour,
        max_fff=max_fff,
        rpm_limit=rpm_limit,
        output_root=root / "available",
    )

    out_dir = root / "available"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"availability_{run_date.strftime('%Y%m%d')}_{run_hour:02d}z.json"
    out_file.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print(f"Existing fff count: {len(result['fff_existing'])}")
    print(f"Wrote: {out_file}")


if __name__ == "__main__":
    main()

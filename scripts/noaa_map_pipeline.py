from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl"
COMMON_QUERY = {
    "leftlon": "0",
    "rightlon": "360",
    "toplat": "90",
    "bottomlat": "-90",
}


@dataclass(frozen=True)
class Product:
    name: str
    query: Dict[str, str]


@dataclass(frozen=True)
class MapSpec:
    map_type: str
    description: str
    products: List[Product]
    fff_segments_full: List[Tuple[int, int, int]]


def format_eta(seconds: float) -> str:
    sec = max(0, int(seconds))
    hours = sec // 3600
    minutes = (sec % 3600) // 60
    secs = sec % 60
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def print_progress(prefix: str, current: int, total: int, started_at: float, detail: str = "") -> None:
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


def segment_fff(segments: Iterable[Tuple[int, int, int]]) -> List[int]:
    fff_values = set()
    for start, end, step in segments:
        for value in range(start, end + 1, step):
            fff_values.add(value)
    return sorted(fff_values)


MAP_SPECS: Dict[str, MapSpec] = {
    "temperature_feels_like": MapSpec(
        map_type="temperature_feels_like",
        description="TMP at 2m, full forecast from f000: 0-120h hourly, then 3-hourly to 384h",
        products=[
            Product(
                name="tmp_2m",
                query={"lev_2_m_above_ground": "on", "var_TMP": "on"},
            )
        ],
        fff_segments_full=[(0, 120, 1), (123, 384, 3)],
    ),
    "rain_basic": MapSpec(
        map_type="rain_basic",
        description="APCP at surface, 24h every 3h",
        products=[
            Product(
                name="apcp_surface",
                query={"lev_surface": "on", "var_APCP": "on"},
            )
        ],
        fff_segments_full=[(0, 24, 3)],
    ),
    "rain_advanced": MapSpec(
        map_type="rain_advanced",
        description="PRATE+CRAIN+CSNOW at surface, 24h every 3h",
        products=[
            Product(
                name="rain_adv_surface",
                query={
                    "lev_surface": "on",
                    "var_PRATE": "on",
                    "var_CRAIN": "on",
                    "var_CSNOW": "on",
                },
            )
        ],
        fff_segments_full=[(0, 24, 3)],
    ),
    "cloud_total": MapSpec(
        map_type="cloud_total",
        description="TCDC entire atmosphere",
        products=[
            Product(
                name="tcdc_entire_atmosphere",
                query={"lev_entire_atmosphere": "on", "var_TCDC": "on"},
            )
        ],
        # Stored as analysis snapshots by run time (f000).
        fff_segments_full=[(0, 0, 1)],
    ),
    "cloud_layered": MapSpec(
        map_type="cloud_layered",
        description="LCDC/MCDC/HCDC split by layer",
        products=[
            Product(
                name="low_cloud",
                query={"lev_low_cloud_layer": "on", "var_LCDC": "on"},
            ),
            Product(
                name="mid_cloud",
                query={"lev_middle_cloud_layer": "on", "var_MCDC": "on"},
            ),
            Product(
                name="high_cloud",
                query={"lev_high_cloud_layer": "on", "var_HCDC": "on"},
            ),
        ],
        fff_segments_full=[(0, 0, 1)],
    ),
    "snow_depth": MapSpec(
        map_type="snow_depth",
        description=(
            "SNOD surface, around 10 days: 0-48h/3h, 54h/6h until 54h, "
            "then 12h until 240h"
        ),
        products=[
            Product(
                name="snod_surface",
                query={"lev_surface": "on", "var_SNOD": "on"},
            )
        ],
        fff_segments_full=[(0, 48, 3), (54, 54, 6), (66, 240, 12)],
    ),
    "wind_animation": MapSpec(
        map_type="wind_animation",
        description="U/V winds for selected layers, 16 days",
        products=[
            Product(
                name="wind_30m",
                query={
                    "lev_30_m_above_ground": "on",
                    "var_UGRD": "on",
                    "var_VGRD": "on",
                },
            ),
            Product(
                name="wind_50m",
                query={
                    "lev_50_m_above_ground": "on",
                    "var_UGRD": "on",
                    "var_VGRD": "on",
                },
            ),
            Product(
                name="wind_100m",
                query={
                    "lev_100_m_above_ground": "on",
                    "var_UGRD": "on",
                    "var_VGRD": "on",
                },
            ),
            # Product(
            #     name="wind_1000m",
            #     query={
            #         "lev_1000_m_above_ground": "on",
            #         "var_UGRD": "on",
            #         "var_VGRD": "on",
            #     },
            # ),
            Product(
                name="wind_600mb_approx_4200m",
                query={"lev_600_mb": "on", "var_UGRD": "on", "var_VGRD": "on"},
            ),
            Product(
                name="wind_300mb_approx_9200m",
                query={"lev_300_mb": "on", "var_UGRD": "on", "var_VGRD": "on"},
            ),
            Product(
                name="wind_250mb_approx_10400m",
                query={"lev_250_mb": "on", "var_UGRD": "on", "var_VGRD": "on"},
            ),
            Product(
                name="wind_200mb_approx_11800m",
                query={"lev_200_mb": "on", "var_UGRD": "on", "var_VGRD": "on"},
            ),
        ],
        fff_segments_full=[(0, 120, 1), (123, 384, 3)],
    ),
}


def build_url(run_date: date, run_hour: int, fff: int, product_query: Dict[str, str]) -> str:
    hh = f"{run_hour:02d}"
    ymd = run_date.strftime("%Y%m%d")
    query = {
        "file": f"gfs.t{hh}z.pgrb2.0p25.f{fff:03d}",
        **product_query,
        **COMMON_QUERY,
        "dir": f"/gfs.{ymd}/{hh}/atmos",
    }
    return f"{BASE_URL}?{urlencode(query)}"


def resolve_fff(spec: MapSpec, mode: str = "full") -> List[int]:
    if mode in ("init_only", "f00_only"):
        return [0]
    return segment_fff(spec.fff_segments_full)


def _extract_fff_from_payload(payload: Dict, map_type: str) -> List[int]:
    if "map_info" in payload and isinstance(payload["map_info"], dict):
        union_values = payload["map_info"].get("union_fff", [])
        return sorted({int(v) for v in union_values})

    map_types = payload.get("map_types", {})
    if map_type in map_types and isinstance(map_types[map_type], dict):
        union_values = map_types[map_type].get("union_fff", [])
        return sorted({int(v) for v in union_values})

    return []


def load_fff_from_available(base_dir: str, map_type: str, run_date: date, run_hour: int) -> List[int]:
    root = Path(base_dir)
    ymd = run_date.strftime("%Y%m%d")
    hh = f"{run_hour:02d}"

    candidates = [
        root / "available" / map_type / f"availability_{ymd}_{hh}z_{map_type}.json",
        root / "available" / f"availability_{ymd}_{hh}z.json",
    ]

    for file_path in candidates:
        if not file_path.exists():
            continue
        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
            fff_values = _extract_fff_from_payload(payload, map_type=map_type)
            if fff_values:
                return fff_values
        except Exception:
            continue

    return []


def _download_with_retry(url: str, output_file: Path, retries: int = 5) -> str:
    last_error = ""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "noaa-map-pipeline/1.0"})
            with urlopen(req, timeout=240) as response:
                payload = response.read()
                output_file.write_bytes(payload)
            return "downloaded"
        except HTTPError as exc:
            last_error = f"HTTP {exc.code}"
            if exc.code in (429, 500, 502, 503, 504):
                retry_after = exc.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else min(60, 2 ** attempt)
                time.sleep(sleep_s)
                continue
            return f"failed:{last_error}"
        except URLError as exc:
            last_error = f"URL error: {exc.reason}"
            time.sleep(min(60, 2 ** attempt))
        except Exception as exc:  # noqa: BLE001
            last_error = f"Error: {exc}"
            time.sleep(min(60, 2 ** attempt))
    return f"failed:{last_error}"


def download_map_dataset(
    map_type: str,
    run_date: date,
    run_hour: int,
    base_dir: str,
    mode: str = "full",
    rpm_limit: int = 60,
    skip_existing: bool = True,
) -> Dict:
    if map_type not in MAP_SPECS:
        raise ValueError(f"Unknown map_type: {map_type}")

    spec = MAP_SPECS[map_type]
    target_dir = Path(base_dir) / "data" / map_type
    target_dir.mkdir(parents=True, exist_ok=True)

    fff_source = "segments"
    if mode == "full":
        available_fff = load_fff_from_available(
            base_dir=base_dir,
            map_type=map_type,
            run_date=run_date,
            run_hour=run_hour,
        )
        if available_fff:
            fff_values = available_fff
            fff_source = "available"
        else:
            fff_values = resolve_fff(spec, mode=mode)
    elif mode == "available_only":
        fff_values = load_fff_from_available(
            base_dir=base_dir,
            map_type=map_type,
            run_date=run_date,
            run_hour=run_hour,
        )
        if not fff_values:
            raise ValueError(
                f"No availability file found for map '{map_type}' at {run_date} {run_hour:02d}z"
            )
        fff_source = "available"
    else:
        fff_values = resolve_fff(spec, mode=mode)

    min_interval = 60.0 / max(1, rpm_limit)
    last_request_time = 0.0

    downloads = []
    total_jobs = len(fff_values) * len(spec.products)
    processed_jobs = 0
    started_at = time.time()

    for fff in fff_values:
        for product in spec.products:
            file_name = f"gfs.t{run_hour:02d}z.pgrb2.0p25.f{fff:03d}.{product.name}.grib2"
            out_file = target_dir / file_name
            url = build_url(run_date=run_date, run_hour=run_hour, fff=fff, product_query=product.query)

            if skip_existing and out_file.exists() and out_file.stat().st_size > 0:
                status = "skipped_existing"
            else:
                elapsed = time.time() - last_request_time
                if elapsed < min_interval:
                    time.sleep(min_interval - elapsed)
                status = _download_with_retry(url=url, output_file=out_file)
                last_request_time = time.time()

            downloads.append(
                {
                    "fff": fff,
                    "product": product.name,
                    "file": str(out_file),
                    "status": status,
                    "url": url,
                }
            )

            processed_jobs += 1
            print_progress(
                prefix=f"Download {map_type}",
                current=processed_jobs,
                total=total_jobs,
                started_at=started_at,
                detail=f"fff={fff:03d} product={product.name} status={status}",
            )

    manifest = {
        "map_type": map_type,
        "run_date": run_date.isoformat(),
        "run_hour": run_hour,
        "mode": mode,
        "fff_source": fff_source,
        "fff_segments_full": spec.fff_segments_full,
        "fff_values_requested": fff_values,
        "downloads": downloads,
        "generated_at_epoch": int(time.time()),
    }

    manifest_file = target_dir / f"manifest_{run_date.strftime('%Y%m%d')}_{run_hour:02d}z_{mode}.json"
    manifest_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def analyze_data_folder(base_dir: str, map_type: str) -> Dict:
    folder = Path(base_dir) / "data" / map_type
    folder.mkdir(parents=True, exist_ok=True)

    grib_files = sorted(folder.glob("*.grib2"))
    entries = []
    fff_seen = set()
    total_size = 0

    for file in grib_files:
        match = re.search(r"\.f(\d{3})\.", file.name)
        fff = int(match.group(1)) if match else None
        if fff is not None:
            fff_seen.add(fff)

        size = file.stat().st_size
        total_size += size
        entries.append({"file": file.name, "size_bytes": size, "fff": fff})

    summary = {
        "map_type": map_type,
        "file_count": len(entries),
        "total_size_mb": round(total_size / (1024 * 1024), 3),
        "fff_available": sorted(fff_seen),
        "files": entries,
    }

    out_json = folder / "data_summary.json"
    out_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def download_all_maps_for_run(
    base_dir: str,
    run_date: date,
    run_hour: int,
    mode: str = "full",
    rpm_limit: int = 60,
) -> Dict[str, Dict]:
    all_results: Dict[str, Dict] = {}
    for map_type in MAP_SPECS:
        manifest = download_map_dataset(
            map_type=map_type,
            run_date=run_date,
            run_hour=run_hour,
            base_dir=base_dir,
            mode=mode,
            rpm_limit=rpm_limit,
        )
        analyze_data_folder(base_dir=base_dir, map_type=map_type)
        all_results[map_type] = manifest
    return all_results

from __future__ import annotations

"""
downloader.py — download GRIB2 files from NOAA NOMADS.

Ported from scripts/noaa_map_pipeline.py — noaa_be is fully self-contained.

File naming convention inside noaa_be:
  data/<map_type>/<run_id>/<product_name>/f<fff:03d>.grib2

e.g.:
  data/rain_basic/20260406_00z/apcp_surface/f000.grib2
  data/wind_animation/20260406_00z/wind_30m/f003.grib2

This differs from the research scripts/ naming so the two never conflict.
"""

import json
import time
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .map_specs import MAP_SPECS, NOMADS_BASE_URL, NOMADS_COMMON_QUERY, Product, segment_fff


# ---------------------------------------------------------------------------
# URL builder
# ---------------------------------------------------------------------------

def build_download_url(run_date: date, run_hour: int, fff: int, product_query: dict) -> str:
    hh = f"{run_hour:02d}"
    ymd = run_date.strftime("%Y%m%d")
    query = {
        "file": f"gfs.t{hh}z.pgrb2.0p25.f{fff:03d}",
        **product_query,
        **NOMADS_COMMON_QUERY,
        "dir": f"/gfs.{ymd}/{hh}/atmos",
    }
    return f"{NOMADS_BASE_URL}?{urlencode(query)}"


# ---------------------------------------------------------------------------
# HTTP helpers (stdlib only — no extra deps)
# ---------------------------------------------------------------------------

def _download_with_retry(url: str, output_file: Path, retries: int = 5) -> str:
    """Download url → output_file. Returns status string."""
    last_error = ""
    for attempt in range(retries):
        try:
            req = Request(url, headers={"User-Agent": "noaa-be/1.0"})
            with urlopen(req, timeout=240) as response:
                output_file.write_bytes(response.read())
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
        except Exception as exc:
            last_error = str(exc)
            time.sleep(min(60, 2 ** attempt))
    return f"failed:{last_error}"


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

def grib_path(data_dir: Path, map_type: str, run_id: str, product_name: str, fff: int) -> Path:
    """
    Canonical path for a single GRIB2 file inside noaa_be.
    data_dir / map_type / run_id / product_name / f<fff:03d>.grib2
    """
    return data_dir / map_type / run_id / product_name / f"f{fff:03d}.grib2"


def run_id_from_date(run_date: date, run_hour: int) -> str:
    return f"{run_date.strftime('%Y%m%d')}_{run_hour:02d}z"


# ---------------------------------------------------------------------------
# Public download API
# ---------------------------------------------------------------------------

def download_product(
    map_type: str,
    run_date: date,
    run_hour: int,
    fff: int,
    product: Product,
    data_dir: Path,
    rpm_limit: int = 60,
    skip_existing: bool = True,
) -> dict:
    """Download one (map_type, fff, product) GRIB2 file. Returns status dict."""
    run_id = run_id_from_date(run_date, run_hour)
    out = grib_path(data_dir, map_type, run_id, product.name, fff)
    out.parent.mkdir(parents=True, exist_ok=True)

    if skip_existing and out.exists() and out.stat().st_size > 0:
        return {"fff": fff, "product": product.name, "file": str(out), "status": "skipped_existing"}

    url = build_download_url(run_date, run_hour, fff, product.query)
    status = _download_with_retry(url, out)
    return {"fff": fff, "product": product.name, "file": str(out), "status": status, "url": url}


def download_map(
    map_type: str,
    run_date: date,
    run_hour: int,
    data_dir: Path,
    fff_values: list[int] | None = None,
    rpm_limit: int = 60,
    skip_existing: bool = True,
) -> dict:
    """
    Download all products for a (map_type, run).
    If fff_values is None, uses the full fff_segments_full from MAP_SPECS.
    Returns manifest dict.
    """
    if map_type not in MAP_SPECS:
        raise ValueError(f"Unknown map_type: {map_type!r}")

    spec = MAP_SPECS[map_type]
    if fff_values is None:
        fff_values = segment_fff(spec.fff_segments_full)

    min_interval = 60.0 / max(1, rpm_limit)
    last_request_time = 0.0
    results: list[dict] = []
    
    from .db import check_cancel_requested, JobCancelledError

    for fff in fff_values:
        if check_cancel_requested(map_type):
            raise JobCancelledError("Job cancelled by user.")
            
        for product in spec.products:
            elapsed = time.time() - last_request_time
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)

            r = download_product(
                map_type=map_type,
                run_date=run_date,
                run_hour=run_hour,
                fff=fff,
                product=product,
                data_dir=data_dir,
                skip_existing=skip_existing,
            )
            results.append(r)
            last_request_time = time.time()

    return {
        "map_type": map_type,
        "run_date": run_date.isoformat(),
        "run_hour": run_hour,
        "run_id": run_id_from_date(run_date, run_hour),
        "fff_values": fff_values,
        "downloads": results,
    }


def list_local_files(data_dir: Path, map_type: str, run_id: str) -> dict[str, list[int]]:
    """
    Scan data_dir/<map_type>/<run_id>/ and return {product_name: [fff, ...]} for files on disk.
    """
    base = data_dir / map_type / run_id
    result: dict[str, list[int]] = {}
    if not base.exists():
        return result
    for product_dir in sorted(base.iterdir()):
        if not product_dir.is_dir():
            continue
        fffs = []
        for f in sorted(product_dir.glob("f*.grib2")):
            try:
                fff = int(f.stem[1:])  # "f003" → 3
                if f.stat().st_size > 0:
                    fffs.append(fff)
            except ValueError:
                continue
        if fffs:
            result[product_dir.name] = fffs
    return result

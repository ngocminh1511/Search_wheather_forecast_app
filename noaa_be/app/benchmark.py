"""
benchmark.py — First-frame and multi-frame benchmark for the tile generation pipeline.

Modes:
  baseline    : WORKERS=1 across all stages (single-threaded)
  stable-prod : parse=1, build=1, cut=2, write=1 (closest to production)

Target maps (first-frame):
  rain_advanced, rain_advanced_base_only, rain_advanced_full
  rain_basic, temperature_feels_like, snow_depth
  wind_surface, wind_base_only, wind_field_only

Multi-frame targets (3 frames near/mid/far):
  rain_advanced, wind_surface
"""
import sys
import os
import json
import csv
import time
import threading
import argparse
from typing import Dict, Any, List, Optional  # noqa: F401

psutil: Any
try:
    import psutil  # type: ignore[no-redef]
    _HAS_PSUTIL = True
except ImportError:
    psutil = None
    _HAS_PSUTIL = False

from pathlib import Path

# --- 1. PARSE ARGS EARLY (before heavy imports) ---
def _parse_args():
    p = argparse.ArgumentParser(description="Tile Generation Benchmark")
    p.add_argument(
        "--mode",
        choices=["baseline", "stable-prod", "scheduler_realistic", "predict", "cold_zone"],
        default="stable-prod",
        help=(
            "baseline=WORKERS=1 | stable-prod=production limits | "
            "scheduler_realistic=simulate real scheduler queue | "
            "predict=estimate frame counts + storage without running generation | "
            "cold_zone=measure generate-vs-hardlink speedup for cold zone frames"
        ),
    )
    p.add_argument(
        "--multi-frame",
        action="store_true",
        help="Also run 3-frame benchmark for rain_advanced and wind_surface",
    )
    p.add_argument(
        "--maps",
        nargs="*",
        help="Limit to specific map types (e.g. rain_advanced wind_surface)",
    )
    p.add_argument(
        "--zoom",
        type=int,
        default=8,
        help="Zoom level for predict mode tile-count estimates (default: 8)",
    )
    p.add_argument(
        "--cold-frames",
        type=int,
        default=1,
        dest="cold_frames",
        help="cold_zone mode: number of frames to test per zone (hot/cold) per map (default: 1)",
    )
    return p.parse_args()

_ARGS = _parse_args()

# --- 2. OVERRIDE SETTINGS BEFORE IMPORTING HEAVY MODULES ---
from app.config import get_settings
cfg = get_settings()

cfg.WRITE_DEBUG_PNGS = False
cfg.TILE_SKIP_EXISTING_CHUNKS = False

if _ARGS.mode == "baseline":
    cfg.MAX_PARSE_WORKERS = 1
    cfg.MAX_BUILD_WORKERS = 1
    cfg.MAX_CUT_WORKERS = 1
    cfg.MAX_WRITE_WORKERS = 1
    cfg.TILE_WORKERS = 1
    cfg.TILE_PROCESS_WORKERS = 1
    cfg.TILE_MIN_PROCESS_WORKERS = 1
    cfg.TILE_MAX_INFLIGHT_MULTIPLIER = 1
elif _ARGS.mode in ("stable-prod", "scheduler_realistic"):
    cfg.MAX_PARSE_WORKERS = 1
    cfg.MAX_BUILD_WORKERS = 1
    cfg.MAX_CUT_WORKERS = 2
    cfg.MAX_WRITE_WORKERS = 1
    cfg.TILE_WORKERS = 1
    cfg.TILE_PROCESS_WORKERS = 2
    cfg.TILE_MIN_PROCESS_WORKERS = 1
    cfg.TILE_MAX_INFLIGHT_MULTIPLIER = 2

from app.services.pipeline_tasks import (
    task_parse_fields,
    task_build_canvas,
    task_cut_and_write_tiles,
    task_generate_custom_frame,
    publish_staging_to_live,
)
from app.services.tile_generator import _MAP_PRODUCTS


# --- 3. RESOURCE MONITOR ---
class ResourceMonitor:
    def __init__(self):
        self.running = False
        self.thread = None
        self.cpu_samples = []
        self.ram_peak = 0.0
        self.io_wait_peak = 0.0
        self.start_disk_io = None
        self.end_disk_io = None
        self.interval = 0.1

    def _monitor_loop(self):
        if not _HAS_PSUTIL:
            return
        try:
            self.start_disk_io = psutil.disk_io_counters()
        except Exception:
            pass
        while self.running:
            try:
                cpu = psutil.cpu_percent(interval=None)
                self.cpu_samples.append(cpu)
                mem = psutil.virtual_memory()
                ram_gb = mem.used / (1024 ** 3)
                if ram_gb > self.ram_peak:
                    self.ram_peak = ram_gb
                cpu_times = psutil.cpu_times_percent(interval=None)
                io_wait = getattr(cpu_times, "iowait", 0.0)
                if io_wait > self.io_wait_peak:
                    self.io_wait_peak = io_wait
            except Exception:
                pass
            time.sleep(self.interval)
        try:
            self.end_disk_io = psutil.disk_io_counters()
        except Exception:
            pass

    def start(self):
        self.running = True
        self.cpu_samples = []
        self.ram_peak = 0.0
        self.io_wait_peak = 0.0
        if _HAS_PSUTIL:
            psutil.cpu_percent(interval=None)
            psutil.cpu_times_percent(interval=None)
            self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
            self.thread.start()

    def stop(self) -> Dict[str, float]:
        self.running = False
        if self.thread:
            self.thread.join(timeout=2.0)
        cpu_avg = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0.0
        cpu_peak = max(self.cpu_samples) if self.cpu_samples else 0.0
        read_mb = write_mb = 0.0
        if self.start_disk_io and self.end_disk_io:
            read_mb = max(0, self.end_disk_io.read_bytes - self.start_disk_io.read_bytes) / (1024 ** 2)
            write_mb = max(0, self.end_disk_io.write_bytes - self.start_disk_io.write_bytes) / (1024 ** 2)
        return {
            "cpu_avg_percent": round(cpu_avg, 2),
            "cpu_peak_percent": round(cpu_peak, 2),
            "ram_peak_gb": round(self.ram_peak, 2),
            "io_wait_peak_percent": round(self.io_wait_peak, 2),
            "read_mb": round(read_mb, 2),
            "write_mb": round(write_mb, 2),
        }


# --- 4. HELPERS ---
def find_frame(map_type: str, target_fff: Optional[int] = None) -> tuple:
    """
    Returns (run_id, fff, product_name) for a given map.
    If target_fff is None → picks the first (smallest) available fff.
    """
    # Normalize the map_type for the data dir lookup (strip suffixes used in benchmark)
    base_map = map_type.replace("_base_only", "").replace("_field_only", "").replace("_full", "")
    map_dir = cfg.DATA_DIR / base_map
    if not map_dir.exists():
        return None, None, None

    runs = sorted([d.name for d in map_dir.iterdir() if d.is_dir()], reverse=True)
    if not runs:
        return None, None, None

    latest_run = runs[0]
    products = _MAP_PRODUCTS.get(base_map, [])
    product_name = products[0] if products else None

    prod_dir = map_dir / latest_run / product_name if product_name else None
    if not prod_dir or not prod_dir.exists():
        subdirs = [d for d in (map_dir / latest_run).iterdir() if d.is_dir()]
        if not subdirs:
            return None, None, None
        prod_dir = subdirs[0]
        product_name = prod_dir.name

    fffs = []
    for grib in prod_dir.glob("f*.grib2"):
        try:
            fffs.append(int(grib.stem[1:]))
        except ValueError:
            pass

    if not fffs:
        return latest_run, None, product_name

    if target_fff is not None:
        return latest_run, target_fff if target_fff in fffs else None, product_name

    return latest_run, min(fffs), product_name


def pick_3_frames(map_type: str) -> List[int]:
    """Pick 3 representative fffs: earliest, middle, and latest available."""
    base_map = map_type.replace("_base_only", "").replace("_field_only", "").replace("_full", "")
    map_dir = cfg.DATA_DIR / base_map
    if not map_dir.exists():
        return []
    runs = sorted([d.name for d in map_dir.iterdir() if d.is_dir()], reverse=True)
    if not runs:
        return []
    latest_run = runs[0]
    products = _MAP_PRODUCTS.get(base_map, [])
    product_name = products[0] if products else None
    prod_dir = map_dir / latest_run / (product_name or "")
    if not prod_dir.exists():
        subdirs = [d for d in (map_dir / latest_run).iterdir() if d.is_dir()]
        if not subdirs:
            return []
        prod_dir = subdirs[0]
    fffs = sorted({int(g.stem[1:]) for g in prod_dir.glob("f*.grib2") if g.stem[1:].isdigit()})
    if len(fffs) < 3:
        return fffs
    return [fffs[0], fffs[len(fffs) // 2], fffs[-1]]


def count_output_dir(staging_dir: Path) -> tuple:
    if not staging_dir.exists():
        return 0, 0.0
    chunks = list(staging_dir.rglob("*.chunk"))
    total_bytes = sum(f.stat().st_size for f in chunks)
    return len(chunks), total_bytes


def _extract_custom_stats(res: dict, staging_dir: Path, fff: int, start_time: float) -> dict:
    """Pull stage timing + tile stats from custom pipeline results."""
    timings = res.get("timings", {})
    raw_read = timings.get("raw_read_time_s", 0.0)
    warp = timings.get("warp_time_s", 0.0)
    tile_cut = timings.get("tile_cut_time_s", 0.0)
    total_t = time.perf_counter() - start_time

    base = res.get("base", {})

    # Aggregate tile stats from manifests if base doesn't have them
    tiles_total = base.get("total", 0)
    tiles_saved = base.get("saved", 0)
    tiles_skipped = base.get("empty_skipped", 0)
    chunks_written = base.get("chunks_written", 0)

    if not tiles_total:
        frame_dir = staging_dir / f"{fff:03d}"
        for m_path in frame_dir.rglob("manifest.json"):
            try:
                m = json.loads(m_path.read_text())
                tiles_total += m.get("total", 0)
                tiles_saved += m.get("saved", 0)
                tiles_skipped += m.get("empty_skipped", 0)
                chunks_written += m.get("chunks_written", 0)
            except Exception:
                pass

    _, output_bytes = count_output_dir(staging_dir / f"{fff:03d}")

    # Detail from metatile-level stats if available
    metatiles_total = base.get("metatiles_total", 0)
    metatiles_empty = base.get("metatiles_empty_skipped", 0)
    tiles_skipped_inside = base.get("tiles_empty_skipped_inside_nonempty", 0)
    bytes_before = base.get("bytes_before_compress", 0)
    bytes_after = base.get("bytes_after_compress", 0)
    colorize_t = base.get("colorize_time_s", 0.0)
    encode_t = base.get("encode_time_s", 0.0)
    chunk_write_t = base.get("chunk_write_time_s", 0.0)
    metatile_extract_t = base.get("metatile_extract_time_s", 0.0)

    return dict(
        parse_time_s=round(raw_read, 3),
        build_time_s=round(warp, 3),
        cut_time_s=round(tile_cut, 3),
        metatile_extract_time_s=round(metatile_extract_t, 3),
        colorize_time_s=round(colorize_t, 3),
        encode_time_s=round(encode_t, 3),
        chunk_write_time_s=round(chunk_write_t, 3),
        total_time_s=round(total_t, 3),
        tiles_total=tiles_total,
        tiles_saved=tiles_saved,
        tiles_empty_skipped=tiles_skipped,
        tiles_empty_skipped_inside_nonempty=tiles_skipped_inside,
        metatiles_total=metatiles_total,
        metatiles_empty_skipped=metatiles_empty,
        chunks_written=chunks_written,
        output_bytes=output_bytes,
        bytes_before_compress=bytes_before,
        bytes_after_compress=bytes_after,
    )


def _build_row(
    map_type: str, label: str, run_id: str, fff: int, stats: dict,
    metrics: dict, publish_t: float, status: str
) -> dict:
    tiles_saved = stats.get("tiles_saved", 0)
    chunks_written = stats.get("chunks_written", 0)
    total_t = stats.get("total_time_s", 0.0)
    bytes_before = stats.get("bytes_before_compress", 0)
    bytes_after = stats.get("bytes_after_compress", 0)
    compression_ratio = round(bytes_after / bytes_before, 4) if bytes_before > 0 else None
    time_per_saved_tile_ms = round(total_t * 1000 / tiles_saved, 2) if tiles_saved > 0 else None
    time_per_chunk_ms = round(total_t * 1000 / chunks_written, 2) if chunks_written > 0 else None

    return {
        "benchmark_mode": _ARGS.mode,
        "map_type": map_type,
        "frame_label": label,
        "source_run": run_id,
        "source_fff": fff,

        "parse_time_s": stats.get("parse_time_s", 0.0),
        "build_time_s": stats.get("build_time_s", 0.0),
        "cut_time_s": stats.get("cut_time_s", 0.0),
        "metatile_extract_time_s": stats.get("metatile_extract_time_s", 0.0),
        "colorize_time_s": stats.get("colorize_time_s", 0.0),
        "encode_time_s": stats.get("encode_time_s", 0.0),
        "chunk_write_time_s": stats.get("chunk_write_time_s", 0.0),
        "publish_time_s": round(publish_t, 3),
        "total_time_s": stats.get("total_time_s", 0.0),

        "tiles_total": stats.get("tiles_total", 0),
        "tiles_saved": tiles_saved,
        "tiles_empty_skipped": stats.get("tiles_empty_skipped", 0),
        "tiles_empty_skipped_inside_nonempty": stats.get("tiles_empty_skipped_inside_nonempty", 0),
        "metatiles_total": stats.get("metatiles_total", 0),
        "metatiles_empty_skipped": stats.get("metatiles_empty_skipped", 0),
        "chunks_written": chunks_written,
        "output_bytes": stats.get("output_bytes", 0),

        "bytes_before_compress": stats.get("bytes_before_compress", 0),
        "bytes_after_compress": stats.get("bytes_after_compress", 0),
        "compression_ratio": compression_ratio,
        "time_per_saved_tile_ms": time_per_saved_tile_ms,
        "time_per_chunk_ms": time_per_chunk_ms,

        **metrics,
        "status": status,
    }


# --- 5. SINGLE-FRAME RUNNER ---
def _run_one_frame(map_type: str, run_id: str, fff: int, product_name: str) -> dict:
    """
    Execute one map+frame benchmark.
    Returns a stats dict compatible with _build_row().
    """
    is_custom = map_type in ["rain_advanced", "wind_surface"]
    base_only = map_type.endswith("_base_only")
    field_only = map_type.endswith("_field_only")
    # Normalize lookup key for custom frames
    base_map = map_type.replace("_base_only", "").replace("_field_only", "").replace("_full", "")

    parse_t = build_t = cut_t = publish_t = 0.0
    metatile_extract_t = colorize_t = encode_t = chunk_write_t = 0.0
    tiles_total = tiles_saved = tiles_skipped = tiles_skipped_inside = 0
    metatiles_total = metatiles_empty = 0
    chunks_written = output_bytes = 0
    bytes_before = bytes_after = 0

    monitor = ResourceMonitor()
    monitor.start()
    start_time = time.perf_counter()

    try:
        if is_custom or base_only or field_only:
            res = task_generate_custom_frame(
                base_map, run_id, fff, product_name,
                base_only=base_only, field_only=field_only,
            )
            if res.get("skipped"):
                raise RuntimeError(f"Skipped: {res.get('reason')}")

            staging_dir = cfg.STAGING_DIR / base_map / run_id
            extracted = _extract_custom_stats(res, staging_dir, fff, start_time)
            parse_t = extracted["parse_time_s"]
            build_t = extracted["build_time_s"]
            cut_t = extracted["cut_time_s"]
            metatile_extract_t = extracted["metatile_extract_time_s"]
            colorize_t = extracted["colorize_time_s"]
            encode_t = extracted["encode_time_s"]
            chunk_write_t = extracted["chunk_write_time_s"]
            tiles_total = extracted["tiles_total"]
            tiles_saved = extracted["tiles_saved"]
            tiles_skipped = extracted["tiles_empty_skipped"]
            tiles_skipped_inside = extracted["tiles_empty_skipped_inside_nonempty"]
            metatiles_total = extracted["metatiles_total"]
            metatiles_empty = extracted["metatiles_empty_skipped"]
            chunks_written = extracted["chunks_written"]
            output_bytes = extracted["output_bytes"]
            bytes_before = extracted["bytes_before_compress"]
            bytes_after = extracted["bytes_after_compress"]

        else:
            # Standard map: parse -> build -> cut
            res_parse = task_parse_fields(base_map, run_id, fff, product_name)
            if res_parse.get("skipped"):
                raise RuntimeError(f"Parse skipped: {res_parse.get('reason')}")
            parse_t = res_parse.get("parse_duration_s", 0.0)

            res_build = task_build_canvas(res_parse)
            if res_build.get("skipped"):
                raise RuntimeError("Build skipped.")
            build_t = res_build.get("build_duration_s", 0.0)

            res_cut = task_cut_and_write_tiles(base_map, run_id, fff, product_name, res_build)
            cut_t = res_cut.get("cut_duration_s", 0.0)
            tiles_total = res_cut.get("total", 0)
            tiles_saved = res_cut.get("saved", 0)
            tiles_skipped = res_cut.get("empty_skipped", 0)
            tiles_skipped_inside = res_cut.get("tiles_empty_skipped_inside_nonempty", 0)
            metatiles_total = res_cut.get("metatiles_total", 0)
            metatiles_empty = res_cut.get("metatiles_empty_skipped", 0)
            chunks_written = res_cut.get("chunks_written", 0)
            metatile_extract_t = res_cut.get("metatile_extract_time_s", 0.0)
            colorize_t = res_cut.get("colorize_time_s", 0.0)
            encode_t = res_cut.get("encode_time_s", 0.0)
            chunk_write_t = res_cut.get("chunk_write_time_s", 0.0)
            bytes_before = res_cut.get("bytes_before_compress", 0)
            bytes_after = res_cut.get("bytes_after_compress", 0)

            staging_dir = cfg.STAGING_DIR / base_map / run_id / f"{fff:03d}"
            _, output_bytes = count_output_dir(staging_dir)

        # Publish
        pub_start = time.perf_counter()
        publish_staging_to_live(base_map, run_id)
        publish_t = time.perf_counter() - pub_start

        total_t = time.perf_counter() - start_time
        status = "SUCCESS"

    except Exception as e:
        print(f"  [ERROR] {e}")
        status = f"ERROR: {str(e)}"
        total_t = time.perf_counter() - start_time
        publish_t = 0.0

    metrics = monitor.stop()

    return dict(
        status=status,
        parse_time_s=round(parse_t, 3),
        build_time_s=round(build_t, 3),
        cut_time_s=round(cut_t, 3),
        metatile_extract_time_s=round(metatile_extract_t, 3),
        colorize_time_s=round(colorize_t, 3),
        encode_time_s=round(encode_t, 3),
        chunk_write_time_s=round(chunk_write_t, 3),
        publish_t=publish_t,
        total_time_s=round(total_t, 3),
        tiles_total=tiles_total,
        tiles_saved=tiles_saved,
        tiles_empty_skipped=tiles_skipped,
        tiles_empty_skipped_inside_nonempty=tiles_skipped_inside,
        metatiles_total=metatiles_total,
        metatiles_empty_skipped=metatiles_empty,
        chunks_written=chunks_written,
        output_bytes=output_bytes,
        bytes_before_compress=bytes_before,
        bytes_after_compress=bytes_after,
        **metrics,
    )


# --- 6. MAIN BENCHMARK ---
def run_benchmark():
    # First-frame targets
    FIRST_FRAME_MAPS = [
        "rain_advanced",
        "rain_advanced_base_only",  # Only base PNG, skip field binary
        "rain_basic",
        "temperature_feels_like",
        "snow_depth",
        "wind_surface",
        "wind_base_only",           # Only base PNG tiles
        "wind_field_only",          # Only field binary tiles
    ]

    if _ARGS.maps:
        FIRST_FRAME_MAPS = [m for m in FIRST_FRAME_MAPS if m in _ARGS.maps]

    # Multi-frame targets (3 frames each)
    MULTI_FRAME_MAPS = ["rain_advanced", "wind_surface"]

    results = []

    print("=" * 60)
    print(f"  FIRST-FRAME BENCHMARK  [mode={_ARGS.mode}]")
    print("=" * 60)

    for map_type in FIRST_FRAME_MAPS:
        print(f"\n--- [{map_type}] ---")
        run_id, fff, product_name = find_frame(map_type)
        if fff is None:
            print(f"  [SKIP] No data found")
            results.append({"benchmark_mode": _ARGS.mode, "map_type": map_type, "status": "NO_DATA"})
            continue

        print(f"  run_id={run_id}, fff={fff:03d}, product={product_name}")

        stats = _run_one_frame(map_type, run_id, fff, product_name)
        publish_t = stats.pop("publish_t", 0.0)
        # Extract sys-resource metrics (they were merged into stats by _run_one_frame)
        sys_keys = ["cpu_avg_percent", "cpu_peak_percent", "ram_peak_gb",
                    "io_wait_peak_percent", "read_mb", "write_mb"]
        metrics = {k: stats.pop(k) for k in sys_keys if k in stats}
        status = stats.pop("status", "UNKNOWN")

        row = _build_row(map_type, product_name, run_id, fff, stats, metrics, publish_t, status)
        results.append(row)
        print(f"  [{status}] total={stats.get('total_time_s', 0):.2f}s "
              f"saved={stats.get('tiles_saved', 0)} tiles "
              f"metatiles_skipped={stats.get('metatiles_empty_skipped', 0)}")

    # Multi-frame benchmark
    if _ARGS.multi_frame:
        print("\n" + "=" * 60)
        print("  MULTI-FRAME BENCHMARK (3 frames: near / mid / far)")
        print("=" * 60)

        for map_type in MULTI_FRAME_MAPS:
            if _ARGS.maps and map_type not in _ARGS.maps:
                continue
            fffs = pick_3_frames(map_type)
            if not fffs:
                print(f"\n  [{map_type}] No frames found, skipping")
                continue

            for fff_target in fffs:
                run_id, fff, product_name = find_frame(map_type, target_fff=fff_target)
                if fff is None:
                    print(f"\n  [{map_type}] f{fff_target:03d} not found, skipping")
                    continue
                label = f"{'near' if fff == fffs[0] else 'mid' if fff == fffs[len(fffs)//2] else 'far'}_f{fff:03d}"
                print(f"\n--- [{map_type}] {label} ---")

                stats = _run_one_frame(map_type, run_id, fff, product_name)
                publish_t = stats.pop("publish_t", 0.0)
                sys_keys = ["cpu_avg_percent", "cpu_peak_percent", "ram_peak_gb",
                            "io_wait_peak_percent", "read_mb", "write_mb"]
                metrics = {k: stats.pop(k) for k in sys_keys if k in stats}
                status = stats.pop("status", "UNKNOWN")
                row = _build_row(
                    f"{map_type}_multiframe", label, run_id, fff,
                    stats, metrics, publish_t, status
                )
                results.append(row)
                print(f"  [{status}] total={stats.get('total_time_s', 0):.2f}s "
                      f"saved={stats.get('tiles_saved', 0)} tiles")

    # --- Export ---
    suffix = f"_{_ARGS.mode}"
    csv_file = f"benchmark_first_frame_report{suffix}.csv"
    json_file = f"benchmark_first_frame_summary{suffix}.json"

    # For backwards compatibility, also write the canonical file when in baseline mode
    if _ARGS.mode == "baseline":
        canonical_json = "benchmark_first_frame_summary.json"
    else:
        canonical_json = None

    if results:
        # Collect all keys to support varying result shapes
        all_keys: list = []
        for r in results:
            for k in r:
                if k not in all_keys:
                    all_keys.append(k)
        with open(csv_file, mode="w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(results)

    with open(json_file, "w") as f:
        json.dump({"benchmark_mode": _ARGS.mode, "benchmark_results": results}, f, indent=2)

    if canonical_json:
        import shutil
        shutil.copy(json_file, canonical_json)

    print("\n" + "=" * 60)
    print("Benchmark Complete. Reports generated:")
    print(f"  - {csv_file}")
    print(f"  - {json_file}")
    if canonical_json:
        print(f"  - {canonical_json}  (canonical copy)")
    print("=" * 60)

    return results


def run_scheduler_benchmark():
    from datetime import datetime, timedelta, timezone

    # Sequence of jobs defined by the user for scheduler simulation
    SCHEDULER_SEQUENCE = [
        ("rain_advanced", "near"),
        ("rain_basic", "near"),
        ("temperature_feels_like", "near"), # maps to temperature_near
        ("snow_depth", "near"),             # maps to snow_near
        ("wind_surface", "near"),           # maps to wind_near
        ("temperature_feels_like", "mid"),  # maps to temperature_mid
        ("snow_depth", "mid"),              # maps to snow_mid
        ("wind_surface", "mid"),            # maps to wind_mid
        ("snow_depth", "far"),              # maps to snow_far
        ("wind_surface", "far"),            # maps to wind_far
    ]

    print("=" * 60)
    print(f"  SCHEDULER REALISTIC BENCHMARK  [mode={_ARGS.mode}]")
    print("=" * 60)

    results = []
    
    # 1. Resolve actual FFFs for each job
    resolved_jobs = []
    for base_map, label in SCHEDULER_SEQUENCE:
        map_dir = cfg.DATA_DIR / base_map
        if not map_dir.exists():
            continue
        runs = sorted([d.name for d in map_dir.iterdir() if d.is_dir()], reverse=True)
        if not runs:
            continue
        latest_run = runs[0]
        
        products = _MAP_PRODUCTS.get(base_map, [])
        product_name = products[0] if products else None
        prod_dir = map_dir / latest_run / (product_name or "")
        
        if not prod_dir.exists():
            subdirs = [d for d in (map_dir / latest_run).iterdir() if d.is_dir()]
            if not subdirs:
                continue
            prod_dir = subdirs[0]
            product_name = prod_dir.name
            
        fffs = sorted({int(g.stem[1:]) for g in prod_dir.glob("f*.grib2") if g.stem[1:].isdigit()})
        if not fffs:
            continue
            
        if label == "near":
            target_fff = fffs[0]
        elif label == "mid":
            target_fff = fffs[len(fffs) // 2]
        else: # far
            target_fff = fffs[-1]
            
        try:
            run_dt = datetime.strptime(latest_run, "%Y%m%d%H").replace(tzinfo=timezone.utc)
        except Exception:
            run_dt = datetime.now(timezone.utc)
            
        valid_time_dt = run_dt + timedelta(hours=target_fff)
        
        # Synthetic NOAA download timestamps to create a realistic queue simulation
        # run_dt = 00Z. raw is available around run_dt + 3.5h + fff*1m
        base_avail = run_dt + timedelta(hours=3, minutes=30)
        raw_avail_dt = base_avail + timedelta(minutes=target_fff)
        
        # job created at run_dt + 3h (we eagerly create the job before raw is ready)
        job_created_dt = run_dt + timedelta(hours=3)
        
        # In our simulation clock, T0 starts when the FIRST job is created.
        # But to keep latency numbers human-readable in seconds, we will map these datetimes to UNIX seconds.
        resolved_jobs.append({
            "map_type": base_map,
            "label": label,
            "run_id": latest_run,
            "fff": target_fff,
            "product_name": product_name,
            "valid_time_dt": valid_time_dt,
            "raw_available_dt": raw_avail_dt,
            "job_created_dt": job_created_dt
        })

    if not resolved_jobs:
        print("No valid jobs found for scheduler simulation.")
        return []

    # Initialize simulation clock (Unix timestamp)
    # We set the simulation clock to the earliest job_created_dt
    earliest_created = min(j["job_created_dt"] for j in resolved_jobs)
    sim_clock = earliest_created.timestamp()
    
    last_finished_job_id = None
    last_finished_map_type = None

    for j in resolved_jobs:
        map_type = j["map_type"]
        run_id = j["run_id"]
        fff = j["fff"]
        job_id = f"{map_type}_{run_id}_f{fff:03d}"
        
        print(f"\n--- [Scheduler] {job_id} ({j['label']}) ---")
        
        job_created_s = j["job_created_dt"].timestamp()
        raw_available_s = j["raw_available_dt"].timestamp()
        valid_time_s = j["valid_time_dt"].timestamp()
        
        # 2. Scheduler loop wait
        # The scheduler ticks, sees job created.
        # It must wait until sim_clock reaches raw_available_s
        if sim_clock < raw_available_s:
            print(f"  [Queue] Waiting for raw NOAA data ({raw_available_s - sim_clock:.1f}s)")
            sim_clock = raw_available_s
            
        job_eligible_at = sim_clock
        job_started_at = sim_clock
        
        # Record blocked by info
        blocked_by_job_id = last_finished_job_id if last_finished_job_id else ""
        blocked_by_map_type = last_finished_map_type if last_finished_map_type else ""
        
        # 3. Process
        stats = _run_one_frame(map_type, run_id, fff, j["product_name"])
        
        processing_time_s = stats.get("total_time_s", 0.0)
        job_finished_at = job_started_at + processing_time_s
        
        publish_time_s = stats.get("publish_t", 0.0)
        job_published_at = job_finished_at + publish_time_s
        
        # Update simulation clock
        sim_clock = job_published_at
        
        # 4. Calculate Latencies
        raw_wait_s = raw_available_s - job_created_s
        queue_wait_s = job_started_at - job_eligible_at
        end_to_end_s = job_published_at - job_created_s
        ready_after_raw_s = job_published_at - raw_available_s
        lateness_s = job_published_at - valid_time_s
        usable = lateness_s < 0
        
        status = stats.get("status", "UNKNOWN")
        
        # Construct output row
        row = {
            "benchmark_mode": _ARGS.mode,
            "run_id": run_id,
            "job_id": job_id,
            "map_type": map_type,
            "block_type": "sequential", # Always sequential in this sim
            "frame_label": f"{j['label']}_f{fff:03d}",
            "source_run": run_id,
            "source_fff": fff,
            "valid_time": j["valid_time_dt"].isoformat(),
            "job_created_at": j["job_created_dt"].isoformat(),
            "raw_available_at": j["raw_available_dt"].isoformat(),
            "job_eligible_at": datetime.fromtimestamp(job_eligible_at, timezone.utc).isoformat(),
            "job_started_at": datetime.fromtimestamp(job_started_at, timezone.utc).isoformat(),
            "job_finished_at": datetime.fromtimestamp(job_finished_at, timezone.utc).isoformat(),
            "job_published_at": datetime.fromtimestamp(job_published_at, timezone.utc).isoformat(),
            "raw_wait_s": round(raw_wait_s, 2),
            "queue_wait_s": round(queue_wait_s, 2),
            "processing_s": round(processing_time_s, 2),
            "publish_s": round(publish_time_s, 2),
            "end_to_end_s": round(end_to_end_s, 2),
            "ready_after_raw_s": round(ready_after_raw_s, 2),
            "lateness_s": round(lateness_s, 2),
            "usable": usable,
            "blocked_by_job_id": blocked_by_job_id,
            "blocked_by_map_type": blocked_by_map_type,
            "status": status
        }
        
        results.append(row)
        
        last_finished_job_id = job_id
        last_finished_map_type = map_type
        
        print(f"  [{status}] end_to_end={end_to_end_s:.2f}s "
              f"ready_after_raw={ready_after_raw_s:.2f}s lateness={lateness_s:.2f}s usable={usable}")

    # Export
    csv_file = "benchmark_scheduler_latency_report.csv"
    if results:
        all_keys = list(results[0].keys())
        with open(csv_file, mode="w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(results)
            
    print("\n" + "=" * 60)
    print("Scheduler Benchmark Complete. Report generated:")
    print(f"  - {csv_file}")
    print("=" * 60)
    
    return results


def _count_tile_files(directory: Path) -> tuple:
    """Return (file_count, total_bytes) for all files under directory."""
    count = total = 0
    if not directory.exists():
        return 0, 0
    for f in directory.rglob("*"):
        if f.is_file():
            count += 1
            try:
                total += f.stat().st_size
            except OSError:
                pass
    return count, total


def _hardlink_dir(src: Path, dst: Path) -> tuple:
    """
    Hardlink all files from src tree into dst tree.
    Returns (files_linked, logical_bytes, elapsed_s).
    logical_bytes = sum of source file sizes (hardlinks add 0 real disk space
    on the same filesystem, but the logical size is still meaningful for comparison).
    """
    import shutil as _shutil
    t0 = time.perf_counter()
    linked = 0
    logical_bytes = 0
    for src_file in src.rglob("*"):
        if not src_file.is_file():
            continue
        dst_file = dst / src_file.relative_to(src)
        dst_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(src_file, dst_file)
        except OSError:
            _shutil.copy2(src_file, dst_file)
        linked += 1
        try:
            logical_bytes += src_file.stat().st_size
        except OSError:
            pass
    return linked, logical_bytes, round(time.perf_counter() - t0, 4)


def run_cold_zone_benchmark():
    """
    Mode: cold_zone
    For each map type that has a cold zone (cold_fff_min < 9999), test N frames
    from both the hot zone and the cold zone.

    Per frame:
      GENERATE  — run the full tile generation pipeline (parse → build → cut → publish).
                  Records wall-clock time, tiles written, and bytes on disk.
      HARDLINK  — copy those tiles to a throwaway run_id via os.link().
                  Records wall-clock time and logical bytes (hardlinks use 0 extra
                  disk space on the same filesystem).

    The speedup ratio (generate / hardlink) shows how much faster a cold-zone
    cycle is compared to a full re-generation.

    Usage:
      python -m app.benchmark --mode cold_zone
      python -m app.benchmark --mode cold_zone --maps wind_surface snow_depth
      python -m app.benchmark --mode cold_zone --cold-frames 2
    """
    import shutil as _shutil
    from app.core.map_specs import MAP_SPECS, segment_fff

    n_per_zone = max(1, _ARGS.cold_frames)

    maps_with_cold = [
        m for m in MAP_SPECS
        if MAP_SPECS[m].cold_fff_min < 9999
    ]
    if _ARGS.maps:
        maps_with_cold = [m for m in maps_with_cold if m in _ARGS.maps]

    if not maps_with_cold:
        print("No maps with cold zones found (or --maps filter excluded them).")
        return []

    results = []

    print("=" * 72)
    print(f"  COLD ZONE BENCHMARK  [mode={_ARGS.mode}, cold_frames={n_per_zone}]")
    print("  Compares: generate from scratch  vs  hardlink from previous cycle")
    print("  Hardlinks use 0 extra disk space (same inode, same filesystem)")
    print("=" * 72)

    import datetime as _dt
    _log_ts = _dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    _log_path = Path(f"benchmark_cold_zone_{_log_ts}.log")
    _log_fh = open(_log_path, "w", buffering=1)

    def czlog(msg: str) -> None:
        ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        _log_fh.write(f"[{ts}] {msg}\n")

    czlog(f"COLD ZONE BENCHMARK START  n_per_zone={n_per_zone}  maps={','.join(maps_with_cold)}")

    for map_type in maps_with_cold:
        spec = MAP_SPECS[map_type]
        fffs_all = segment_fff(spec.fff_segments_full)
        hot_fffs = [f for f in fffs_all if f < spec.cold_fff_min]
        cold_fffs = [f for f in fffs_all if f >= spec.cold_fff_min]

        print(f"\n{'─'*72}")
        print(f"  [{map_type}]")
        print(f"    cold_fff_min = {spec.cold_fff_min}   cold_max_age = {spec.cold_max_age_h}h")
        if hot_fffs:
            print(f"    Hot  zone : {len(hot_fffs)} frames  "
                  f"f{hot_fffs[0]:03d}–f{hot_fffs[-1]:03d}  → always generated")
        if cold_fffs:
            print(f"    Cold zone : {len(cold_fffs)} frames  "
                  f"f{cold_fffs[0]:03d}–f{cold_fffs[-1]:03d}  → hardlinked at non-refresh cycles")

        test_cases: list[tuple[str, int]] = []
        for fff in hot_fffs[:n_per_zone]:
            test_cases.append(("hot", fff))
        for fff in cold_fffs[:n_per_zone]:
            test_cases.append(("cold", fff))

        for zone, target_fff in test_cases:
            print(f"\n  ┌── f{target_fff:03d}  [{zone}] ──")

            run_id, fff, product_name = find_frame(map_type, target_fff)
            if fff is None:
                print(f"  │   [SKIP] No GRIB data found for f{target_fff:03d}")
                continue

            print(f"  │   run_id={run_id}  fff={fff:03d}  product={product_name}")

            # ── GENERATE ──────────────────────────────────────────────────
            print("  │   Generating tiles…", end=" ", flush=True)
            czlog(f"[{map_type}] f{target_fff:03d} {zone}: GENERATE start  run={run_id}")
            gen_stats = _run_one_frame(map_type, run_id, fff, product_name)
            gen_status = gen_stats.get("status", "UNKNOWN")
            gen_time_s = gen_stats.get("total_time_s", 0.0)
            gen_tiles = gen_stats.get("tiles_saved", 0)

            # Measure actual on-disk bytes after publishing
            tile_src = cfg.TILES_DIR / map_type / run_id / f"{fff:03d}"
            gen_files, gen_bytes = _count_tile_files(tile_src)
            gen_mb = gen_bytes / (1024 ** 2)

            print(f"{gen_time_s:.2f}s  →  {gen_files} files  {gen_mb:.2f} MB  [{gen_status}]")
            czlog(f"[{map_type}] f{fff:03d} {zone}: GENERATE done  {gen_time_s:.3f}s  {gen_files} files  {gen_mb:.2f}MB  [{gen_status}]")

            if gen_status.startswith("ERROR") or not tile_src.exists():
                results.append({
                    "map_type": map_type, "fff": fff, "zone": zone,
                    "cold_fff_min": spec.cold_fff_min,
                    "gen_status": gen_status, "gen_time_s": gen_time_s,
                    "gen_files": gen_files, "gen_mb": round(gen_mb, 3),
                    "link_time_s": None, "link_files": None,
                    "link_logical_mb": None, "extra_disk_mb": None,
                    "speedup_x": None,
                })
                continue

            # ── HARDLINK ─────────────────────────────────────────────────
            fake_run = f"_czb_{map_type[:8]}_{fff:03d}"
            tile_dst = cfg.TILES_DIR / map_type / fake_run / f"{fff:03d}"
            # Ensure clean slate
            fake_run_dir = cfg.TILES_DIR / map_type / fake_run
            if fake_run_dir.exists():
                _shutil.rmtree(fake_run_dir, ignore_errors=True)

            print("  │   Hardlinking tiles…", end=" ", flush=True)
            link_files, link_logical_bytes, link_time_s = _hardlink_dir(tile_src, tile_dst)

            # Extra disk space = 0 if same filesystem (hardlinks share inode).
            # Detect by checking if st_nlink > 1 for any linked file.
            extra_disk_bytes = 0
            try:
                sample = next(tile_dst.rglob("*") if tile_dst.exists() else iter([]), None)
                if sample and sample.is_file():
                    if sample.stat().st_nlink < 2:
                        # Fell back to copy (cross-device) — actual bytes used
                        extra_disk_bytes = link_logical_bytes
            except Exception:
                pass

            link_logical_mb = link_logical_bytes / (1024 ** 2)
            extra_disk_mb = extra_disk_bytes / (1024 ** 2)
            speedup = round(gen_time_s / link_time_s, 1) if link_time_s > 0 else None

            print(f"{link_time_s:.4f}s  →  {link_files} files  "
                  f"logical={link_logical_mb:.2f} MB  extra_disk={extra_disk_mb:.2f} MB  "
                  f"speedup={speedup}×")
            czlog(f"[{map_type}] f{fff:03d} {zone}: HARDLINK done  {link_time_s:.5f}s  {link_files} files  logical={link_logical_mb:.2f}MB  extra_disk={extra_disk_mb:.2f}MB  speedup={speedup}x")

            # Clean up fake run
            if fake_run_dir.exists():
                _shutil.rmtree(fake_run_dir, ignore_errors=True)

            print(f"  └── speedup: {speedup}× faster  "
                  f"({'no' if extra_disk_mb == 0 else str(round(extra_disk_mb,2))+' MB'} extra disk)")

            results.append({
                "map_type": map_type,
                "fff": fff,
                "zone": zone,
                "cold_fff_min": spec.cold_fff_min,
                "gen_status": gen_status,
                "gen_time_s": gen_time_s,
                "gen_files": gen_files,
                "gen_mb": round(gen_mb, 3),
                "link_time_s": link_time_s,
                "link_files": link_files,
                "link_logical_mb": round(link_logical_mb, 3),
                "extra_disk_mb": round(extra_disk_mb, 3),
                "speedup_x": speedup,
            })

    # ── SUMMARY TABLE ────────────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("  SUMMARY")
    print(f"  {'MAP':<28} {'FFF':>5} {'ZONE':>5} "
          f"{'GEN':>8} {'LINK':>8} {'SPEEDUP':>9} {'EXTRA DISK':>11}")
    print("  " + "─" * 70)
    for r in results:
        speedup_str = f"{r['speedup_x']}×" if r["speedup_x"] else "—"
        disk_str = f"{r['extra_disk_mb']} MB" if r.get("extra_disk_mb") is not None else "—"
        gen_str = f"{r['gen_time_s']:.2f}s" if r.get("gen_time_s") else "—"
        link_str = f"{r['link_time_s']:.4f}s" if r.get("link_time_s") else "—"
        print(f"  {r['map_type']:<28} f{r['fff']:03d}  {r['zone']:>5}  "
              f"{gen_str:>8}  {link_str:>8}  {speedup_str:>9}  {disk_str:>11}")

    # ── DETAILED DAILY PROJECTION (per-cycle per-map, multi-tier aware) ────────
    from app.core.map_specs import (MAP_SPECS as _MSPECS, segment_fff as _seg,
                                     tier_max_age_for_fff as _tmaf,
                                     tier_info_for_fff as _tiaf, tier_frame_groups as _tfg)
    import datetime as _dt2

    daily_projection: dict = {}
    czlog("=== DAILY PROJECTION ===")

    for map_type in maps_with_cold:
        spec = _MSPECS[map_type]
        fffs_all   = _seg(spec.fff_segments_full)
        hot_fffs   = [f for f in fffs_all if _tmaf(spec, f) is None]
        cold_fffs_list = [f for f in fffs_all if _tmaf(spec, f) is not None]
        n_hot  = len(hot_fffs)
        n_cold = len(cold_fffs_list)
        cycles_per_day = 4

        hot_t      = next((r["gen_time_s"]  for r in results if r["map_type"] == map_type and r["zone"] == "hot"),  None)
        cold_gen_t = next((r["gen_time_s"]  for r in results if r["map_type"] == map_type and r["zone"] == "cold"), None)
        cold_link_t= next((r["link_time_s"] for r in results if r["map_type"] == map_type and r["zone"] == "cold"), None)
        gen_mb     = next((r["gen_mb"]      for r in results if r["map_type"] == map_type and r["zone"] == "cold"),
                          next((r["gen_mb"] for r in results if r["map_type"] == map_type), None))
        speedup_x  = next((r["speedup_x"]  for r in results if r["map_type"] == map_type and r["zone"] == "cold"), None)

        if not (hot_t and cold_gen_t and cold_link_t):
            czlog(f"  [{map_type}] SKIP — incomplete measurements")
            continue

        # Precompute stagger groups for this spec (stable indices)
        _all_fffs = _seg(spec.fff_segments_full)
        _frame_grps = _tfg(spec, _all_fffs)
        cycle_detail = []
        for hour, label in [(0, "00z"), (6, "06z"), (12, "12z"), (18, "18z")]:
            # Per-frame: count how many cold frames are gen vs link this cycle
            _slot = hour // 6
            def _should_gen(f):
                info = _tiaf(spec, f)
                if info is None: return False  # hot
                max_age_h, stagger_n = info
                if stagger_n > 1:
                    return _slot % stagger_n == _frame_grps.get(f, 0)
                return hour % max_age_h == 0
            n_cold_gen  = sum(1 for f in cold_fffs_list if _should_gen(f))
            n_cold_link = n_cold - n_cold_gen
            all_refresh = (n_cold_link == 0)
            all_link    = (n_cold_gen  == 0)
            action = "full_gen" if all_refresh else ("gen_and_link" if n_cold_link > 0 else "gen_and_link")

            hot_time   = round(n_hot       * hot_t,      1)
            cold_g_t   = round(n_cold_gen  * cold_gen_t, 1)
            cold_l_t   = round(n_cold_link * cold_link_t, 4)
            total_time = round(hot_time + cold_g_t + cold_l_t, 1)
            storage_mb = round((n_hot + n_cold_gen) * (gen_mb or 0), 1)

            cycle_detail.append({
                "cycle": label, "hour": hour, "action": action,
                "hot_frames_gen":   n_hot,
                "cold_frames_gen":  n_cold_gen,
                "cold_frames_link": n_cold_link,
                "hot_time_s":       hot_time,
                "cold_gen_time_s":  cold_g_t,
                "cold_link_time_s": cold_l_t,
                "cold_time_s":      round(cold_g_t + cold_l_t, 2),
                "total_time_s":     total_time,
                "storage_written_mb": storage_mb,
            })
            czlog(f"  [{map_type}] {label}: {action}  "
                  f"hot={n_hot}fr/{hot_time:.0f}s  "
                  f"cold_gen={n_cold_gen}fr/{cold_g_t:.0f}s  "
                  f"cold_link={n_cold_link}fr/{cold_l_t:.1f}s  "
                  f"total={total_time:.0f}s  storage={storage_mb:.0f}MB")

        # Cold tier summary for reporting
        tiers_summary = []
        for ti, t in enumerate(spec.cold_tiers):
            fff_min, max_age_h = t[0], t[1]
            stagger_n = t[2] if len(t) > 2 else 1
            next_min = spec.cold_tiers[ti + 1][0] if ti + 1 < len(spec.cold_tiers) else None
            cnt = sum(1 for f in cold_fffs_list if f >= fff_min and (next_min is None or f < next_min))
            tiers_summary.append({"fff_min": fff_min, "max_age_h": max_age_h, "stagger_n": stagger_n, "frame_count": cnt})

        before_s  = round((n_hot * hot_t + n_cold * cold_gen_t) * cycles_per_day, 1)
        after_s   = round(sum(c["total_time_s"] for c in cycle_detail), 1)
        saved_s   = round(before_s - after_s, 1)
        saved_pct = round(saved_s / before_s * 100, 1) if before_s > 0 else 0
        stor_before = round((n_hot + n_cold) * (gen_mb or 0) * cycles_per_day, 1)
        stor_after  = round(sum(c["storage_written_mb"] for c in cycle_detail), 1)

        daily_projection[map_type] = {
            "n_hot": n_hot, "n_cold": n_cold,
            "hot_range":  f"f{hot_fffs[0]:03d}–f{hot_fffs[-1]:03d}"       if hot_fffs       else None,
            "cold_range": f"f{cold_fffs_list[0]:03d}–f{cold_fffs_list[-1]:03d}" if cold_fffs_list else None,
            "cold_tiers": tiers_summary,
            "hot_gen_t_s":   round(hot_t,       3),
            "cold_gen_t_s":  round(cold_gen_t,  3),
            "cold_link_t_s": round(cold_link_t, 6),
            "gen_mb_per_frame": round(gen_mb, 3) if gen_mb else None,
            "speedup_x": speedup_x,
            "cycles": cycle_detail,
            "daily": {
                "before_opt_s":   before_s,
                "after_opt_s":    after_s,
                "saved_s":        saved_s,
                "saved_pct":      saved_pct,
                "saved_h":        round(saved_s / 3600, 3),
                "hardlink_extra_disk_mb": 0,
                "storage_per_day_before_mb": stor_before,
                "storage_per_day_after_mb":  stor_after,
                "storage_saved_mb": round(stor_before - stor_after, 1),
            }
        }
        czlog(f"  [{map_type}] DAILY TOTAL: before={before_s:.0f}s  after={after_s:.0f}s  "
              f"saved={saved_s:.0f}s ({saved_pct}%)  storage: {stor_before:.0f}→{stor_after:.0f}MB")

    # Grand total across all maps
    grand_before  = sum(dp["daily"]["before_opt_s"]            for dp in daily_projection.values())
    grand_after   = sum(dp["daily"]["after_opt_s"]             for dp in daily_projection.values())
    grand_saved   = round(grand_before - grand_after, 1)
    g_stor_before = sum(dp["daily"]["storage_per_day_before_mb"] for dp in daily_projection.values())
    g_stor_after  = sum(dp["daily"]["storage_per_day_after_mb"]  for dp in daily_projection.values())
    grand_total = {
        "before_opt_daily_s":  round(grand_before, 1),
        "after_opt_daily_s":   round(grand_after,  1),
        "saved_daily_s":       grand_saved,
        "saved_daily_pct":     round(grand_saved / grand_before * 100, 1) if grand_before > 0 else 0,
        "saved_daily_h":       round(grand_saved / 3600, 2),
        "storage_before_mb":   round(g_stor_before, 1),
        "storage_after_mb":    round(g_stor_after,  1),
        "storage_saved_mb":    round(g_stor_before - g_stor_after, 1),
    }
    czlog(f"GRAND TOTAL: before={grand_before:.0f}s  after={grand_after:.0f}s  "
          f"saved={grand_saved:.0f}s ({grand_total['saved_daily_pct']}%)  "
          f"={grand_total['saved_daily_h']:.2f}h/day  "
          f"storage: {g_stor_before:.0f}→{g_stor_after:.0f}MB/day")

    # ── PRINT SUMMARY TABLE ──────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print("  SUMMARY  (per-frame measurements)")
    print(f"  {'MAP':<28} {'FFF':>5} {'ZONE':>5} "
          f"{'GEN':>8} {'LINK':>8} {'SPEEDUP':>9} {'EXTRA DISK':>11}")
    print("  " + "─" * 70)
    for r in results:
        speedup_str = f"{r['speedup_x']}×" if r["speedup_x"] else "—"
        disk_str  = f"{r['extra_disk_mb']} MB" if r.get("extra_disk_mb") is not None else "—"
        gen_str   = f"{r['gen_time_s']:.2f}s"  if r.get("gen_time_s")  else "—"
        link_str  = f"{r['link_time_s']:.4f}s" if r.get("link_time_s") else "—"
        print(f"  {r['map_type']:<28} f{r['fff']:03d}  {r['zone']:>5}  "
              f"{gen_str:>8}  {link_str:>8}  {speedup_str:>9}  {disk_str:>11}")

    # ── PRINT DETAILED DAILY PROJECTION ─────────────────────────────────────
    print("\n  DETAILED DAILY PROJECTION  (measured per-frame × actual frame counts)")
    print(f"  {'MAP':<26} {'CYCLE':>6} {'ACTION':>12} "
          f"{'HOT(s)':>8} {'COLD(s)':>9} {'TOTAL(s)':>9} {'STORAGE':>9}")
    print("  " + "─" * 84)
    for map_type, dp in daily_projection.items():
        for i, c in enumerate(dp["cycles"]):
            label_col = map_type if i == 0 else ""
            action_str = "full GEN" if c["action"] == "full_gen" else "GEN+link"
            print(f"  {label_col:<26} {c['cycle']:>6}  {action_str:>12}  "
                  f"{c['hot_time_s']:>7.0f}s  {c['cold_time_s']:>8.1f}s  "
                  f"{c['total_time_s']:>8.0f}s  {c['storage_written_mb']:>7.0f}MB")
        d = dp["daily"]
        print(f"  {'':26} {'─ DAY ─':>6}  {'before':>12}  "
              f"{d['before_opt_s']:>7.0f}s  {'after':>8}  {d['after_opt_s']:>8.0f}s  "
              f"saved {d['saved_pct']}%")
        print("  " + "─" * 84)
    if grand_total["before_opt_daily_s"] > 0:
        print(f"  {'GRAND TOTAL':<26} {'ALL':>6}  {'before':>12}  "
              f"{grand_total['before_opt_daily_s']:>7.0f}s  {'after':>8}  "
              f"{grand_total['after_opt_daily_s']:>8.0f}s  "
              f"saved {grand_total['saved_daily_pct']}% = {grand_total['saved_daily_h']:.2f}h/day")

    # ── EXPORT ───────────────────────────────────────────────────────────────
    csv_file  = "benchmark_cold_zone_report.csv"
    json_file = "benchmark_cold_zone_report.json"
    if results:
        all_keys = list(results[0].keys())
        with open(csv_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=all_keys)
            writer.writeheader()
            writer.writerows(results)
    report = {
        "mode":             "cold_zone",
        "generated_at":     _dt2.datetime.now().isoformat(),
        "n_per_zone":       n_per_zone,
        "results":          results,
        "daily_projection": daily_projection,
        "grand_total":      grand_total,
    }
    with open(json_file, "w") as f:
        json.dump(report, f, indent=2)

    czlog(f"REPORTS SAVED: {json_file}  {csv_file}  {_log_path}")
    _log_fh.close()

    print(f"\n  Reports: {csv_file}, {json_file}")
    print(f"  Log:     {_log_path}")
    print("=" * 72)
    return results


def run_predict_benchmark():
    """Predict mode: estimate frame counts, tile counts, and storage without running generation.

    Uses map_specs FFF segments to compute exact frame counts, then applies
    benchmark-calibrated per-tile size estimates (WebP-adjusted from baseline PNG data).
    """
    from app.core.map_specs import MAP_SPECS, segment_fff

    zoom = getattr(_ARGS, "zoom", 8)

    # At z=Z, there are (2^Z)^2 tiles covering the full globe.
    # Weather data is sparse: empirically ~30-55% of tiles are non-empty at z=8.
    # Values calibrated from benchmark_first_frame_summary.json baseline:
    #   rain_advanced: 26733/65536 = 40.8% fill
    #   wind_surface:  87379/(65536*2) = 66.7% fill (wind_base + wind_field separate)
    #   rain_basic:    38407/65536 = 58.6%
    #   temperature:   21845/65536 = 33.3%
    #   snow_depth:    13229/65536 = 20.2%
    total_z8_tiles = (2 ** zoom) ** 2  # = 65536 at z=8

    # (fill_rate, webp_bytes_per_tile, note)
    # webp_bytes estimated from: baseline_png_bytes × 0.45 (typical WebP saving for weather)
    TILE_PROFILE = {
        "rain_advanced": (0.408, 6500,   "precip_base WebP (classify-once; was ~14.5KB PNG)"),
        "rain_basic":    (0.586, 1800,   "apcp_surface WebP (banded, very compressible)"),
        "temperature_feels_like": (0.333, 3900, "tmp_2m WebP (continuous gradient)"),
        "snow_depth":    (0.202, 1600,   "snod WebP (banded, seasonal coverage)"),
        "wind_surface":  (0.408, 8500,   "wind_base WebP + wind_field WFLD combined"),
    }

    # rain_advanced interpolated sub-frames: 27 frames at 15-min covering f006_15..f014_45
    # (sliding "now" support: 9 anchor pairs × 3 sub-frames each)
    INTERP_FRAMES = {
        "rain_advanced": 27,
    }

    rolling_buffer_h = 12  # CYCLE_INTERVAL(6) + NOAA_UPLOAD(5) + PROC_BUFFER(1)

    print("=" * 72)
    print(f"  PREDICT MODE  [zoom=z{zoom}, tile_grid={total_z8_tiles:,} total, webp_default=true]")
    print(f"  Rolling buffer: {rolling_buffer_h}h beyond user window (for all maps)")
    print("=" * 72)
    print()

    all_maps = list(MAP_SPECS.keys())
    if getattr(_ARGS, "maps", None):
        all_maps = [m for m in all_maps if m in _ARGS.maps]

    summary_rows = []
    grand_storage = 0.0
    grand_frames = 0

    for map_type in all_maps:
        spec = MAP_SPECS[map_type]
        fffs = segment_fff(spec.fff_segments_full)
        n_standard = len(fffs)
        n_interp = INTERP_FRAMES.get(map_type, 0)
        n_total = n_standard + n_interp

        profile = TILE_PROFILE.get(map_type, (0.40, 4000, "estimate"))
        fill_rate, bytes_per_tile, note = profile

        # Scale tile counts if zoom differs from z=8 baseline
        if zoom != 8:
            scale = ((2 ** zoom) / (2 ** 8)) ** 2
            fill_rate_adj = fill_rate  # fill_rate stays roughly constant with zoom
        else:
            scale = 1.0
            fill_rate_adj = fill_rate

        active_tiles_per_frame = int(total_z8_tiles * scale * fill_rate_adj)
        total_active_tiles = active_tiles_per_frame * n_total
        storage_bytes = total_active_tiles * bytes_per_tile
        storage_mb = storage_bytes / (1024 ** 2)
        storage_gb = storage_bytes / (1024 ** 3)

        # Time estimate based on per-tile processing rate (from baseline benchmarks)
        # rain_advanced with classify-once: ~60s/frame (was 1600s), others ~30-50s/frame
        if map_type == "rain_advanced":
            sec_per_frame = 65.0   # classify-once speedup
        elif map_type == "wind_surface":
            sec_per_frame = 290.0  # wind_base + wind_field together
        elif map_type in ("rain_basic", "temperature_feels_like"):
            sec_per_frame = 45.0
        elif map_type == "snow_depth":
            sec_per_frame = 30.0
        else:
            sec_per_frame = 40.0

        est_total_s = n_total * sec_per_frame
        est_total_h = est_total_s / 3600

        print(f"  [{map_type}]")
        print(f"    Segments:        {spec.fff_segments_full}")
        print(f"    Standard frames: {n_standard}")
        if n_interp:
            print(f"    Interp frames:   +{n_interp} (15-min sub-frames)")
        print(f"    Total frames:    {n_total}")
        print(f"    Tiles/frame:     ~{active_tiles_per_frame:,} active  "
              f"(fill={fill_rate_adj*100:.0f}% of {int(total_z8_tiles*scale):,})")
        print(f"    Total tiles:     ~{total_active_tiles:,}")
        print(f"    Storage:         ~{storage_mb:.0f} MB  (~{storage_gb:.1f} GB)")
        print(f"    Est. gen time:   ~{est_total_s:.0f}s total  (~{est_total_h:.1f}h)")
        print(f"    Note:            {note}")
        print()

        grand_storage += storage_bytes
        grand_frames += n_total
        summary_rows.append({
            "map_type": map_type,
            "standard_frames": n_standard,
            "interp_frames": n_interp,
            "total_frames": n_total,
            "active_tiles_per_frame": active_tiles_per_frame,
            "total_active_tiles": total_active_tiles,
            "storage_mb": round(storage_mb, 1),
            "storage_gb": round(storage_gb, 2),
            "est_gen_time_s": round(est_total_s, 0),
            "bytes_per_tile_estimate": bytes_per_tile,
            "tile_fill_rate": fill_rate,
            "note": note,
        })

    print("─" * 72)
    grand_mb = grand_storage / (1024 ** 2)
    grand_gb = grand_storage / (1024 ** 3)
    print(f"  TOTAL across all maps:")
    print(f"    Frames:   {grand_frames}")
    print(f"    Storage:  {grand_mb:.0f} MB  ({grand_gb:.1f} GB)")
    print()
    print("  Assumptions:")
    print(f"    - Zoom fixed at z={zoom} (no lazy generation above this)")
    print(f"    - Default format: webp (quality=85)")
    print(f"    - Rolling buffer: {rolling_buffer_h}h already included in frame counts")
    print(f"    - rain_advanced interp frames share classify_on_mercator from source GRIBs")
    print("=" * 72)

    # Save to JSON
    predict_file = "benchmark_predict_report.json"
    with open(predict_file, "w") as f:
        json.dump({
            "mode": "predict",
            "zoom": zoom,
            "rolling_buffer_h": rolling_buffer_h,
            "grand_total_frames": grand_frames,
            "grand_total_storage_mb": round(grand_mb, 1),
            "grand_total_storage_gb": round(grand_gb, 2),
            "maps": summary_rows,
        }, f, indent=2)
    print(f"  Report saved: {predict_file}")
    return summary_rows


if __name__ == "__main__":
    if _ARGS.mode == "predict":
        run_predict_benchmark()
    elif _ARGS.mode == "scheduler_realistic":
        run_scheduler_benchmark()
    elif _ARGS.mode == "cold_zone":
        run_cold_zone_benchmark()
    else:
        run_benchmark()

import logging
import time
import shutil
from pathlib import Path
from ..config import get_settings
from ..core.grib_reader import read_first_field, read_multi_fields, GribField
from ..core.shared_mem import SharedCanvas

log = logging.getLogger(__name__)



# ---------------------------------------------------------------------------
# CUSTOM MAP TASK — rain_advanced / cloud_total / wind_surface
# These use multi-field pipelines — bypass the Parse→Build→Cut stages entirely.
# ---------------------------------------------------------------------------
def task_generate_custom_frame(map_type: str, run_id: str, fff: int, product_name: str) -> dict:
    """Generate tiles for custom map types using their dedicated pipelines.

    Called from the parse worker when map_type is in _CUSTOM_MAP_TYPES.
    Writes tiles directly to the staging directory and returns a summary.
    """
    cfg = get_settings()

    output_dir = cfg.STAGING_DIR / map_type / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    start_t = time.perf_counter()
    result = None

    try:
        if map_type == "rain_advanced":
            from ..core.precip_pipeline import generate_precip_frame
            result = generate_precip_frame(
                run_id=run_id, fff=fff,
                data_dir=cfg.DATA_DIR, output_dir=output_dir,
                zoom_min=0, zoom_max=cfg.TILE_ZOOM_EAGER_MAX,
                workers=cfg.TILE_WORKERS, skip_existing=True,
            )

        elif map_type == "cloud_total":
            from ..core.cloud_pipeline import generate_cloud_frame
            result = generate_cloud_frame(
                run_id=run_id, fff=fff,
                data_dir=cfg.DATA_DIR, output_dir=output_dir,
                zoom_min=0, zoom_max=cfg.TILE_ZOOM_EAGER_MAX,
                workers=cfg.TILE_WORKERS, skip_existing=True,
            )

        elif map_type == "wind_surface":
            from ..core.wind_pipeline import generate_wind_frame
            result = generate_wind_frame(
                run_id=run_id, fff=fff,
                data_dir=cfg.DATA_DIR, output_dir=output_dir,
                zoom_min=0, zoom_max=cfg.TILE_ZOOM_EAGER_MAX,
                workers=cfg.TILE_WORKERS, skip_existing=True,
            )
        else:
            return {"skipped": True, "reason": f"Unknown custom map_type: {map_type}"}

    except Exception as exc:
        log.error("task_generate_custom_frame failed %s/%s/f%03d: %s", map_type, run_id, fff, exc)
        raise

    if result and result.get("skipped"):
        return result

    # Count actual tiles written to staging dir for this frame
    tile_dir = output_dir / f"{fff:03d}"
    actual_tiles = sum(1 for _ in tile_dir.rglob("*.png")) if tile_dir.exists() else 0

    elapsed = time.perf_counter() - start_t
    log.info(
        "Custom frame done %s/%s/f%03d/%s: actual_tiles=%d duration=%.2fs",
        map_type, run_id, fff, product_name, actual_tiles, elapsed,
    )

    return {
        "map_type":     map_type,
        "run_id":       run_id,
        "fff":          fff,
        "product":      product_name,
        "actual_tiles": actual_tiles,
        "duration_s":   round(elapsed, 2),
    }


# ---------------------------------------------------------------------------
# PARSE WORKER TASK
# ---------------------------------------------------------------------------
def task_parse_fields(map_type: str, run_id: str, fff: int, product_name: str) -> dict:
    """Read GRIB file and return the raw fields (lat, lon, values).
    Runs in a subprocess to avoid eccodes C-lib blocking the event loop."""
    cfg = get_settings()
    from ..services.tile_generator import _load_primary_field, _COLORMAP_PRODUCT, _COLORMAP_PRODUCT_ARG

    grib_file = cfg.DATA_DIR / map_type / run_id / product_name / f"f{fff:03d}.grib2"

    # REAL check: file must exist AND be larger than 1KB
    if not grib_file.exists():
        return {"skipped": True, "reason": f"GRIB file not found: {grib_file}"}
    if grib_file.stat().st_size < 1024:
        return {"skipped": True, "reason": f"GRIB file is too small ({grib_file.stat().st_size} bytes), likely corrupted: {grib_file}"}

    start_t = time.perf_counter()
    try:
        field = _load_primary_field(grib_file, product_name)
    except Exception as exc:
        return {"skipped": True, "reason": f"Failed to read GRIB: {exc}"}

    if field is None:
        return {"skipped": True, "reason": f"No field data returned from GRIB: {grib_file}"}

    cmap_type = _COLORMAP_PRODUCT.get(product_name, map_type)
    cmap_product = _COLORMAP_PRODUCT_ARG.get(product_name)

    return {
        "map_type": map_type,
        "run_id": run_id,
        "fff": fff,
        "product_name": product_name,
        "field": field,
        "cmap_type": cmap_type,
        "cmap_product": cmap_product,
        "parse_duration_s": time.perf_counter() - start_t,
    }


# ---------------------------------------------------------------------------
# BUILD WORKER TASK
# ---------------------------------------------------------------------------
def task_build_canvas(parse_result: dict) -> dict:
    """Warp GribField to Web Mercator canvas and save as .npy for IPC."""
    if parse_result.get("skipped"):
        return parse_result

    from ..core.tile_cutter import _warp_scalar_to_mercator
    from rasterio.warp import Resampling
    import numpy as np

    cfg = get_settings()
    field: GribField = parse_result["field"]

    start_t = time.perf_counter()
    canvas_size = min((2 ** cfg.TILE_ZOOM_EAGER_MAX) * cfg.TILE_SIZE, 8192)

    merc, px_per_meter = _warp_scalar_to_mercator(
        field.values, field.lat, field.lon, canvas_size, resampling=Resampling.bilinear
    )

    # Save canvas to disk as .npy (avoids Windows SharedMemory destruction bug)
    staging_dir = cfg.STAGING_DIR / parse_result["map_type"] / parse_result["run_id"] / "canvases"
    staging_dir.mkdir(parents=True, exist_ok=True)

    npy_path = staging_dir / f"{parse_result['product_name']}_{parse_result['fff']:03d}.npy"
    np.save(str(npy_path), merc)

    # Validate the file was written correctly
    if not npy_path.exists() or npy_path.stat().st_size < 1024:
        raise RuntimeError(f"Failed to write canvas to disk: {npy_path}")

    return {
        "npy_path": str(npy_path),
        "px_per_meter": float(px_per_meter),
        "cmap_type": parse_result["cmap_type"],
        "cmap_product": parse_result["cmap_product"],
        "build_duration_s": time.perf_counter() - start_t,
    }


# ---------------------------------------------------------------------------
# CUT & WRITE WORKER TASK
# ---------------------------------------------------------------------------
def task_cut_and_write_tiles(
    map_type: str, run_id: str, fff: int, product_name: str, build_result: dict
) -> dict:
    """Load mmap canvas, cut tiles in parallel threads, write PNGs to staging dir."""
    if build_result.get("skipped"):
        return build_result

    import mercantile
    import numpy as np
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from ..core.tile_cutter import _cut_tile_from_merc

    cfg = get_settings()
    start_t = time.perf_counter()

    npy_path = build_result["npy_path"]
    px_per_meter = build_result["px_per_meter"]
    cmap_type = build_result["cmap_type"]
    cmap_product = build_result["cmap_product"]

    # Validate canvas file before loading
    npy_file = Path(npy_path)
    if not npy_file.exists():
        raise FileNotFoundError(f"Canvas .npy file disappeared before Cut stage: {npy_path}")
    if npy_file.stat().st_size < 1024:
        raise RuntimeError(f"Canvas .npy file is too small, likely corrupted: {npy_path}")

    # Load via mmap — shared read-only across all threads (zero RAM copy)
    merc = np.load(npy_path, mmap_mode="r")

    # Output directories
    staging_dir = cfg.STAGING_DIR / map_type / run_id / f"{fff:03d}" / product_name
    live_dir = cfg.TILES_DIR / map_type / run_id / f"{fff:03d}" / product_name
    staging_dir.mkdir(parents=True, exist_ok=True)

    # Build full tile list for the configured zoom range
    zoom_min = 0
    zoom_max = cfg.TILE_ZOOM_EAGER_MAX
    all_tiles = []
    for z in range(zoom_min, zoom_max + 1):
        all_tiles.extend(mercantile.tiles(-180, -85.051129, 180, 85.051129, zooms=z))

    # Pre-filter: skip tiles that already exist with non-zero size on staging OR live
    # (REAL filesystem check — stat() must succeed AND size > 0)
    pending_tiles = []
    skipped = 0
    for tile in all_tiles:
        out_s = staging_dir / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        out_l = live_dir / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        s_ok = out_s.exists() and out_s.stat().st_size > 0
        l_ok = out_l.exists() and out_l.stat().st_size > 0
        if s_ok or l_ok:
            skipped += 1
        else:
            pending_tiles.append(tile)

    log.debug(
        "Cut %s/%s/f%03d/%s: total=%d pending=%d skipped=%d",
        map_type, run_id, fff, product_name,
        len(all_tiles), len(pending_tiles), skipped,
    )

    saved = errors = 0

    def _cut_one(tile: mercantile.Tile) -> str:
        out = staging_dir / str(tile.z) / str(tile.x) / f"{tile.y}.png"
        out.parent.mkdir(parents=True, exist_ok=True)
        try:
            png_bytes = _cut_tile_from_merc(merc, px_per_meter, tile, cmap_type, cmap_product)
            if not png_bytes:
                return "err:empty_png"
            out.write_bytes(png_bytes)
            # Validate written file
            if not out.exists() or out.stat().st_size == 0:
                return "err:write_failed"
            return "ok"
        except Exception as exc:
            log.error(f"Tile cut error {tile}: {exc}")
            return f"err:{exc}"

    # 16 I/O threads per process — PNG encode is CPU-fast, disk I/O is the bottleneck
    n_threads = min(16, len(pending_tiles)) if pending_tiles else 1
    with ThreadPoolExecutor(max_workers=n_threads) as pool:
        futures = [pool.submit(_cut_one, t) for t in pending_tiles]
        for fut in as_completed(futures):
            result = fut.result()
            if result == "ok":
                saved += 1
            else:
                errors += 1
                log.warning(f"Tile error in {map_type}/{run_id}/f{fff:03d}/{product_name}: {result}")

    # Final validation: count actual PNGs written (staging only, live hasn't been swapped yet)
    actual_on_disk = sum(1 for _ in staging_dir.rglob("*.png"))
    expected_total = len(all_tiles)

    log.info(
        "Cut complete %s/%s/f%03d/%s: saved=%d skipped=%d errors=%d disk_actual=%d/%d duration=%.2fs",
        map_type, run_id, fff, product_name,
        saved, skipped, errors, actual_on_disk, expected_total,
        time.perf_counter() - start_t,
    )

    if errors > 0:
        raise RuntimeError(
            f"Cut had {errors} tile errors for {map_type}/{run_id}/f{fff:03d}/{product_name}. "
            f"Only {actual_on_disk}/{expected_total} tiles on disk."
        )

    return {
        "saved": saved,
        "skipped": skipped,
        "errors": errors,
        "total": expected_total,
        "actual_on_disk": actual_on_disk,
        "cut_duration_s": time.perf_counter() - start_t,
    }


# ---------------------------------------------------------------------------
# PUBLISH WORKER TASK
# ---------------------------------------------------------------------------
def publish_staging_to_live(map_type: str, run_id: str):
    """Atomically swap STAGING → LIVE for a completed run.

    Strategy:
      1. Merge staging tiles into live (missing tiles get added, existing live tiles preserved).
      2. Remove staging after successful merge.

    This is safer than a rename-swap because after publish, live already contains
    all tiles from staging PLUS any tiles from a previous run that were not re-generated.
    """
    cfg = get_settings()
    staging = cfg.STAGING_DIR / map_type / run_id
    live = cfg.TILES_DIR / map_type / run_id

    if not staging.exists():
        log.warning(f"publish_staging_to_live: staging dir does not exist: {staging}")
        return

    # Count staging tiles before merge (sanity check)
    staging_count = sum(1 for _ in staging.rglob("*.png"))
    if staging_count == 0:
        log.warning(
            f"publish_staging_to_live: staging dir exists but has 0 PNG files — skipping publish: {staging}"
        )
        return

    log.info(f"Publishing {map_type}/{run_id}: {staging_count} tiles from staging → live")

    live.mkdir(parents=True, exist_ok=True)

    try:
        # Use copytree with dirs_exist_ok to merge staging into live
        # New files from staging overwrite old ones in live (tiles regenerated take priority)
        shutil.copytree(str(staging), str(live), dirs_exist_ok=True)

        # Validate live dir after merge
        live_count = sum(1 for _ in live.rglob("*.png"))
        log.info(f"Published {map_type}/{run_id}: live now has {live_count} PNG tiles")

        # Only remove staging if merge was successful
        shutil.rmtree(str(staging), ignore_errors=True)

    except Exception as exc:
        log.error(f"publish_staging_to_live failed for {map_type}/{run_id}: {exc}")
        raise

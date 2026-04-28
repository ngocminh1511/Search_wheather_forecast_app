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
                workers=cfg.TILE_PROCESS_WORKERS, skip_existing=True,
            )

        elif map_type == "cloud_total":
            from ..core.cloud_pipeline import generate_cloud_frame
            result = generate_cloud_frame(
                run_id=run_id, fff=fff,
                data_dir=cfg.DATA_DIR, output_dir=output_dir,
                zoom_min=0, zoom_max=cfg.TILE_ZOOM_EAGER_MAX,
                workers=cfg.TILE_PROCESS_WORKERS, skip_existing=True,
            )

        elif map_type == "wind_surface":
            from ..core.wind_pipeline import generate_wind_frame
            result = generate_wind_frame(
                run_id=run_id, fff=fff,
                data_dir=cfg.DATA_DIR, output_dir=output_dir,
                zoom_min=0, zoom_max=cfg.TILE_ZOOM_EAGER_MAX,
                workers=cfg.TILE_PROCESS_WORKERS, skip_existing=True,
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
    z_max = min(
        cfg.TILE_ZOOM_EAGER_MAX,
        cfg.TILE_PER_MAP_ZOOM.get(parse_result["map_type"], cfg.TILE_ZOOM_EAGER_MAX),
    )
    canvas_size = min((2 ** z_max) * cfg.TILE_SIZE, 8192)

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

    zoom_min = 0
    zoom_max = min(cfg.TILE_ZOOM_EAGER_MAX, cfg.TILE_PER_MAP_ZOOM.get(map_type, cfg.TILE_ZOOM_EAGER_MAX))

    from ..core.metatile_processor import process_all_metatiles
    
    summary = process_all_metatiles(
        npy_path=npy_path,
        px_per_meter=px_per_meter,
        cmap_type=cmap_type,
        cmap_product=cmap_product,
        output_dir=staging_dir,
        zoom_min=zoom_min,
        zoom_max=zoom_max,
        workers=cfg.TILE_PROCESS_WORKERS
    )
    
    # Write manifest
    import json
    manifest = {
        "ready": True,
        "product": product_name,
        "total": summary["total"],
        "saved": summary["saved"],
        "empty_skipped": summary["empty_skipped"],
        "errors": summary["errors"],
        "chunks_written": summary["chunks_written"],
        "total_size_bytes": summary["bytes"],
        "format": "chunk",
        "tile_format": cfg.TILE_FORMAT_DEFAULT,
        "tile_ext": "webp" if cfg.TILE_FORMAT_DEFAULT == "webp" else "png",
        "timestamp": time.time()
    }
    (staging_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

    log.info(
        "Cut complete %s/%s/f%03d/%s: saved=%d chunks=%d empty_skipped=%d errors=%d duration=%.2fs",
        map_type, run_id, fff, product_name,
        summary["saved"], summary["chunks_written"], summary["empty_skipped"], summary["errors"],
        time.perf_counter() - start_t,
    )

    if summary["errors"] > 0:
        raise RuntimeError(
            f"Cut had {summary['errors']} tile errors for {map_type}/{run_id}/f{fff:03d}/{product_name}."
        )

    return {
        "saved": summary["saved"],
        "empty_skipped": summary["empty_skipped"],
        "errors": summary["errors"],
        "chunks_written": summary["chunks_written"],
        "total": summary["total"],
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
    staging_count = sum(1 for _ in staging.rglob("*.chunk"))
    if staging_count == 0:
        log.warning(
            f"publish_staging_to_live: staging dir exists but has 0 CHUNK files — skipping publish: {staging}"
        )
        return

    log.info(f"Publishing {map_type}/{run_id}: {staging_count} chunks from staging → live")

    live.mkdir(parents=True, exist_ok=True)

    try:
        # Use copytree with dirs_exist_ok to merge staging into live
        # New files from staging overwrite old ones in live (tiles regenerated take priority)
        shutil.copytree(str(staging), str(live), dirs_exist_ok=True)

        # Validate live dir after merge
        live_count = sum(1 for _ in live.rglob("*.chunk"))
        log.info(f"Published {map_type}/{run_id}: live now has {live_count} CHUNK files")

        # Only remove staging if merge was successful
        shutil.rmtree(str(staging), ignore_errors=True)

    except Exception as exc:
        log.error(f"publish_staging_to_live failed for {map_type}/{run_id}: {exc}")
        raise

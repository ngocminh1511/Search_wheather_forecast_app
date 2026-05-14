import logging
import time
import shutil
from pathlib import Path
from ..config import get_settings
from ..core.grib_reader import read_first_field, read_multi_fields, GribField
from ..core.shared_mem import SharedCanvas

log = logging.getLogger(__name__)



# ---------------------------------------------------------------------------
# CUSTOM MAP TASK — rain_advanced / wind_surface
# These use multi-field pipelines — bypass the Parse→Build→Cut stages entirely.
# ---------------------------------------------------------------------------
def task_generate_custom_frame(map_type: str, run_id: str, fff: int, product_name: str, base_only: bool = False, field_only: bool = False) -> dict:
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
                base_only=base_only,
            )

        elif map_type == "wind_surface":
            from ..core.wind_pipeline import generate_wind_frame
            result = generate_wind_frame(
                run_id=run_id, fff=fff,
                data_dir=cfg.DATA_DIR, output_dir=output_dir,
                zoom_min=0, zoom_max=cfg.TILE_ZOOM_EAGER_MAX,
                workers=cfg.TILE_PROCESS_WORKERS, skip_existing=True,
                base_only=base_only, field_only=field_only,
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
        # Pass through pipeline-level timings and metatile stats for benchmarking
        "timings":      result.get("timings", {}),
        "base":         result.get("base", {}),
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

    # Load via mmap — shared read-only across all threads (zero RAM copy).
    # On Windows, mmap holds an exclusive file handle in the worker process and
    # prevents subsequent delete/cleanup (WinError 32). We open without mmap
    # there to keep the file deletable; the array is small enough that the
    # full read isn't a problem for the rare Windows dev runs.
    import sys as _sys
    if _sys.platform.startswith("win"):
        merc = np.load(npy_path)
    else:
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
        # Metatile-level detail for benchmark
        "metatiles_total": summary.get("metatiles_total", 0),
        "metatiles_empty_skipped": summary.get("metatiles_empty_skipped", 0),
        "tiles_empty_skipped_inside_nonempty": summary.get("tiles_empty_skipped_inside_nonempty", 0),
        "metatile_extract_time_s": summary.get("metatile_extract_time_s", 0.0),
        "colorize_time_s": summary.get("colorize_time_s", 0.0),
        "encode_time_s": summary.get("encode_time_s", 0.0),
        "chunk_write_time_s": summary.get("chunk_write_time_s", 0.0),
        "bytes_before_compress": summary.get("bytes_before_compress", 0),
        "bytes_after_compress": summary.get("bytes_after_compress", 0),
    }


# ---------------------------------------------------------------------------
# PUBLISH WORKER TASK
# ---------------------------------------------------------------------------
def publish_staging_to_live(map_type: str, run_id: str):
    """Publish STAGING → live destination.

    Two modes:
      - Bunny enabled: tiles already pushed per-frame to Bunny during cut.
        Just cleanup STAGING (Bunny is canonical, no LIVE local).
      - Bunny disabled (legacy): copy STAGING → LIVE local then remove STAGING.

    Called once per run from the publish worker.
    """
    cfg = get_settings()
    staging = cfg.STAGING_DIR / map_type / run_id

    if not staging.exists():
        log.warning(f"publish_staging_to_live: staging dir does not exist: {staging}")
        return

    # ── Bunny mode: STAGING is ephemeral, Bunny is canonical ──────────────
    if cfg.BUNNY_ENABLED:
        # Per-frame push happened during cut. Before deleting STAGING, verify
        # that the orchestrator's push metrics show no per-frame failures —
        # otherwise STAGING is the only remaining copy of those chunks and
        # deleting it now would silently drop tiles.
        try:
            from .pipeline_orchestrator import get_orchestrator
            push_m = get_orchestrator().get_push_metrics(map_type, run_id)
            failed = int(push_m.get("failed", 0) or 0)
            if failed > 0:
                raise RuntimeError(
                    f"publish_staging_to_live: refusing to clean STAGING for "
                    f"{map_type}/{run_id} — {failed} chunk push(es) failed; "
                    f"retry will repush from STAGING."
                )
        except RuntimeError:
            raise
        except Exception as exc:
            # If we can't read metrics for whatever reason, fall through to the
            # original behavior but log loudly.
            log.warning(
                "publish_staging_to_live: push metrics unavailable for %s/%s "
                "(%s); proceeding with cleanup",
                map_type, run_id, exc,
            )

        # Per-frame push happened during cut. STAGING residuals (e.g. interp
        # sub-frames pushed separately by scheduler) may still exist; remove.
        # NO LIVE local — Bunny is the only canonical store.
        try:
            shutil.rmtree(str(staging), ignore_errors=True)
            log.info(
                "Bunny mode: STAGING cleaned up for %s/%s (LIVE local skipped)",
                map_type, run_id,
            )
        except Exception as exc:
            log.warning(
                "Bunny mode: STAGING cleanup failed (non-fatal) %s/%s: %s",
                map_type, run_id, exc,
            )
        return

    # ── Legacy local-only mode: STAGING → LIVE ────────────────────────────
    live = cfg.TILES_DIR / map_type / run_id
    staging_count = sum(1 for _ in staging.rglob("*.chunk"))
    if staging_count == 0:
        log.warning(
            f"publish_staging_to_live: staging dir exists but has 0 CHUNK files — skipping publish: {staging}"
        )
        return

    log.info(
        f"Publishing {map_type}/{run_id}: {staging_count} chunks from staging → live (legacy local mode)"
    )
    live.mkdir(parents=True, exist_ok=True)
    try:
        # Use copytree with dirs_exist_ok to merge staging into live
        shutil.copytree(str(staging), str(live), dirs_exist_ok=True)
        live_count = sum(1 for _ in live.rglob("*.chunk"))
        log.info(f"Published {map_type}/{run_id}: live now has {live_count} CHUNK files")
        shutil.rmtree(str(staging), ignore_errors=True)
    except Exception as exc:
        log.error(f"publish_staging_to_live failed for {map_type}/{run_id}: {exc}")
        raise


# ---------------------------------------------------------------------------
# Bunny.net Storage push — per-frame
# ---------------------------------------------------------------------------
def push_frame_to_bunny(
    map_type: str,
    run_id: str,
    fff: int,
    fff_label: str | None = None,
    product: str | None = None,
    source_root: Path | None = None,
) -> bool:
    """Push 1 frame's (or 1 product within a frame's) tiles to Bunny.net Storage.

    Called per-(frame, product) after tile_cutter writes chunks to STAGING.
    Frontend does NOT see this run yet — pointer (`_current.json`) still points
    to old run. Frontend will only switch when finalize_map_to_bunny() runs.

    Args:
        map_type: e.g. 'wind_surface'
        run_id: e.g. '20260510_06z'
        fff: forecast hour, e.g. 6
        fff_label: optional label override (e.g. '006_15' for interp sub-frames).
                   Defaults to f"{fff:03d}".
        product: optional product name to scope the push to a single product
                 subdir (e.g. 'wind_base'). When None, pushes the whole frame.
        source_root: optional override for source dir (defaults to STAGING_DIR).
                     Used by `finalize_map_to_bunny` to push from LIVE for hardlinked
                     cold frames.

    Returns:
        True if all chunks uploaded successfully (or Bunny disabled).
        False if any chunk failed (caller may retry or abort).
    """
    cfg = get_settings()
    label = fff_label if fff_label is not None else f"{fff:03d}"
    base = source_root if source_root is not None else cfg.STAGING_DIR

    # Build local source dir + remote label
    if product:
        local_dir = base / map_type / run_id / label / product
        remote_label = f"{label}/{product}"
    else:
        local_dir = base / map_type / run_id / label
        remote_label = label

    if not local_dir.exists():
        log.warning(
            "push_frame_to_bunny: source dir missing %s",
            local_dir,
        )
        return False

    try:
        from .bunny_storage import get_bunny_client
        bunny = get_bunny_client()
    except Exception as exc:
        log.error("push_frame_to_bunny: failed to init Bunny client: %s", exc)
        return False

    if bunny is None:
        # BUNNY_ENABLED=0 or misconfigured → noop, treat as success
        return True

    try:
        stats = bunny.push_frame(local_dir, map_type, run_id, remote_label)
    except Exception as exc:
        log.error(
            "push_frame_to_bunny: %s/%s/%s exception: %s",
            map_type, run_id, remote_label, exc,
        )
        if cfg.BUNNY_FAIL_FAST:
            raise
        return False

    success = (stats["failed"] == 0 and stats["total"] > 0)
    log.info(
        "Bunny push %s/%s/%s: ok=%d failed=%d bytes=%d %s",
        map_type, run_id, remote_label,
        stats["ok"], stats["failed"], stats["bytes"],
        "✓" if success else "✗",
    )

    # Per-frame STAGING cleanup after successful push (Bunny is canonical, no LIVE local).
    # Only delete the dir we just pushed (product subdir or full frame dir).
    if success and source_root is None:
        try:
            shutil.rmtree(local_dir, ignore_errors=True)
            log.debug("STAGING cleaned up after push: %s", local_dir)
            # If product-scoped and parent frame dir is now empty, remove parent too
            if product:
                parent = cfg.STAGING_DIR / map_type / run_id / label
                try:
                    if parent.exists() and not any(parent.iterdir()):
                        parent.rmdir()
                except OSError:
                    pass
        except Exception as exc:
            log.warning(
                "STAGING cleanup failed (non-fatal) %s: %s", local_dir, exc,
            )

    if not success and cfg.BUNNY_FAIL_FAST:
        raise RuntimeError(
            f"Bunny push had {stats['failed']} failures for "
            f"{map_type}/{run_id}/{remote_label}"
        )
    return success


def push_frame_to_bunny_with_stats(
    map_type: str,
    run_id: str,
    fff: int,
    fff_label: str | None = None,
    product: str | None = None,
    source_root: Path | None = None,
) -> dict:
    """Same as push_frame_to_bunny() but returns full stats dict for reporting.

    Returns:
        {
          "success": bool,
          "ok": int,         # chunks uploaded successfully
          "failed": int,     # chunks failed
          "bytes": int,      # bytes uploaded
          "total": int,      # total chunks attempted
        }
    """
    cfg = get_settings()
    label = fff_label if fff_label is not None else f"{fff:03d}"
    base = source_root if source_root is not None else cfg.STAGING_DIR

    if product:
        local_dir = base / map_type / run_id / label / product
        remote_label = f"{label}/{product}"
    else:
        local_dir = base / map_type / run_id / label
        remote_label = label

    if not local_dir.exists():
        return {"success": False, "ok": 0, "failed": 0, "bytes": 0, "total": 0}

    try:
        from .bunny_storage import get_bunny_client
        bunny = get_bunny_client()
    except Exception:
        return {"success": False, "ok": 0, "failed": 0, "bytes": 0, "total": 0}

    if bunny is None:
        return {"success": True, "ok": 0, "failed": 0, "bytes": 0, "total": 0}

    try:
        stats = bunny.push_frame(local_dir, map_type, run_id, remote_label)
    except Exception as exc:
        log.error(
            "push_frame_to_bunny_with_stats: %s/%s/%s exception: %s",
            map_type, run_id, remote_label, exc,
        )
        if cfg.BUNNY_FAIL_FAST:
            raise
        return {"success": False, "ok": 0, "failed": 0, "bytes": 0, "total": 0}

    success = (stats["failed"] == 0 and stats["total"] > 0)
    log.info(
        "Bunny push %s/%s/%s: ok=%d failed=%d bytes=%d %s",
        map_type, run_id, remote_label,
        stats["ok"], stats["failed"], stats["bytes"],
        "✓" if success else "✗",
    )

    # Per-frame STAGING cleanup after successful push
    if success and source_root is None:
        try:
            shutil.rmtree(local_dir, ignore_errors=True)
            if product:
                parent = cfg.STAGING_DIR / map_type / run_id / label
                try:
                    if parent.exists() and not any(parent.iterdir()):
                        parent.rmdir()
                except OSError:
                    pass
        except Exception as exc:
            log.warning("STAGING cleanup failed (non-fatal) %s: %s", local_dir, exc)

    return {
        "success": success,
        "ok": stats["ok"],
        "failed": stats["failed"],
        "bytes": stats["bytes"],
        "total": stats["total"],
    }

import asyncio
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, Any, List

from ..config import get_settings
from ..core.db import upsert_pipeline_job, get_pipeline_jobs_by_run, check_cancel_requested, JobCancelledError
from .resource_guard import check_resources

log = logging.getLogger(__name__)

# Map types that use custom multi-field pipelines (precip, wind, cloud).
# These bypass the generic Parse→Build→Cut stages and run generate_frame directly.
_CUSTOM_MAP_TYPES = frozenset({"rain_advanced", "cloud_total", "wind_surface"})

_orchestrator_instance = None


class PipelineOrchestrator:
    def __init__(self):
        self.cfg = get_settings()

        # PriorityQueues: priority = fff, so f000 from all maps runs before f003 of any map
        self.parse_queue   = asyncio.PriorityQueue()
        self.build_queue   = asyncio.PriorityQueue()
        self.cut_queue     = asyncio.PriorityQueue()
        self.publish_queue = asyncio.PriorityQueue()

        # Process pools for CPU-bound work
        self.parse_pool = ProcessPoolExecutor(max_workers=self.cfg.MAX_PARSE_WORKERS)
        self.build_pool = ProcessPoolExecutor(max_workers=self.cfg.MAX_BUILD_WORKERS)
        self.cut_pool   = ProcessPoolExecutor(max_workers=self.cfg.MAX_CUT_WORKERS)

        self.loop       = None
        self.is_running = False
        self.workers    = []
        self.active_runs: set[str] = set()
        self.hot_fff = set(self.cfg.PRIORITY_FFF_HOT_LIST)

    def _job_priority(self, map_type: str, fff: int) -> tuple[int, int]:
        """
        Priority order:
          0: hot fff list (now/near terms)
          1: 0..24h
          2: 24..72h
          3: >72h
        Lower tuple value = higher priority.
        """
        if fff in self.hot_fff:
            return (0, fff)
        if fff <= 24:
            return (1, fff)
        if fff <= 72:
            return (2, fff)
        return (3, fff)

    async def start(self):
        if self.is_running:
            return
        self.is_running = True
        self.loop = asyncio.get_running_loop()
        log.info("Starting Pipeline Orchestrator — spawning workers per stage …")

        # ---------------------------------------------------------------
        # CRITICAL FIX: spawn N concurrent asyncio Tasks per stage so that
        # all N ProcessPool workers are kept busy simultaneously.
        # Previously only 1 Task existed per stage → sequential processing.
        # ---------------------------------------------------------------
        self.workers = []
        # Parse: N workers (includes custom-map runners)
        for _ in range(self.cfg.MAX_PARSE_WORKERS):
            self.workers.append(asyncio.create_task(self._parse_worker()))
        # Build: N workers
        for _ in range(self.cfg.MAX_BUILD_WORKERS):
            self.workers.append(asyncio.create_task(self._build_worker()))
        # Cut: N workers (most CPU-intensive)
        for _ in range(self.cfg.MAX_CUT_WORKERS):
            self.workers.append(asyncio.create_task(self._cut_worker()))
        # Publish: 1 worker is enough (atomic swap is fast)
        self.workers.append(asyncio.create_task(self._publish_worker()))

        log.info(
            "Orchestrator started: %d parse + %d build + %d cut + 1 publish workers",
            self.cfg.MAX_PARSE_WORKERS,
            self.cfg.MAX_BUILD_WORKERS,
            self.cfg.MAX_CUT_WORKERS,
        )

    async def stop(self):
        self.is_running = False
        for w in self.workers:
            w.cancel()
        self.parse_pool.shutdown(wait=False)
        self.build_pool.shutdown(wait=False)
        self.cut_pool.shutdown(wait=False)
        log.info("Pipeline Orchestrator stopped.")

    # ------------------------------------------------------------------
    # Submit a run (called from a non-async thread via tile_generator)
    # ------------------------------------------------------------------

    def submit_run(self, map_type: str, run_id: str, fff_values: List[int]):
        """Enqueue jobs for all (fff, product) combinations in this run.

        Skip check is REAL: we count actual PNG files on disk (staging + live).
        Only jobs with < MIN_TILES_THRESHOLD files get queued for generation.
        """
        run_key = f"{map_type}_{run_id}"
        if run_key in self.active_runs:
            log.warning("Run %s is already active — ignoring duplicate submit.", run_key)
            return

        self.active_runs.add(run_key)

        def _push_jobs():
            MIN_CHUNKS = 1

            from ..services.tile_generator import _MAP_PRODUCTS
            products = _MAP_PRODUCTS.get(map_type, [])

            for fff in sorted(fff_values):
                for product in products:
                    job_id = f"{map_type}_{run_id}_{fff:03d}_{product}"

                    # --- REAL filesystem check (never trust DB alone) ---
                    stg = self.cfg.STAGING_DIR / map_type / run_id / f"{fff:03d}" / product
                    live = self.cfg.TILES_DIR   / map_type / run_id / f"{fff:03d}" / product

                    n_stg = sum(1 for _ in stg.rglob("*.chunk")) if stg.exists() else 0
                    n_live = sum(1 for _ in live.rglob("*.chunk")) if live.exists() else 0
                    n_total = n_stg + n_live

                    if n_total >= MIN_CHUNKS:
                        log.debug("[submit] SKIP %s — %d chunks on disk", job_id, n_total)
                        upsert_pipeline_job(job_id, map_type, run_id, fff, product, "READY")
                        continue

                    log.debug("[submit] QUEUE %s (disk=%d)", job_id, n_total)
                    job_data = {
                        "id":       job_id,
                        "map_type": map_type,
                        "run_id":   run_id,
                        "fff":      fff,
                        "product":  product,
                        "state":    "PENDING",
                    }
                    upsert_pipeline_job(job_id, map_type, run_id, fff, product, "PENDING")
                    self.parse_queue.put_nowait((self._job_priority(map_type, fff), job_id, job_data))

        if self.loop and self.loop.is_running():
            self.loop.call_soon_threadsafe(_push_jobs)
        else:
            _push_jobs()

    # ------------------------------------------------------------------
    # Resource guard helper
    # ------------------------------------------------------------------

    async def _wait_for_resources(self, stage: str):
        while not check_resources(stage):
            await asyncio.sleep(5.0)

    # ------------------------------------------------------------------
    # PARSE worker — also handles custom map types directly
    # ------------------------------------------------------------------

    async def _parse_worker(self):
        loop = asyncio.get_running_loop()
        while self.is_running:
            try:
                prio, j_id, job = await self.parse_queue.get()
            except Exception:
                continue
            try:
                map_type = job["map_type"]
                upsert_pipeline_job(
                    job["id"], map_type, job["run_id"], job["fff"], job["product"], "PARSING"
                )
                await self._wait_for_resources("parse")

                if check_cancel_requested(map_type):
                    raise JobCancelledError("Cancelled by user")

                # -------------------------------------------------------
                # CUSTOM MAPS: rain_advanced, cloud_total, wind_surface
                # These use multi-field pipelines (precip, cloud, wind).
                # We call generate_frame directly in the parse pool and
                # mark READY immediately — no Build/Cut stages needed.
                # -------------------------------------------------------
                if map_type in _CUSTOM_MAP_TYPES:
                    from .pipeline_tasks import task_generate_custom_frame
                    result = await loop.run_in_executor(
                        self.parse_pool,
                        task_generate_custom_frame,
                        map_type, job["run_id"], job["fff"], job["product"],
                    )
                    if result.get("skipped"):
                        upsert_pipeline_job(
                            job["id"], map_type, job["run_id"], job["fff"], job["product"], "SKIPPED"
                        )
                    else:
                        upsert_pipeline_job(
                            job["id"], map_type, job["run_id"], job["fff"], job["product"], "READY"
                        )
                    await self._check_run_completion(map_type, job["run_id"])
                    continue

                # -------------------------------------------------------
                # STANDARD MAPS: Parse → Build → Cut
                # -------------------------------------------------------
                from .pipeline_tasks import task_parse_fields
                parse_result = await loop.run_in_executor(
                    self.parse_pool,
                    task_parse_fields,
                    map_type, job["run_id"], job["fff"], job["product"],
                )

                if parse_result.get("skipped"):
                    log.warning(
                        "[parse] SKIP %s: %s", job["id"], parse_result.get("reason", "")
                    )
                    upsert_pipeline_job(
                        job["id"], map_type, job["run_id"], job["fff"], job["product"], "SKIPPED"
                    )
                    await self._check_run_completion(map_type, job["run_id"])
                    continue

                job["parse_result"] = parse_result
                upsert_pipeline_job(
                    job["id"], map_type, job["run_id"], job["fff"], job["product"], "PARSED"
                )
                self.build_queue.put_nowait((job["fff"], job["id"], job))

            except JobCancelledError:
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "CANCELLED"
                )
            except Exception as e:
                log.error("Parse failed for %s: %s", job["id"], e)
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "ERROR", str(e)
                )
            finally:
                self.parse_queue.task_done()

    # ------------------------------------------------------------------
    # BUILD worker
    # ------------------------------------------------------------------

    async def _build_worker(self):
        loop = asyncio.get_running_loop()
        while self.is_running:
            try:
                prio, j_id, job = await self.build_queue.get()
            except Exception:
                continue
            try:
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "BUILDING"
                )
                await self._wait_for_resources("build")

                if check_cancel_requested(job["map_type"]):
                    raise JobCancelledError("Cancelled by user")

                from .pipeline_tasks import task_build_canvas
                build_result = await loop.run_in_executor(
                    self.build_pool,
                    task_build_canvas,
                    job["parse_result"],
                )

                job["build_result"] = build_result
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "BUILT"
                )
                self.cut_queue.put_nowait((job["fff"], job["id"], job))

            except JobCancelledError:
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "CANCELLED"
                )
            except Exception as e:
                log.error("Build failed for %s: %s", job["id"], e)
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "ERROR", str(e)
                )
            finally:
                self.build_queue.task_done()

    # ------------------------------------------------------------------
    # CUT worker
    # ------------------------------------------------------------------

    async def _cut_worker(self):
        loop = asyncio.get_running_loop()
        while self.is_running:
            try:
                prio, j_id, job = await self.cut_queue.get()
            except Exception:
                continue
            npy_path = None
            try:
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "CUTTING"
                )
                await self._wait_for_resources("cut")

                if check_cancel_requested(job["map_type"]):
                    raise JobCancelledError("Cancelled by user")

                build_result = job["build_result"]
                npy_path = build_result.get("npy_path")

                from .pipeline_tasks import task_cut_and_write_tiles
                cut_result = await loop.run_in_executor(
                    self.cut_pool,
                    task_cut_and_write_tiles,
                    job["map_type"], job["run_id"], job["fff"], job["product"], build_result,
                )

                # Validate: only mark READY if tiles actually exist on disk
                actual_on_disk = cut_result.get("actual_on_disk", 0)
                total_skipped  = cut_result.get("skipped", 0)
                if actual_on_disk < 10 and total_skipped < 10:
                    raise RuntimeError(
                        f"Cut produced only {actual_on_disk} tiles on disk — something went wrong"
                    )

                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "READY"
                )
                await self._check_run_completion(job["map_type"], job["run_id"])

            except JobCancelledError:
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "CANCELLED"
                )
            except Exception as e:
                log.error("Cut failed for %s: %s", job["id"], e)
                upsert_pipeline_job(
                    job["id"], job["map_type"], job["run_id"], job["fff"], job["product"], "ERROR", str(e)
                )
            finally:
                if npy_path:
                    try:
                        Path(npy_path).unlink(missing_ok=True)
                    except Exception as e:
                        log.error("Failed to delete npy canvas %s: %s", npy_path, e)
                self.cut_queue.task_done()

    # ------------------------------------------------------------------
    # PUBLISH worker — atomic staging → live swap
    # ------------------------------------------------------------------

    async def _publish_worker(self):
        while self.is_running:
            try:
                prio, j_id, run_info = await self.publish_queue.get()
            except Exception:
                continue
            try:
                map_type = run_info["map_type"]
                run_id   = run_info["run_id"]
                log.info("Publishing %s/%s to LIVE", map_type, run_id)

                from .pipeline_tasks import publish_staging_to_live
                publish_staging_to_live(map_type, run_id)

                self.active_runs.discard(f"{map_type}_{run_id}")
            except Exception as e:
                log.error("Publish failed for %s/%s: %s", run_info["map_type"], run_info["run_id"], e)
            finally:
                self.publish_queue.task_done()

    # ------------------------------------------------------------------
    # Run completion check
    # ------------------------------------------------------------------

    async def _check_run_completion(self, map_type: str, run_id: str):
        """Push run to publish queue when all its jobs are in a terminal state."""
        jobs = get_pipeline_jobs_by_run(map_type, run_id)
        if not jobs:
            return

        terminal = {"READY", "SKIPPED", "ERROR", "CANCELLED"}
        all_done = all(j["state"] in terminal for j in jobs)
        if not all_done:
            return

        has_ready = any(j["state"] == "READY" for j in jobs)
        if has_ready:
            self.publish_queue.put_nowait((0, f"{map_type}_{run_id}", {"map_type": map_type, "run_id": run_id}))
        else:
            # All errored/cancelled — remove from active without publishing
            self.active_runs.discard(f"{map_type}_{run_id}")


def get_orchestrator() -> PipelineOrchestrator:
    global _orchestrator_instance
    if _orchestrator_instance is None:
        _orchestrator_instance = PipelineOrchestrator()
    return _orchestrator_instance

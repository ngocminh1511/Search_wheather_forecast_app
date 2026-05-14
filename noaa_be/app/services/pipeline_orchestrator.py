import asyncio
import logging
import time
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path
from typing import Dict, Any, List

from ..config import get_settings
from ..core.db import upsert_pipeline_job, get_pipeline_jobs_by_run, check_cancel_requested, JobCancelledError
from .resource_guard import check_resources

log = logging.getLogger(__name__)

# Map types that use custom multi-field pipelines (precip, wind).
# These bypass the generic Parse→Build→Cut stages and run generate_frame directly.
_CUSTOM_MAP_TYPES = frozenset({"rain_advanced", "wind_surface"})

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
        # run_key → timestamp (monotonic seconds). Acts as a short-lived
        # de-dupe so we don't queue the same publish twice in quick succession,
        # but auto-expires so a publish that ultimately failed can be retried
        # the next cycle. TTL chosen larger than the longest publish (~minutes)
        # but well under one NOAA cycle (6h) so a stuck entry can self-heal.
        self.published_runs: dict[str, float] = {}
        self._PUBLISHED_TTL_S: float = 4 * 3600.0
        # Tracks runs currently queued or in-flight for publish, so the
        # completion check doesn't enqueue duplicates while a publish is
        # waiting in publish_queue or running in _publish_worker.
        self.publishing_runs: set[str] = set()
        self.hot_fff = set(self.cfg.PRIORITY_FFF_HOT_LIST)

        # Per-(map_type, run_id) push metrics tracker for reporting.
        # Updated after each push_frame_to_bunny() call in cut_worker / parse_worker.
        # Schema: {(map_type, run_id): {
        #     "first_push_at": str ISO | None,
        #     "last_push_at":  str ISO | None,
        #     "accumulated_seconds": float,
        #     "ok": int, "failed": int, "bytes": int,
        #     "transient_errors": int, "permanent_errors": int,
        # }}
        self.push_metrics: dict[tuple[str, str], dict] = {}

    def get_push_metrics(self, map_type: str, run_id: str) -> dict:
        """Return accumulated push metrics for a (map, run); empty dict if none."""
        return self.push_metrics.get((map_type, run_id), {}) or {}

    def reset_push_metrics(self, map_type: str, run_id: str) -> None:
        """Clear push metrics after consumed by reporting."""
        self.push_metrics.pop((map_type, run_id), None)

    def _accumulate_push(
        self,
        map_type: str,
        run_id: str,
        duration_s: float,
        ok: int,
        failed: int,
        bytes_uploaded: int,
        is_transient_error: bool = False,
    ) -> None:
        """Track a single push_frame call's outcome into per-map metrics."""
        from datetime import datetime as _dt, timezone as _tz
        now = _dt.now(_tz.utc).isoformat()
        key = (map_type, run_id)
        m = self.push_metrics.setdefault(key, {
            "first_push_at": now,
            "last_push_at": now,
            "accumulated_seconds": 0.0,
            "ok": 0, "failed": 0, "bytes": 0,
            "transient_errors": 0, "permanent_errors": 0,
        })
        if m.get("first_push_at") is None:
            m["first_push_at"] = now
        m["last_push_at"] = now
        m["accumulated_seconds"] += duration_s
        m["ok"] += ok
        m["failed"] += failed
        m["bytes"] += bytes_uploaded
        if is_transient_error:
            m["transient_errors"] += 1
        if failed > 0 and not is_transient_error:
            m["permanent_errors"] += failed

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

                    # --- REAL filesystem check ---
                    # Only count LIVE tiles — staging is transient/incomplete.
                    # Counting staging would incorrectly skip frames that haven't
                    # been published yet (stale staging from a previous run).
                    live = self.cfg.TILES_DIR / map_type / run_id / f"{fff:03d}" / product
                    n_live = sum(1 for _ in live.rglob("*.chunk")) if live.exists() else 0

                    if n_live >= MIN_CHUNKS:
                        log.debug("[submit] SKIP %s — %d live chunks on disk", job_id, n_live)
                        upsert_pipeline_job(job_id, map_type, run_id, fff, product, "READY")
                        continue

                    log.debug("[submit] QUEUE %s (disk=%d)", job_id, n_live)
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

    def _rebuild_pool_if_broken(self, stage: str, exc: BaseException) -> bool:
        """If `exc` is a BrokenProcessPool for `stage`, replace the pool so the
        next job can run. Returns True if a rebuild happened.

        Without this, one worker crash (e.g. import error, OOM) bricks the
        pool and every subsequent submit raises BrokenProcessPool indefinitely.
        """
        if not isinstance(exc, BrokenProcessPool):
            return False
        if stage == "parse":
            log.warning("parse_pool is broken — rebuilding (%s)", exc)
            try:
                self.parse_pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self.parse_pool = ProcessPoolExecutor(max_workers=self.cfg.MAX_PARSE_WORKERS)
            return True
        if stage == "build":
            log.warning("build_pool is broken — rebuilding (%s)", exc)
            try:
                self.build_pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self.build_pool = ProcessPoolExecutor(max_workers=self.cfg.MAX_BUILD_WORKERS)
            return True
        if stage == "cut":
            log.warning("cut_pool is broken — rebuilding (%s)", exc)
            try:
                self.cut_pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self.cut_pool = ProcessPoolExecutor(max_workers=self.cfg.MAX_CUT_WORKERS)
            return True
        return False

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
                # CUSTOM MAPS: rain_advanced, wind_surface
                # These use multi-field pipelines (precip, wind).
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
                        # Per-frame Bunny push for custom maps (whole frame dir at once,
                        # since custom pipelines write all products under same fff/ subdir).
                        from ..config import get_settings
                        _cfg = get_settings()
                        if _cfg.BUNNY_ENABLED:
                            import time as _time
                            push_t0 = _time.perf_counter()
                            push_ok = False
                            push_stats = None
                            try:
                                from .pipeline_tasks import push_frame_to_bunny_with_stats
                                push_stats = await loop.run_in_executor(
                                    None,
                                    lambda: push_frame_to_bunny_with_stats(
                                        map_type=map_type,
                                        run_id=job["run_id"],
                                        fff=job["fff"],
                                    ),
                                )
                                push_ok = push_stats.get("success", False)
                            except Exception as e:
                                log.error(
                                    "Bunny push exception (non-fatal) for custom %s/%s/f%03d: %s",
                                    map_type, job["run_id"], job["fff"], e,
                                )
                                if _cfg.BUNNY_FAIL_FAST:
                                    raise
                            finally:
                                push_dt = _time.perf_counter() - push_t0
                                if push_stats:
                                    self._accumulate_push(
                                        map_type=map_type,
                                        run_id=job["run_id"],
                                        duration_s=push_dt,
                                        ok=push_stats.get("ok", 0),
                                        failed=push_stats.get("failed", 0),
                                        bytes_uploaded=push_stats.get("bytes", 0),
                                        is_transient_error=(
                                            not push_ok and push_stats.get("failed", 0) > 0
                                        ),
                                    )

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
                self._rebuild_pool_if_broken("parse", e)
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
                self._rebuild_pool_if_broken("build", e)
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

                # Validate: only mark READY if tiles actually produced
                actual_saved  = cut_result.get("saved", 0)
                total_skipped = cut_result.get("empty_skipped", 0)
                if actual_saved < 1 and total_skipped < 1:
                    raise RuntimeError(
                        f"Cut produced 0 tiles for "
                        f"{job['map_type']}/{job['run_id']}/f{job['fff']:03d}/{job['product']}"
                        f" — something went wrong"
                    )

                # Per-frame Bunny push (after tiles cut, before READY marker)
                # Pushes only this product's subdir from STAGING. Frontend doesn't see it
                # until finalize_map_to_bunny() switches the pointer at end of cycle.
                from ..config import get_settings
                _cfg = get_settings()
                if _cfg.BUNNY_ENABLED:
                    import time as _time
                    push_t0 = _time.perf_counter()
                    push_ok = False
                    push_stats = None
                    try:
                        from .pipeline_tasks import push_frame_to_bunny_with_stats
                        push_stats = await loop.run_in_executor(
                            None,
                            lambda: push_frame_to_bunny_with_stats(
                                map_type=job["map_type"],
                                run_id=job["run_id"],
                                fff=job["fff"],
                                product=job["product"],
                            ),
                        )
                        push_ok = push_stats.get("success", False)
                    except Exception as e:
                        log.error(
                            "Bunny push exception (non-fatal) for %s/%s/f%03d/%s: %s",
                            job["map_type"], job["run_id"], job["fff"], job["product"], e,
                        )
                        if _cfg.BUNNY_FAIL_FAST:
                            raise
                    finally:
                        # Accumulate per-map metrics for cycle reporting
                        push_dt = _time.perf_counter() - push_t0
                        if push_stats:
                            self._accumulate_push(
                                map_type=job["map_type"],
                                run_id=job["run_id"],
                                duration_s=push_dt,
                                ok=push_stats.get("ok", 0),
                                failed=push_stats.get("failed", 0),
                                bytes_uploaded=push_stats.get("bytes", 0),
                                is_transient_error=(
                                    not push_ok and push_stats.get("failed", 0) > 0
                                ),
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
                self._rebuild_pool_if_broken("cut", e)
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
        import time as _time
        while self.is_running:
            try:
                prio, j_id, run_info = await self.publish_queue.get()
            except Exception:
                continue
            map_type = run_info["map_type"]
            run_id   = run_info["run_id"]
            run_key  = f"{map_type}_{run_id}"
            try:
                log.info("Publishing %s/%s to LIVE", map_type, run_id)

                from .pipeline_tasks import publish_staging_to_live
                publish_staging_to_live(map_type, run_id)

                # Mark published ONLY after success — keeps a TTL so the same
                # run isn't re-queued in quick succession, but a failed run
                # (not marked here) is naturally retryable on the next cycle.
                self.published_runs[run_key] = _time.monotonic()
                self.active_runs.discard(run_key)
            except Exception as e:
                log.error(
                    "Publish failed for %s/%s: %s — staging left in place for retry",
                    map_type, run_id, e,
                )
                # Ensure dedupe entry is cleared so the next completion check
                # can re-queue this run.
                self.published_runs.pop(run_key, None)
            finally:
                # Always release the in-flight guard so a retry path is open.
                self.publishing_runs.discard(run_key)
                self.publish_queue.task_done()

    # ------------------------------------------------------------------
    # Run completion check
    # ------------------------------------------------------------------

    async def _check_run_completion(self, map_type: str, run_id: str):
        """Push run to publish queue ONCE when all its jobs reach a terminal state."""
        import time as _time
        run_key = f"{map_type}_{run_id}"

        # De-dupe: skip if the same run was recently marked published. Entry
        # auto-expires after `_PUBLISHED_TTL_S` so a publish that ultimately
        # failed and wasn't cleared can self-heal at the next completion check.
        prev_ts = self.published_runs.get(run_key)
        if prev_ts is not None:
            if (_time.monotonic() - prev_ts) < self._PUBLISHED_TTL_S:
                return
            log.info(
                "[completion] published_runs entry for %s expired (>%.0fs), allowing re-publish",
                run_key, self._PUBLISHED_TTL_S,
            )
            self.published_runs.pop(run_key, None)

        jobs = get_pipeline_jobs_by_run(map_type, run_id)
        if not jobs:
            return

        terminal = {"READY", "SKIPPED", "ERROR", "CANCELLED"}
        all_done = all(j["state"] in terminal for j in jobs)
        if not all_done:
            log.debug(
                "[completion] %s — %d/%d jobs terminal, waiting for rest",
                run_key, sum(1 for j in jobs if j["state"] in terminal), len(jobs)
            )
            return

        has_ready = any(j["state"] == "READY" for j in jobs)
        if has_ready:
            # In-flight guard: if a publish is already queued or running for
            # this run, don't enqueue a second one.
            if run_key in self.publishing_runs:
                return
            log.info("[completion] All %d jobs done for %s — queuing publish", len(jobs), run_key)
            self.publishing_runs.add(run_key)
            self.publish_queue.put_nowait((0, run_key, {"map_type": map_type, "run_id": run_id}))
        else:
            # All errored/cancelled — remove from active without publishing
            self.active_runs.discard(run_key)


def get_orchestrator() -> PipelineOrchestrator:
    global _orchestrator_instance
    if _orchestrator_instance is None:
        _orchestrator_instance = PipelineOrchestrator()
    return _orchestrator_instance

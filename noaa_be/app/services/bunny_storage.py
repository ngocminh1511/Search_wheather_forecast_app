"""bunny_storage.py — Bunny.net Storage client với atomic pointer-based deploy.

Workflow:
  1. Pipeline cuts tiles → STAGING/{map}/{run}/{fff}/...
  2. Per-frame: push_frame() uploads frame dir to Bunny tiles/{map}/{run}/{fff}/...
     (Pointer chưa switch → frontend vẫn dùng run cũ.)
  3. Khi 1 map đã upload xong all frames: write_pointer() switches atomically.
  4. Sau switch: delete_run() removes previous run from Bunny.
  5. Local STAGING cleanup is caller's responsibility.

Singleton: get_bunny_client() returns None if BUNNY_ENABLED=0 or misconfigured.

Note on httpx import: lazy-imported inside BunnyStorageClient so the module can
be imported without httpx installed (when Bunny is disabled in dev/test).
"""
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ..config import get_settings

log = logging.getLogger(__name__)


class BunnyStorageClient:
    """HTTP client wrapping Bunny Storage API with retry + parallel upload."""

    def __init__(self, settings) -> None:
        # Lazy-import httpx so module can load without dependency when Bunny disabled.
        import httpx as _httpx
        self._httpx = _httpx

        self.zone = settings.BUNNY_STORAGE_ZONE
        self.prefix = settings.BUNNY_PATH_PREFIX.strip("/")
        self.max_parallel = settings.BUNNY_MAX_PARALLEL
        self._retry = settings.BUNNY_RETRY_ATTEMPTS
        self._fail_fast = settings.BUNNY_FAIL_FAST
        base = (
            "storage.bunnycdn.com"
            if not settings.BUNNY_REGION
            else f"{settings.BUNNY_REGION}.storage.bunnycdn.com"
        )
        self.base_url = f"https://{base}/{self.zone}"
        self._client = _httpx.Client(
            headers={
                "AccessKey": settings.BUNNY_API_KEY,
                "User-Agent": "noaa-be-bunny/1.0",
            },
            timeout=settings.BUNNY_TIMEOUT_S,
            limits=_httpx.Limits(
                max_connections=self.max_parallel * 2,
                max_keepalive_connections=self.max_parallel,
            ),
        )

    def close(self) -> None:
        self._client.close()

    def _url(self, remote_path: str) -> str:
        rp = remote_path.lstrip("/")
        return f"{self.base_url}/{self.prefix}/{rp}"

    def _backoff(self, attempt: int) -> None:
        time.sleep(min(2 ** attempt, 30))

    # ── single-file ops ───────────────────────────────────────────────
    def upload_file(self, local: Path, remote: str) -> bool:
        """PUT one file with retry on transient failures."""
        for attempt in range(self._retry):
            try:
                with local.open("rb") as f:
                    r = self._client.put(self._url(remote), content=f)
                if 200 <= r.status_code < 300:
                    return True
                # 400 = Bunny đôi khi return transient cho connection issues; treat as retryable
                if r.status_code in (400, 429, 500, 502, 503, 504):
                    if attempt < self._retry - 1:
                        self._backoff(attempt)
                        continue
                log.error(
                    "Bunny PUT %s → %d %s",
                    remote, r.status_code, r.text[:200],
                )
                return False
            except (self._httpx.TimeoutException, self._httpx.NetworkError) as e:
                if attempt < self._retry - 1:
                    self._backoff(attempt)
                    continue
                log.error("Bunny PUT %s timeout/network: %s", remote, e)
                return False
            except Exception as e:
                log.error("Bunny PUT %s unexpected: %s", remote, e)
                return False
        return False

    def upload_bytes(
        self,
        data: bytes,
        remote: str,
        content_type: Optional[str] = None,
    ) -> bool:
        """PUT raw bytes (e.g. _current.json pointer file)."""
        headers = {}
        if content_type:
            headers["Content-Type"] = content_type
        for attempt in range(self._retry):
            try:
                r = self._client.put(
                    self._url(remote), content=data, headers=headers,
                )
                if 200 <= r.status_code < 300:
                    return True
                # 400 = Bunny đôi khi return transient cho connection issues; treat as retryable
                if r.status_code in (400, 429, 500, 502, 503, 504):
                    if attempt < self._retry - 1:
                        self._backoff(attempt)
                        continue
                log.error(
                    "Bunny PUT bytes %s → %d %s",
                    remote, r.status_code, r.text[:200],
                )
                return False
            except (self._httpx.TimeoutException, self._httpx.NetworkError) as e:
                if attempt < self._retry - 1:
                    self._backoff(attempt)
                    continue
                log.error("Bunny PUT bytes %s: %s", remote, e)
                return False
            except Exception as e:
                log.error("Bunny PUT bytes %s unexpected: %s", remote, e)
                return False
        return False

    def get_bytes(self, remote: str) -> Optional[bytes]:
        """GET file content. Returns None if 404 or error."""
        try:
            r = self._client.get(self._url(remote))
            if r.status_code == 200:
                return r.content
            if r.status_code != 404:
                log.warning(
                    "Bunny GET %s → %d", remote, r.status_code,
                )
            return None
        except Exception as e:
            log.warning("Bunny GET %s: %s", remote, e)
            return None

    def head_object(self, remote: str) -> bool:
        """Probe object existence without downloading body. Returns True if
        Bunny responds 200, False on 404/error.

        Bunny Storage API supports HEAD on object URLs (cheaper than GET for
        existence checks — no payload transfer).
        """
        try:
            r = self._client.head(self._url(remote))
            return 200 <= r.status_code < 300
        except Exception as e:
            log.warning("Bunny HEAD %s: %s", remote, e)
            return False

    def delete_file(self, remote: str) -> bool:
        try:
            r = self._client.delete(self._url(remote))
            return 200 <= r.status_code < 300 or r.status_code == 404
        except Exception as e:
            log.warning("Bunny DELETE %s: %s", remote, e)
            return False

    def delete_prefix(self, remote_prefix: str) -> bool:
        """Recursive delete via Bunny's trailing-slash convention."""
        url = self._url(remote_prefix.rstrip("/") + "/")
        for attempt in range(self._retry):
            try:
                r = self._client.delete(url)
                if 200 <= r.status_code < 300 or r.status_code == 404:
                    return True
                # 400 = Bunny đôi khi return transient cho connection issues; treat as retryable
                if r.status_code in (400, 429, 500, 502, 503, 504):
                    if attempt < self._retry - 1:
                        self._backoff(attempt)
                        continue
                log.error(
                    "Bunny DELETE prefix %s → %d %s",
                    remote_prefix, r.status_code, r.text[:200],
                )
                return False
            except (self._httpx.TimeoutException, self._httpx.NetworkError) as e:
                if attempt < self._retry - 1:
                    self._backoff(attempt)
                    continue
                log.error(
                    "Bunny DELETE prefix %s: %s", remote_prefix, e,
                )
                return False
            except Exception as e:
                log.error(
                    "Bunny DELETE prefix %s unexpected: %s",
                    remote_prefix, e,
                )
                return False
        return False

    # ── per-frame: push toàn bộ frame dir ────────────────────────────
    def push_frame(
        self,
        local_frame_dir: Path,
        map_type: str,
        run_id: str,
        fff_label: str,
    ) -> dict:
        """Upload all files in local_frame_dir → tiles/{map}/{run}/{fff}/.

        Files are walked recursively; relative path inside frame dir is preserved.
        Concurrency: up to BUNNY_MAX_PARALLEL parallel uploads.

        Returns:
            {"total": N, "ok": K, "failed": M, "bytes": B}
        """
        files = [p for p in local_frame_dir.rglob("*") if p.is_file()]
        results = {"total": len(files), "ok": 0, "failed": 0, "bytes": 0}
        if not files:
            return results

        with ThreadPoolExecutor(max_workers=self.max_parallel) as pool:
            futs = {}
            for f in files:
                rel = f.relative_to(local_frame_dir).as_posix()
                remote = f"{map_type}/{run_id}/{fff_label}/{rel}"
                futs[pool.submit(self.upload_file, f, remote)] = (f, remote)
            for fut in as_completed(futs):
                local, remote = futs[fut]
                try:
                    ok = fut.result()
                except Exception as e:
                    log.error("Bunny push_frame future error %s: %s", remote, e)
                    ok = False
                if ok:
                    results["ok"] += 1
                    try:
                        results["bytes"] += local.stat().st_size
                    except OSError:
                        pass
                else:
                    results["failed"] += 1
                    if self._fail_fast:
                        raise RuntimeError(
                            f"Bunny upload failed: {remote}"
                        )
        return results

    # ── per-map atomic switch ─────────────────────────────────────────
    def read_pointer(self, map_type: str) -> Optional[dict]:
        """GET tiles/{map}/_current.json. Returns dict or None."""
        data = self.get_bytes(f"{map_type}/_current.json")
        if data is None:
            return None
        try:
            return json.loads(data)
        except json.JSONDecodeError as e:
            log.warning("Bunny pointer JSON decode error %s: %s", map_type, e)
            return None

    def write_pointer(
        self,
        map_type: str,
        current_run: str,
        previous_run: Optional[str] = None,
    ) -> bool:
        """PUT tiles/{map}/_current.json. Single atomic API call.

        Frontend reads this file to know which run to fetch tiles from.
        """
        payload = {
            "current_run": current_run,
            "previous_run": previous_run,
            "switched_at": datetime.now(timezone.utc).isoformat(),
        }
        return self.upload_bytes(
            json.dumps(payload, indent=2).encode("utf-8"),
            f"{map_type}/_current.json",
            content_type="application/json",
        )

    def write_timeline_metadata(self, map_type: str, timeline: dict) -> bool:
        """PUT tiles/{map}/_timeline.json — static metadata for FE.

        Contains: run_id, run_time, window_start_time, window_end_time,
        segments[], frames[] (with tiles_ready). NO now_offset_hours / is_past
        (client computes those from device UTC time).
        """
        return self.upload_bytes(
            json.dumps(timeline, indent=2).encode("utf-8"),
            f"{map_type}/_timeline.json",
            content_type="application/json",
        )

    def delete_timeline_metadata(self, map_type: str) -> bool:
        """Remove tiles/{map}/_timeline.json."""
        return self.delete_file(f"{map_type}/_timeline.json")

    def delete_pointer(self, map_type: str) -> bool:
        """Remove tiles/{map}/_current.json (used when admin deletes the only run)."""
        return self.delete_file(f"{map_type}/_current.json")

    def delete_run(self, map_type: str, run_id: str) -> bool:
        """Recursively delete tiles/{map}/{run_id}/."""
        return self.delete_prefix(f"{map_type}/{run_id}")

    # ── LIST + Bunny-side copy (for hardlink-equivalent on Bunny) ────
    def list_files(self, remote_prefix: str) -> list[str]:
        """List all files under a Bunny prefix (recursively).

        Returns list of full remote paths (relative to {prefix}/), sorted.
        Returns empty list on error or 404.
        """
        url = self._url(remote_prefix.rstrip("/") + "/")
        try:
            r = self._client.get(url)
            if r.status_code != 200:
                return []
            items = r.json() or []
        except Exception as e:
            log.warning("Bunny LIST %s: %s", remote_prefix, e)
            return []

        results: list[str] = []
        for item in items:
            name = item.get("ObjectName", "")
            if not name:
                continue
            full_path = f"{remote_prefix.rstrip('/')}/{name}"
            if item.get("IsDirectory"):
                results.extend(self.list_files(full_path))
            else:
                results.append(full_path)
        return sorted(results)

    def copy_object(self, src_remote: str, dst_remote: str) -> bool:
        """Copy one object from src to dst path on Bunny (no native COPY API).

        Implementation: GET src bytes → PUT dst. Pipeline acts as proxy but
        no local disk persistence. Bandwidth = 1× egress + 1× ingress on Bunny.
        """
        for attempt in range(self._retry):
            try:
                r = self._client.get(self._url(src_remote))
                if r.status_code == 404:
                    log.warning("Bunny copy: source not found %s", src_remote)
                    return False
                if r.status_code != 200:
                    # 400 = Bunny đôi khi return transient cho connection issues; treat as retryable
                    if r.status_code in (400, 429, 500, 502, 503, 504):
                        if attempt < self._retry - 1:
                            self._backoff(attempt)
                            continue
                    log.error(
                        "Bunny copy GET %s → %d", src_remote, r.status_code,
                    )
                    return False
                # Stream content into PUT
                data = r.content
                return self.upload_bytes(data, dst_remote)
            except (self._httpx.TimeoutException, self._httpx.NetworkError) as e:
                if attempt < self._retry - 1:
                    self._backoff(attempt)
                    continue
                log.error(
                    "Bunny copy %s → %s timeout/network: %s",
                    src_remote, dst_remote, e,
                )
                return False
            except Exception as e:
                log.error(
                    "Bunny copy %s → %s unexpected: %s",
                    src_remote, dst_remote, e,
                )
                return False
        return False

    def copy_run_subset(
        self,
        map_type: str,
        src_run: str,
        dst_run: str,
        fff_labels: list[str],
    ) -> dict:
        """Copy a list of fff labels (e.g. ['072', '084', '006_15']) from src_run
        to dst_run on Bunny side. Used for cold-zone hardlink equivalent.

        For each fff_label:
          1. LIST tiles/{map}/{src_run}/{label}/ recursively
          2. For each file: GET old → PUT new (parallel up to max_parallel)

        Returns:
            {"total": N, "ok": K, "failed": M, "frames": F,
             "frames_incomplete": [labels], "bytes": B}

        Raises RuntimeError if any frame ends up with strictly fewer files
        on the destination than were present on the source — this protects
        the atomic pointer switch from publishing an incomplete cold frame.
        """
        results: dict = {
            "total": 0, "ok": 0, "failed": 0, "frames": 0,
            "frames_incomplete": [], "bytes": 0,
        }
        for label in fff_labels:
            src_prefix = f"{map_type}/{src_run}/{label}"
            files = self.list_files(src_prefix)
            if not files:
                log.debug("Bunny copy_run_subset: no files at %s (skip)", src_prefix)
                continue
            results["frames"] += 1
            results["total"] += len(files)
            frame_ok_count = 0

            # Parallel GET+PUT
            with ThreadPoolExecutor(max_workers=self.max_parallel) as pool:
                futs = {}
                for src_path in files:
                    # Replace src_run segment with dst_run
                    # src_path = "{map}/{src_run}/{label}/{rest}"
                    rest = src_path[len(f"{map_type}/{src_run}/"):]
                    dst_path = f"{map_type}/{dst_run}/{rest}"
                    futs[pool.submit(self.copy_object, src_path, dst_path)] = (
                        src_path, dst_path,
                    )
                for fut in as_completed(futs):
                    src, dst = futs[fut]
                    try:
                        ok = fut.result()
                    except Exception as e:
                        log.error("Bunny copy future error %s: %s", src, e)
                        ok = False
                    if ok:
                        results["ok"] += 1
                        frame_ok_count += 1
                    else:
                        results["failed"] += 1
                        if self._fail_fast:
                            raise RuntimeError(
                                f"Bunny copy failed: {src} → {dst}"
                            )

            # Per-frame post-copy verification: re-LIST destination and
            # compare counts. If we copied N source files but the destination
            # has fewer than N for this label, refuse to call the run safe.
            if frame_ok_count < len(files):
                results["frames_incomplete"].append(label)
            else:
                dst_prefix = f"{map_type}/{dst_run}/{label}"
                dst_files = self.list_files(dst_prefix)
                if len(dst_files) < len(files):
                    log.warning(
                        "Bunny copy_run_subset: dst %s has %d files, src had %d — incomplete",
                        dst_prefix, len(dst_files), len(files),
                    )
                    results["frames_incomplete"].append(label)

        if results["frames_incomplete"]:
            # Surface as a hard error — caller (finalize_map_to_bunny) must
            # NOT switch the pointer to a run whose cold frames are missing.
            raise RuntimeError(
                f"Bunny cold copy incomplete: {len(results['frames_incomplete'])} frame(s) "
                f"missing files ({results['frames_incomplete'][:5]}…)"
            )
        return results


# ── singleton ──────────────────────────────────────────────────────────
_singleton: Optional[BunnyStorageClient] = None


def get_bunny_client() -> Optional[BunnyStorageClient]:
    """Returns singleton client or None if disabled/misconfigured.

    Callers must check for None and noop accordingly.
    """
    global _singleton
    cfg = get_settings()
    if not cfg.BUNNY_ENABLED:
        return None
    if not (cfg.BUNNY_STORAGE_ZONE and cfg.BUNNY_API_KEY):
        log.warning(
            "BUNNY_ENABLED=1 but missing STORAGE_ZONE or API_KEY → noop",
        )
        return None
    if _singleton is None:
        _singleton = BunnyStorageClient(cfg)
        log.info(
            "Bunny client initialized: zone=%s region=%s prefix=%s",
            cfg.BUNNY_STORAGE_ZONE, cfg.BUNNY_REGION or "global", cfg.BUNNY_PATH_PREFIX,
        )
    return _singleton


def reset_bunny_client() -> None:
    """Clear cached singleton (useful in tests or config reload)."""
    global _singleton
    if _singleton is not None:
        try:
            _singleton.close()
        except Exception:
            pass
        _singleton = None

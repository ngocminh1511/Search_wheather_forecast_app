"""worker_scaler.py — auto-size worker pools based on available CPU.

Detection priority (most accurate → least):
  1. cgroup v2 CPU quota   (e.g. Docker `cpus: 4` limit → reads cpu.max)
  2. cgroup v1 CPU quota   (older kernels)
  3. CPU_BUDGET_PERCENT of os.cpu_count()  (fallback when no container limit)

Hard floors prevent under-allocation; hard ceilings prevent the worker count
from exceeding what Python's ProcessPoolExecutor handles efficiently. Resource
guard (resource_guard.py) catches runtime spikes — this module only sizes the
*static* pool capacity.
"""

from __future__ import annotations

import logging
import math
import os

log = logging.getLogger(__name__)

# Per-pool ceiling — beyond this the pool overhead dominates the gain.
_MAX_PARSE = 16
_MAX_BUILD = 24
_MAX_CUT   = 32
_MAX_TILE  = 32
_MAX_CONCURRENCY = 5   # 5 maps total in the system; never useful to exceed 5


def _read_cgroup_v2_cpu() -> float | None:
    """Read cgroup v2 CPU quota. Returns effective core count or None."""
    try:
        with open("/sys/fs/cgroup/cpu.max", "r") as f:
            data = f.read().strip()
        parts = data.split()
        if len(parts) != 2 or parts[0] == "max":
            return None
        quota = int(parts[0])
        period = int(parts[1])
        if quota <= 0 or period <= 0:
            return None
        return quota / period
    except (FileNotFoundError, ValueError, OSError, PermissionError):
        return None


def _read_cgroup_v1_cpu() -> float | None:
    """Read cgroup v1 CPU quota. Returns effective core count or None."""
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us", "r") as f:
            quota = int(f.read().strip())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us", "r") as f:
            period = int(f.read().strip())
        if quota <= 0 or period <= 0:
            return None
        return quota / period
    except (FileNotFoundError, ValueError, OSError, PermissionError):
        return None


def _detect_cgroup_cpu() -> float | None:
    """Return cgroup CPU limit (effective core count) or None if unlimited."""
    return _read_cgroup_v2_cpu() or _read_cgroup_v1_cpu()


def _read_cgroup_memory_bytes() -> int | None:
    """Return cgroup memory limit in bytes, or None if unlimited."""
    # v2
    try:
        with open("/sys/fs/cgroup/memory.max", "r") as f:
            data = f.read().strip()
        if data == "max":
            return None
        return int(data)
    except (FileNotFoundError, ValueError, OSError, PermissionError):
        pass
    # v1
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes", "r") as f:
            val = int(f.read().strip())
        # Linux uses ~9.2 EB sentinel for unlimited
        if val > (1 << 60):
            return None
        return val
    except (FileNotFoundError, ValueError, OSError, PermissionError):
        pass
    return None


def compute_worker_budget(cpu_budget_percent: float = 50.0) -> dict:
    """Compute worker pool sizes from cgroup limit or % of host CPU.

    Args:
        cpu_budget_percent: When NO cgroup limit is set, use this % of host cores.
                            Ignored when running inside a container with --cpus.

    Returns dict with int keys: parse, build, cut, tile, concurrency.
    Plus metadata keys: _source, _effective_cores, _total_cores, _cgroup_cores.

    Memory-aware safety: if the container has a cgroup memory limit and the
    CPU-derived worker count would exceed 85% of that limit at peak, the
    effective core count is reduced so peak RAM fits. Prevents OOM-kill on
    shared hosts where memory is the actual binding constraint.
    """
    total_cores = os.cpu_count() or 4
    cgroup_cores = _detect_cgroup_cpu()
    cgroup_mem_bytes = _read_cgroup_memory_bytes()

    if cgroup_cores is not None and cgroup_cores < total_cores:
        # Container has explicit CPU limit — trust it absolutely.
        effective_f = cgroup_cores
        source = f"cgroup limit ({cgroup_cores:.1f} cores)"
    else:
        # No container limit (or limit >= host) — share host with other projects.
        budget_pct = max(10.0, min(100.0, cpu_budget_percent))
        effective_f = total_cores * (budget_pct / 100.0)
        source = f"{budget_pct:.0f}% of {total_cores} host cores"

    # Memory safety cap: don't pick worker count that would peak above 85% of
    # the container's memory limit (if one is set). Each "effective core" turns
    # into ~4.7GB of peak working set (parse + build + cut + tile workers).
    mem_capped = False
    if cgroup_mem_bytes is not None:
        mem_gb_safe = (cgroup_mem_bytes / (1024 ** 3)) * 0.85
        # Empirical: per-effective-core peak ≈ 0.95 + 1.725 + 1.3 + 0.7 ≈ 4.7 GB
        max_e_by_ram = mem_gb_safe / 4.7
        if effective_f > max_e_by_ram and max_e_by_ram >= 1.0:
            log.info(
                "Worker scaler: memory cap kicked in — reducing effective cores "
                "from %.1f to %.1f to stay under 85%% of %.1fGB container limit",
                effective_f, max_e_by_ram, cgroup_mem_bytes / (1024 ** 3),
            )
            effective_f = max_e_by_ram
            source += f" → mem-capped to {effective_f:.1f}c"
            mem_capped = True

    # Quantize to int (round down — safer for shared servers).
    e = max(1, int(effective_f))

    parse = max(2, min(_MAX_PARSE, e))
    build = max(2, min(_MAX_BUILD, int(e * 1.5)))
    cut   = max(2, min(_MAX_CUT,   e * 2))
    tile  = max(2, min(_MAX_TILE,  e * 2))
    # Concurrency: ~1 map per 3 effective cores (ceil), clamped to [2, 5].
    # 4 cores → 2 maps, 6 cores → 2, 8 cores → 3, 12 → 4, 15+ → 5.
    concurrency = max(2, min(_MAX_CONCURRENCY, math.ceil(e / 3)))

    return {
        "parse":       parse,
        "build":       build,
        "cut":         cut,
        "tile":        tile,
        "concurrency": concurrency,
        # Metadata for logging / diagnostics surfacing
        "_source":          source,
        "_effective_cores": round(effective_f, 2),
        "_total_cores":     total_cores,
        "_cgroup_cores":    round(cgroup_cores, 2) if cgroup_cores is not None else None,
        "_cgroup_memory_gb": (
            round(cgroup_mem_bytes / (1024 ** 3), 1)
            if cgroup_mem_bytes is not None else None
        ),
        "_mem_capped":     mem_capped,
    }


def log_scaling_decision(budget: dict, peak_ram_estimate_gb: float | None = None) -> None:
    """Emit a single INFO line summarising the chosen pool sizes."""
    log.info(
        "Worker auto-scale: %d host cores → effective %.1f (%s) → "
        "parse=%d build=%d cut=%d tile=%d concurrency=%d%s",
        budget["_total_cores"],
        budget["_effective_cores"],
        budget["_source"],
        budget["parse"], budget["build"], budget["cut"], budget["tile"],
        budget["concurrency"],
        f" | est peak RAM ≈ {peak_ram_estimate_gb:.1f} GB" if peak_ram_estimate_gb else "",
    )


def estimate_peak_ram_gb(budget: dict) -> float:
    """Rough peak RAM estimate when all pools are busy at once.

    Empirical per-worker memory footprints (GFS data, EPSG:3857 warp):
      parse: ~0.8 GB (GRIB array in memory)
      build: ~1.0 GB (rasterio warp + numpy canvas)
      cut:   ~0.5 GB (PIL encode + per-frame chunks)
      tile:  ~0.2 GB (small per-task overhead)
    Plus ~150 MB process startup overhead per ProcessPool worker.
    """
    overhead_per_worker_gb = 0.15
    parse_mem = budget["parse"] * (0.8 + overhead_per_worker_gb)
    build_mem = budget["build"] * (1.0 + overhead_per_worker_gb)
    cut_mem   = budget["cut"]   * (0.5 + overhead_per_worker_gb)
    # Note: parse → build → cut are stage-pipelined; in steady state all three
    # CAN be busy simultaneously across different frames, so sum them.
    return round(parse_mem + build_mem + cut_mem, 1)

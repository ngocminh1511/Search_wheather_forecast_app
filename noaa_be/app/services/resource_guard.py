import os
import shutil
import logging
from typing import Any
from ..config import get_settings

psutil: Any
try:
    import psutil  # type: ignore[no-redef]
    _HAS_PSUTIL = True
except ImportError:
    psutil = None
    _HAS_PSUTIL = False
    logging.warning("psutil not installed. RAM/CPU guards will be degraded.")

log = logging.getLogger(__name__)

def check_resources(stage: str) -> bool:
    """
    Check if the system has enough resources to proceed with a given stage.
    Returns True if OK, False if resources are exhausted.
    """
    cfg = get_settings()
    
    # 1. Disk space check
    try:
        disk_usage = shutil.disk_usage(str(cfg.BASE_DIR))
        free_gb = disk_usage.free / (1024**3)
        if free_gb < cfg.MIN_DISK_FREE_GB:
            log.warning(f"Resource Guard: Low disk space ({free_gb:.1f} GB < {cfg.MIN_DISK_FREE_GB} GB). Throttling {stage}.")
            _notify_throttle("disk", free_gb, cfg.MIN_DISK_FREE_GB, is_pct=False)
            return False
    except Exception as e:
        log.error(f"Error checking disk space: {e}")

    # 2. CPU and RAM checks
    if _HAS_PSUTIL:
        try:
            mem = psutil.virtual_memory()
            if mem.percent > cfg.MAX_RAM_PERCENT:
                log.warning(f"Resource Guard: High RAM usage ({mem.percent}% > {cfg.MAX_RAM_PERCENT}%). Throttling {stage}.")
                _notify_throttle("ram", mem.percent, cfg.MAX_RAM_PERCENT)
                return False

            cpu_percent = psutil.cpu_percent(interval=0.1)
            if cpu_percent > cfg.MAX_CPU_PERCENT:
                if stage in ("build", "cut"):
                    log.warning(f"Resource Guard: High CPU usage ({cpu_percent}% > {cfg.MAX_CPU_PERCENT}%). Throttling {stage}.")
                    _notify_throttle("cpu", cpu_percent, cfg.MAX_CPU_PERCENT)
                    return False
            iowait = 0.0
            try:
                iowait = float(getattr(psutil.cpu_times_percent(interval=0.0), "iowait", 0.0))
            except Exception:
                iowait = 0.0
            if iowait > cfg.MAX_IOWAIT_PERCENT and stage in ("build", "cut", "publish"):
                log.warning(
                    "Resource Guard: High IO wait (%.1f%% > %.1f%%). Throttling %s.",
                    iowait, cfg.MAX_IOWAIT_PERCENT, stage
                )
                _notify_throttle("iowait", iowait, cfg.MAX_IOWAIT_PERCENT)
                return False
        except Exception as e:
            log.error(f"Error checking CPU/RAM: {e}")
            
    return True

def _notify_throttle(resource: str, value: float, threshold: float, is_pct: bool = True) -> None:
    """Fire Telegram throttle warning (non-fatal, best-effort)."""
    try:
        from .pause_notifier import notify_resource_throttle
        notify_resource_throttle(resource, value, threshold)
    except Exception:
        pass


def get_resource_metrics() -> dict:
    """Return current resource usage metrics for monitoring."""
    cfg = get_settings()
    metrics = {"disk_free_gb": 0.0, "ram_percent": 0.0, "cpu_percent": 0.0, "iowait_percent": 0.0}
    
    try:
        disk_usage = shutil.disk_usage(str(cfg.BASE_DIR))
        metrics["disk_free_gb"] = round(disk_usage.free / (1024**3), 1)
    except:
        pass
        
    if _HAS_PSUTIL:
        try:
            metrics["ram_percent"] = psutil.virtual_memory().percent
            metrics["cpu_percent"] = psutil.cpu_percent()
            metrics["iowait_percent"] = float(getattr(psutil.cpu_times_percent(interval=0.0), "iowait", 0.0))
        except:
            pass
            
    return metrics

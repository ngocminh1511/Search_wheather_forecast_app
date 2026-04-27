from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional during minimal setups
    load_dotenv = None


_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"
if load_dotenv is not None and _ENV_FILE.exists():
    load_dotenv(_ENV_FILE)


class Settings:
    """
    All configuration is read from environment variables with sane defaults.
    BASE_DIR auto-detects the workspace root (two levels above this file).
    DB_DIR may be overridden independently via NOAA_DB_DIR.
    """

    def __init__(self) -> None:
        # ── Paths ──────────────────────────────────────────────────────────
        _default_base = Path(__file__).resolve().parents[2]
        self.BASE_DIR: Path = Path(
            os.getenv("NOAA_BASE_DIR", str(_default_base))
        ).resolve()
        _local_db_dir = self.BASE_DIR / "database"
        _external_db_dir = self.BASE_DIR.parent / "database"
        _default_db_dir = (
            _external_db_dir
            if not _local_db_dir.exists() and _external_db_dir.exists()
            else _local_db_dir
        )
        self.DB_DIR: Path = Path(
            os.getenv("NOAA_DB_DIR", str(_default_db_dir))
        ).resolve()
        self.DATA_DIR: Path = self.DB_DIR / "data"
        self.AVAILABLE_DIR: Path = self.DB_DIR / "available"
        self.TILES_DIR: Path = self.DB_DIR / "tiles"
        self.JSON_GRIDS_DIR: Path = self.DB_DIR / "json_grids"
        self.SCRIPTS_DIR: Path = self.BASE_DIR / "scripts"
        self.STAGING_DIR: Path = self.DB_DIR / "tiles_staging"
        self.SHARED_DB_PATH: Path = self.DB_DIR / "noaa_shared.sqlite"
        self.DATABASE_URL: str = f"sqlite:///{self.SHARED_DB_PATH}"

        # ── Tile generation ────────────────────────────────────────────────
        self.TILE_SIZE: int = int(os.getenv("TILE_SIZE", "256"))
        self.TILE_ZOOM_EAGER_MAX: int = int(
            os.getenv("TILE_ZOOM_EAGER_MAX", "8"))
        self.TILE_ZOOM_LAZY_MAX: int = int(
            os.getenv("TILE_ZOOM_LAZY_MAX", "10"))
        self.TILE_WORKERS: int = int(os.getenv("TILE_WORKERS", "8"))
        # LRU cache for lazy z=6..10 tiles (number of PNG bytes segments)
        self.TILE_CACHE_MB: int = int(os.getenv("TILE_CACHE_MB", "2048"))

        # ── Data download ──────────────────────────────────────────────────
        self.RPM_LIMIT: int = int(os.getenv("NOAA_RPM_LIMIT", "100"))
        self.DOWNLOAD_RETRIES: int = int(os.getenv("DOWNLOAD_RETRIES", "5"))

        # ── Retention ──────────────────────────────────────────────────────
        # How many GFS cycles to keep on disk (1 cycle = 6h → 4 = 24h)
        self.KEEP_CYCLES: int = int(os.getenv("KEEP_CYCLES", "4"))
        # Cloud is archive (past) - keep separate circular buffer
        self.CLOUD_KEEP_CYCLES: int = int(os.getenv("CLOUD_KEEP_CYCLES", "4"))

        # ── Pipeline Architecture (Workers & Limits) ────────────────────────
        self.MAX_DOWNLOAD_WORKERS: int = int(os.getenv("MAX_DOWNLOAD_WORKERS", "2"))
        self.MAX_PARSE_WORKERS: int = int(os.getenv("MAX_PARSE_WORKERS", "4"))
        self.MAX_BUILD_WORKERS: int = int(os.getenv("MAX_BUILD_WORKERS", "6"))
        self.MAX_CUT_WORKERS: int = int(os.getenv("MAX_CUT_WORKERS", "8"))
        self.MAX_WRITE_WORKERS: int = int(os.getenv("MAX_WRITE_WORKERS", "8"))

        # ── Resource Guards ────────────────────────────────────────────────
        self.MAX_RAM_PERCENT: float = float(os.getenv("MAX_RAM_PERCENT", "85.0"))
        self.MAX_CPU_PERCENT: float = float(os.getenv("MAX_CPU_PERCENT", "95.0"))
        self.MIN_DISK_FREE_GB: float = float(os.getenv("MIN_DISK_FREE_GB", "5.0"))

        # ── Server ─────────────────────────────────────────────────────────
        self.HOST: str = os.getenv("HOST", "0.0.0.0")
        self.PORT: int = int(os.getenv("PORT", "8000"))
        self.CORS_ORIGINS: list[str] = [
            o.strip()
            for o in os.getenv("CORS_ORIGINS", "*").split(",")
            if o.strip()
        ]
        self.API_VERSION: str = "v1"
        self.APP_VERSION: str = "1.0.0"

        # ── Scheduler ──────────────────────────────────────────────────────
        self.SCHEDULER_ENABLED: bool = os.getenv(
            "SCHEDULER_ENABLED", "false").lower() == "true"
        # Interval in minutes between cycle-check jobs
        self.CHECK_INTERVAL_MINUTES: int = int(
            os.getenv("CHECK_INTERVAL_MINUTES", "30"))
        self.SCHEDULER_INTERVAL_MINUTES: int = self.CHECK_INTERVAL_MINUTES

        # ── Smart FFF window ────────────────────────────────────────────────
        # We need to cover the "dead zone" between two consecutive GFS cycles:
        #   cycle_interval  : time between two NOAA forecast cycles (6h)
        #   noaa_upload_h   : time for NOAA to finish uploading a full cycle
        #   proc_buffer_h   : time for us to download + generate tiles
        #   user_window_h   : how far ahead users need to forecast (24h)
        #
        # Worst case: user queries at (cycle_start + upload_h + proc_h + user_window_h)
        # Example for 00z: need data up to f(6+5+1+24) = f036 to guarantee
        # users always have 24h forecast until the next cycle is ready.
        self.CYCLE_INTERVAL_H: int = 6
        self.NOAA_UPLOAD_H: int = int(os.getenv("NOAA_UPLOAD_H", "5"))
        self.PROC_BUFFER_H: int = int(os.getenv("PROC_BUFFER_H", "1"))
        self.USER_WINDOW_H: int = int(os.getenv("USER_WINDOW_H", "24"))
        self.NEEDED_FORECAST_H: int = (
            self.CYCLE_INTERVAL_H
            + self.NOAA_UPLOAD_H
            + self.PROC_BUFFER_H
            + self.USER_WINDOW_H
        )  # = 36 by default

        # ── Map types using JSON grid for animation (no PNG tiles generated) ──
        self.JSON_ONLY_MAP_TYPES: set[str] = {"wind_animation"}

        # ── Map types that are ARCHIVE (past 24h) vs FUTURE (next 24h) ──
        self.ARCHIVE_MAP_TYPES: set[str] = {"cloud_total", "cloud_layered"}

        # Ensure required directories exist
        for d in (
            self.DATA_DIR,
            self.AVAILABLE_DIR,
            self.TILES_DIR,
            self.JSON_GRIDS_DIR,
            self.SCRIPTS_DIR,
            self.STAGING_DIR,
        ):
            d.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()

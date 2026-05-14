from __future__ import annotations

import os
import json
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
        self.TILE_PROCESS_WORKERS: int = int(
            os.getenv("TILE_PROCESS_WORKERS", str(self.TILE_WORKERS))
        )
        self.TILE_MIN_PROCESS_WORKERS: int = max(
            1, int(os.getenv("TILE_MIN_PROCESS_WORKERS", "2"))
        )
        self.TILE_ADAPTIVE_THROTTLE: bool = os.getenv(
            "TILE_ADAPTIVE_THROTTLE", "true"
        ).lower() == "true"
        self.TILE_MAX_INFLIGHT_MULTIPLIER: int = max(
            1, int(os.getenv("TILE_MAX_INFLIGHT_MULTIPLIER", "2"))
        )
        self.TILE_FORMAT_DEFAULT: str = os.getenv("TILE_FORMAT_DEFAULT", "webp").lower()
        if self.TILE_FORMAT_DEFAULT not in {"png", "webp", "png8"}:
            self.TILE_FORMAT_DEFAULT = "png"
        self.TILE_WEBP_QUALITY: int = max(
            1, min(100, int(os.getenv("TILE_WEBP_QUALITY", "85")))
        )
        self.TILE_USE_PNG8_FOR_BANDED: bool = os.getenv(
            "TILE_USE_PNG8_FOR_BANDED", "false"
        ).lower() == "true"
        self.TILE_LUT_LEVELS: int = max(
            64, min(4096, int(os.getenv("TILE_LUT_LEVELS", "1024")))
        )
        self.TILE_USE_LUT: bool = os.getenv(
            "TILE_USE_LUT", "true"
        ).lower() == "true"
        self.TILE_PER_MAP_ZOOM_JSON: str = os.getenv("TILE_PER_MAP_ZOOM_JSON", "")
        # LRU cache for lazy z=6..10 tiles (number of PNG bytes segments)
        self.TILE_CACHE_MB: int = int(os.getenv("TILE_CACHE_MB", "2048"))
        self.TILE_SKIP_EXISTING_CHUNKS: bool = os.getenv(
            "TILE_SKIP_EXISTING_CHUNKS", "true"
        ).lower() == "true"
        self.TILE_PNG_COMPRESS_LEVEL: int = max(
            0, min(9, int(os.getenv("TILE_PNG_COMPRESS_LEVEL", "1")))
        )
        self.TILE_PNG_OPTIMIZE: bool = os.getenv(
            "TILE_PNG_OPTIMIZE", "false"
        ).lower() == "true"
        self.WIND_FIELD_COMPRESS_LEVEL: int = max(
            0, min(9, int(os.getenv("WIND_FIELD_COMPRESS_LEVEL", "1")))
        )
        self.WRITE_DEBUG_PNGS: bool = os.getenv(
            "WRITE_DEBUG_PNGS", "false"
        ).lower() == "true"

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
        self.MAX_PARALLEL_RUNS: int = max(
            1, int(os.getenv("MAX_PARALLEL_RUNS", "2"))
        )
        self.MAX_PARALLEL_MAP_TYPES: int = max(
            1, int(os.getenv("MAX_PARALLEL_MAP_TYPES", "2"))
        )

        # ── Resource Guards ────────────────────────────────────────────────
        self.MAX_RAM_PERCENT: float = float(os.getenv("MAX_RAM_PERCENT", "85.0"))
        self.MAX_CPU_PERCENT: float = float(os.getenv("MAX_CPU_PERCENT", "95.0"))
        self.MIN_DISK_FREE_GB: float = float(os.getenv("MIN_DISK_FREE_GB", "5.0"))
        self.MAX_IOWAIT_PERCENT: float = float(os.getenv("MAX_IOWAIT_PERCENT", "30.0"))

        # ── Server ─────────────────────────────────────────────────────────
        self.HOST: str = os.getenv("HOST", "0.0.0.0")
        self.PORT: int = int(os.getenv("PORT", "8000"))
        # CORS_ORIGINS: comma-separated. Defaults to localhost-only for safer
        # internal deploy; override via env (e.g. "http://10.0.0.5:5173,http://lan-host:8001").
        self.CORS_ORIGINS: list[str] = [
            o.strip()
            for o in os.getenv(
                "CORS_ORIGINS",
                "http://localhost:8001,http://127.0.0.1:8001",
            ).split(",")
            if o.strip()
        ]
        # Admin endpoint token. Empty disables auth (dev only). For any deploy
        # outside localhost set this to a long random string.
        self.ADMIN_API_TOKEN: str = os.getenv("ADMIN_API_TOKEN", "")
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
        # Currently empty — wind_animation removed; wind_surface uses tile-based WFLD encoding.
        self.JSON_ONLY_MAP_TYPES: set[str] = set()

        # ── Bunny.net Storage (CDN sync) ──────────────────────────────────
        # Master switch: 0 = noop (local-only mode), 1 = sync to Bunny CDN
        self.BUNNY_ENABLED: bool = bool(int(os.getenv("BUNNY_ENABLED", "0")))
        self.BUNNY_STORAGE_ZONE: str = os.getenv("BUNNY_STORAGE_ZONE", "")
        self.BUNNY_API_KEY: str = os.getenv("BUNNY_API_KEY", "")
        # Region: "" = global default, or "ny", "la", "sg", "syd", "uk", "de", "br", "jh", "se"
        self.BUNNY_REGION: str = os.getenv("BUNNY_REGION", "")
        self.BUNNY_PATH_PREFIX: str = os.getenv("BUNNY_PATH_PREFIX", "tiles")
        # CDN pull-zone URL (informational, returned by /api/v1/cdn/info for clients)
        self.BUNNY_PULL_ZONE_URL: str = os.getenv("BUNNY_PULL_ZONE_URL", "")
        self.BUNNY_MAX_PARALLEL: int = int(os.getenv("BUNNY_MAX_PARALLEL", "16"))
        self.BUNNY_RETRY_ATTEMPTS: int = int(os.getenv("BUNNY_RETRY_ATTEMPTS", "3"))
        self.BUNNY_TIMEOUT_S: int = int(os.getenv("BUNNY_TIMEOUT_S", "60"))
        # If 1, raise on any upload failure (scheduler will retry job).
        # If 0, log error and continue (pipeline non-blocking).
        self.BUNNY_FAIL_FAST: bool = bool(int(os.getenv("BUNNY_FAIL_FAST", "0")))
        # After atomic switch, delete previous run from Bunny immediately.
        self.BUNNY_DELETE_PREV_AFTER_SWITCH: bool = bool(
            int(os.getenv("BUNNY_DELETE_PREV_AFTER_SWITCH", "1"))
        )
        # Debug knob: when Bunny is canonical we wipe local DATA/TILES/STAGING/
        # JSON_GRIDS/AVAILABLE for the finalized run to save disk. Set this to
        # 1 to keep them around for manual inspection. Disk will grow over
        # time, so use only for debugging — leave =0 in steady-state.
        self.BUNNY_KEEP_LOCAL_AFTER_FINALIZE: bool = bool(
            int(os.getenv("BUNNY_KEEP_LOCAL_AFTER_FINALIZE", "0"))
        )

        # ── Telegram bot reporting ────────────────────────────────────────
        # Master switch: 0 = noop (no Telegram), 1 = send Telegram alerts
        self.TELEGRAM_ENABLED: bool = bool(int(os.getenv("TELEGRAM_ENABLED", "0")))
        self.TELEGRAM_BOT_TOKEN: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.TELEGRAM_CHAT_ID: str = os.getenv("TELEGRAM_CHAT_ID", "")
        # Verbosity:
        #   0 = chỉ critical alerts + daily report
        #   1 = +per-cycle alerts (sau khi all 6 maps done)
        #   2 = +per-map alerts (sau mỗi map done) — most verbose, default
        self.TELEGRAM_VERBOSITY: int = int(os.getenv("TELEGRAM_VERBOSITY", "2"))

        # ── Bunny Statistics API (account-level, separate from storage) ───
        # Account API key (NOT the storage zone password!) — required for analytics.
        # Get from: https://dash.bunny.net/account/settings (API Key tab).
        self.BUNNY_ACCOUNT_API_KEY: str = os.getenv("BUNNY_ACCOUNT_API_KEY", "")
        # Numeric IDs from Bunny dashboard:
        #   Pull Zone ID: visible in URL when editing pull zone
        #   Storage Zone ID: visible in URL when editing storage zone
        self.BUNNY_PULL_ZONE_ID: str = os.getenv("BUNNY_PULL_ZONE_ID", "")
        self.BUNNY_STORAGE_ZONE_ID: str = os.getenv("BUNNY_STORAGE_ZONE_ID", "")

        # ── Daily report timing (UTC) ─────────────────────────────────────
        # Default: 00:30 UTC = sau khi cycle 18z (begun 18:00 UTC) finish (~23:53 UTC)
        self.DAILY_REPORT_UTC_HOUR: int = int(os.getenv("DAILY_REPORT_UTC_HOUR", "0"))
        self.DAILY_REPORT_UTC_MINUTE: int = int(os.getenv("DAILY_REPORT_UTC_MINUTE", "30"))
        # Bunny analytics polling interval (minutes)
        self.BUNNY_ANALYTICS_POLL_MIN: int = int(os.getenv("BUNNY_ANALYTICS_POLL_MIN", "60"))

        # ── Archive map types (legacy, hiện không còn map nào archive)
        self.ARCHIVE_MAP_TYPES: set[str] = set()

        self.PRIORITY_FFF_HOT_LIST: list[int] = [
            int(v) for v in os.getenv("PRIORITY_FFF_HOT_LIST", "0,3,6,12,24,48,72").split(",")
            if v.strip().isdigit()
        ]
        try:
            parsed_zoom = json.loads(self.TILE_PER_MAP_ZOOM_JSON) if self.TILE_PER_MAP_ZOOM_JSON else {}
        except Exception:
            parsed_zoom = {}
        self.TILE_PER_MAP_ZOOM: dict[str, int] = {
            "rain_advanced": 8,
            "rain_basic": 8,
            "wind_surface": 8,
            "temperature_feels_like": 8,
            "snow_depth": 8,
            **{str(k): int(v) for k, v in parsed_zoom.items() if isinstance(v, int)},
        }

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

"""diag.py — quick diagnostic when manual pipeline appears stuck.

Usage:
    cd noaa_be
    python scripts/diag.py
"""

import json
import shutil
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import get_settings  # noqa: E402

cfg = get_settings()

print(f"\n=== Disk ({cfg.BASE_DIR}) ===")
du = shutil.disk_usage(str(cfg.BASE_DIR))
free_gb = du.free / 1024**3
print(f"  free = {free_gb:.2f} GB   (MIN_DISK_FREE_GB={cfg.MIN_DISK_FREE_GB})")
if free_gb < cfg.MIN_DISK_FREE_GB:
    print(f"  ⚠ DISK BELOW THRESHOLD — parse stage will be throttled in a sleep loop")

print(f"\n=== Deps ===")
for mod in ("scipy", "httpx", "psutil", "cfgrib", "rasterio", "mercantile", "eccodes"):
    try:
        m = __import__(mod)
        ver = getattr(m, "__version__", "?")
        print(f"  {mod:12} OK {ver}")
    except ImportError as e:
        print(f"  {mod:12} MISSING ({e})")

print(f"\n=== job_status ===")
db = sqlite3.connect(str(cfg.SHARED_DB_PATH), timeout=5)
db.row_factory = sqlite3.Row
for r in db.execute("SELECT map_type, data FROM job_status"):
    d = json.loads(r["data"])
    le = (d.get("last_error") or "")[:80]
    print(
        f"  {r['map_type']:25} status={d.get('status'):8} "
        f"started={d.get('last_started','?')[-15:]:15}  err={le}"
    )

print(f"\n=== pipeline_jobs (non-terminal + recent errors) ===")
rows = list(
    db.execute(
        "SELECT map_type, run_id, fff, product, state, error "
        "FROM pipeline_jobs WHERE state NOT IN ('READY','SKIPPED','CANCELLED') "
        "OR (state='ERROR' AND updated_at > strftime('%s','now')-3600) "
        "ORDER BY updated_at DESC LIMIT 40"
    )
)
if not rows:
    print("  (no non-terminal or recent errors)")
for r in rows:
    err = (r["error"] or "")[:120]
    print(
        f"  {r['map_type']:25} {r['run_id']:14} f{r['fff']:03d} "
        f"{r['product']:18} {r['state']:10} {err}"
    )

print(f"\n=== Delete locks ===")
try:
    from app.services.delete_service import _LOCKED_MAP_TYPES

    print(f"  locked map types: {sorted(_LOCKED_MAP_TYPES)}")
except Exception as e:
    print(f"  (cannot inspect, server must be running): {e}")

print(f"\n=== Orchestrator queues (requires server running) ===")
print("  Hit  GET /api/v1/admin/scheduler  to see executor queue depth.")
print(
    "  Or:  curl -s -H 'X-Admin-Token: <token>' "
    "http://127.0.0.1:8000/api/v1/admin/scheduler | jq"
)

db.close()

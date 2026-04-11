#!/usr/bin/env bash
# start.sh — start the noaa_be FastAPI server
# Usage: ./start.sh [--dev] [--port PORT]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# ── Defaults ──────────────────────────────────────────────────────────────
PORT="${PORT:-8000}"
HOST="${HOST:-0.0.0.0}"
WORKERS="${WORKERS:-1}"
MODE="production"

# ── Parse CLI args ─────────────────────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --dev)        MODE="dev" ;;
    --port=*)     PORT="${arg#*=}" ;;
    --port)       shift; PORT="$1" ;;
    --workers=*)  WORKERS="${arg#*=}" ;;
    *)            ;;
  esac
done

# ── Virtual env (optional but recommended) ─────────────────────────────────
if [[ -f ".venv/bin/activate" ]]; then
  # shellcheck disable=SC1091
  source .venv/bin/activate
  echo "[start.sh] venv activated: $(which python)"
fi

# ── Dependency check ───────────────────────────────────────────────────────
python - <<'EOF'
import importlib, sys
missing = []
for pkg in ["fastapi", "uvicorn", "cfgrib", "xarray", "PIL", "mercantile", "apscheduler"]:
    try:
        importlib.import_module(pkg)
    except ImportError:
        missing.append(pkg)
if missing:
    print(f"[start.sh] Missing packages: {', '.join(missing)}")
    print("[start.sh] Run: pip install -r requirements.txt")
    sys.exit(1)
EOF

# ── Launch ─────────────────────────────────────────────────────────────────
echo "[start.sh] Starting noaa_be  mode=$MODE host=$HOST port=$PORT"

if [[ "$MODE" == "dev" ]]; then
  exec uvicorn app.main:app \
    --host "$HOST" \
    --port "$PORT" \
    --reload \
    --log-level info
else
  exec uvicorn app.main:app \
    --host "$HOST" \
    --port "$PORT" \
    --workers "$WORKERS" \
    --log-level info
fi

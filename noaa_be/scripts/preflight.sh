#!/usr/bin/env bash
# preflight.sh — automated smoke check for noaa_be.
#
# Steps:
#   1. `python -m compileall` to catch syntax errors
#   2. import smoke (loads app modules)
#   3. boot uvicorn on a throwaway port, hit a couple of admin endpoints
#      and assert auth gating works
#
# Exit 0 if all good; non-zero otherwise.
#
# Override port via $PREFLIGHT_PORT (default 8011).

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PORT="${PREFLIGHT_PORT:-8011}"
TOKEN="preflight-token-$$"

red()   { printf "\033[31m%s\033[0m\n" "$*"; }
green() { printf "\033[32m%s\033[0m\n" "$*"; }
blue()  { printf "\033[34m%s\033[0m\n" "$*"; }

blue "[1/4] Syntax check (compileall)…"
python -m compileall -q app pipeline_main.py

blue "[2/4] Import smoke…"
python - <<'PY'
import importlib, sys
mods = [
    "app.routers.admin",
    "app.services.scheduler_service",
    "app.services.pipeline_orchestrator",
    "app.services.pipeline_tasks",
    "app.services.bunny_storage",
    "app.core.auth",
    "app.core.db",
    "pipeline_main",
]
for m in mods:
    importlib.import_module(m)
print("imported", len(mods), "modules")
PY

blue "[3/4] Booting uvicorn on :$PORT …"
log_file="$(mktemp -t preflight-uvicorn.XXXXXX.log)"
ADMIN_API_TOKEN="$TOKEN" \
  uvicorn pipeline_main:app --port "$PORT" --host 127.0.0.1 \
  >"$log_file" 2>&1 &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; rm -f "$log_file"' EXIT

# Wait up to 30s for the server to come up
for i in $(seq 1 30); do
  if curl -fsS -o /dev/null "http://127.0.0.1:$PORT/api/v1/admin/scheduler" \
        -H "X-Admin-Token: $TOKEN"; then
    green "  uvicorn up after ${i}s"
    break
  fi
  if ! kill -0 "$PID" 2>/dev/null; then
    red "  uvicorn died early. Log tail:"
    tail -n 50 "$log_file" || true
    exit 1
  fi
  sleep 1
done

blue "[4/4] Endpoint checks…"
fail=0

# Auth gate: no token → 401
code=$(curl -s -o /dev/null -w "%{http_code}" "http://127.0.0.1:$PORT/api/v1/admin/jobs" || true)
if [[ "$code" != "401" ]]; then
  red "  expected 401 without token, got $code"; fail=1
else
  green "  auth gate enforces 401 without token"
fi

# Auth gate: wrong token → 401
code=$(curl -s -o /dev/null -w "%{http_code}" -H "X-Admin-Token: wrong" \
         "http://127.0.0.1:$PORT/api/v1/admin/jobs" || true)
if [[ "$code" != "401" ]]; then
  red "  expected 401 with wrong token, got $code"; fail=1
else
  green "  auth gate enforces 401 with wrong token"
fi

# Correct token → 200
code=$(curl -s -o /dev/null -w "%{http_code}" -H "X-Admin-Token: $TOKEN" \
         "http://127.0.0.1:$PORT/api/v1/admin/jobs" || true)
if [[ "$code" != "200" ]]; then
  red "  expected 200 with valid token, got $code"; fail=1
else
  green "  /jobs OK with valid token"
fi

# Scheduler endpoint sanity
if ! curl -fsS -H "X-Admin-Token: $TOKEN" \
        "http://127.0.0.1:$PORT/api/v1/admin/scheduler" >/dev/null; then
  red "  /scheduler endpoint failed"; fail=1
else
  green "  /scheduler OK"
fi

# Bulk-delete validation: invalid map_type → 400
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
         -H "X-Admin-Token: $TOKEN" \
         -H "Content-Type: application/json" \
         -d '{"map_types": ["../etc"]}' \
         "http://127.0.0.1:$PORT/api/v1/admin/bulk-delete" || true)
if [[ "$code" != "400" ]]; then
  red "  expected 400 on bad bulk-delete map_type, got $code"; fail=1
else
  green "  bulk-delete validates map_type slug"
fi

if [[ "$fail" != "0" ]]; then
  red "PREFLIGHT FAILED"; exit 1
fi
green "PREFLIGHT OK"

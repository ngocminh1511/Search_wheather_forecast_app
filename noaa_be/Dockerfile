# syntax=docker/dockerfile:1.6
# ── Stage 1: builder — compile wheels with all build deps ────────────────────
FROM python:3.12-slim-bookworm AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps required to build/install:
#   - libeccodes0 + libeccodes-dev: GRIB2 reading (cfgrib wraps this C lib)
#   - libgdal-dev: rasterio wraps GDAL for reprojection
#   - build-essential + gfortran: scipy/numpy native extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gfortran \
        libeccodes0 \
        libeccodes-dev \
        libgdal-dev \
        gdal-bin \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY requirements.txt .

RUN pip install --upgrade pip wheel \
    && pip wheel --wheel-dir=/wheels -r requirements.txt


# ── Stage 2: runtime — slim image, only runtime libs ─────────────────────────
FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC

# Runtime-only system libs (no -dev, no compiler — keeps image small)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libeccodes0 \
        libgdal32 \
        curl \
        ca-certificates \
        tzdata \
    && rm -rf /var/lib/apt/lists/*

# Non-root user (UID 1000 matches common host UID on Linux servers)
RUN useradd --create-home --uid 1000 --shell /bin/bash app

WORKDIR /app

# Install wheels built in stage 1
COPY --from=builder /wheels /wheels
COPY noaa_be/requirements.txt .
RUN pip install --no-index --find-links=/wheels -r requirements.txt \
    && rm -rf /wheels

# Copy app code (respects .dockerignore)
COPY --chown=app:app noaa_be/ ./noaa_be/

# Paths that need to be mounted as volumes for persistence
RUN mkdir -p /app/database /app/logs \
    && chown -R app:app /app

USER app
WORKDIR /app/noaa_be

# Map the in-container paths the app uses:
#   NOAA_BASE_DIR=/app             → BASE_DIR (used by LOG_DIR default)
#   NOAA_DB_DIR=/app/database      → DB_DIR (data/tiles/staging/json_grids/available + sqlite)
#   LOG_DIR=/app/logs              → rotating log files
ENV NOAA_BASE_DIR=/app \
    NOAA_DB_DIR=/app/database \
    LOG_DIR=/app/logs

EXPOSE 8001

# Healthcheck hits the diagnostics snapshot (skips slow NOAA probe).
# Returns HTTP 200 with ok=true|false; "exit 1" only when curl itself fails
# (network down, app not listening). Token-protected, so we send the token
# via env var ADMIN_API_TOKEN — pass an empty token in dev to disable auth.
HEALTHCHECK --interval=60s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -fsS -H "X-Admin-Token: ${ADMIN_API_TOKEN:-}" \
        "http://localhost:8001/api/v1/admin/diagnostics?probe_noaa=false" \
        > /dev/null || exit 1

# uvicorn directly (NOT `python pipeline_main.py`) so SIGTERM is handled
# properly by uvicorn and APScheduler / orchestrator get clean shutdown.
CMD ["uvicorn", "pipeline_main:app", \
     "--host", "0.0.0.0", "--port", "8001", \
     "--workers", "1", \
     "--access-log", \
     "--loop", "uvloop", \
     "--http", "h11"]

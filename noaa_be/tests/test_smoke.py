"""Lightweight smoke tests for noaa_be.

Run with:
    cd noaa_be && pytest -q tests/

These tests don't touch the network or real Bunny, and don't need GRIB
fixtures. They exercise the bits we touched in the production-readiness
sweep: run_id parsing, the auth dependency, admin route registration, and
the small allowlist-style validators added to admin.py.
"""

from __future__ import annotations

import os

import pytest
from fastapi.testclient import TestClient


# Force a known token BEFORE importing the app so settings get the right value.
os.environ.setdefault("ADMIN_API_TOKEN", "test-token-xyz")


def _client() -> TestClient:
    # Import lazily so the env var above is honored.
    import importlib
    import app.config as _cfg
    importlib.reload(_cfg)
    import pipeline_main as pm
    importlib.reload(pm)
    return TestClient(pm.app)


# ---------------------------------------------------------------------------
# parse_run_id
# ---------------------------------------------------------------------------

def test_parse_run_id_valid():
    from app.services.availability_service import parse_run_id

    d, h = parse_run_id("20260514_06z")
    assert d == "20260514"
    assert h == 6


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "20260514",
        "20260514_06",
        "2026-05-14_06z",
        "abcdefgh_06z",
        "../etc/passwd",
    ],
)
def test_parse_run_id_rejects_invalid(bad):
    from app.services.availability_service import parse_run_id

    with pytest.raises(ValueError):
        parse_run_id(bad)


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

def test_admin_requires_token():
    c = _client()
    r = c.get("/api/v1/admin/jobs")
    assert r.status_code == 401


def test_admin_rejects_wrong_token():
    c = _client()
    r = c.get(
        "/api/v1/admin/jobs", headers={"X-Admin-Token": "obviously-wrong"}
    )
    assert r.status_code == 401


def test_admin_accepts_correct_token():
    c = _client()
    r = c.get(
        "/api/v1/admin/jobs", headers={"X-Admin-Token": "test-token-xyz"}
    )
    assert r.status_code == 200


def test_admin_accepts_token_via_query():
    # EventSource path: token must work as a query string too.
    c = _client()
    r = c.get("/api/v1/admin/jobs?admin_token=test-token-xyz")
    assert r.status_code == 200


# ---------------------------------------------------------------------------
# Validators added in the production sweep
# ---------------------------------------------------------------------------

def test_bulk_delete_rejects_path_traversal():
    c = _client()
    r = c.post(
        "/api/v1/admin/bulk-delete",
        headers={"X-Admin-Token": "test-token-xyz"},
        json={"map_types": ["../etc"]},
    )
    assert r.status_code == 400


def test_bulk_delete_rejects_empty():
    c = _client()
    r = c.post(
        "/api/v1/admin/bulk-delete",
        headers={"X-Admin-Token": "test-token-xyz"},
        json={"map_types": []},
    )
    assert r.status_code == 400


def test_benchmark_rejects_unknown_mode():
    c = _client()
    r = c.post(
        "/api/v1/admin/benchmark/start",
        headers={"X-Admin-Token": "test-token-xyz"},
        json={"mode": "evil; rm -rf"},
    )
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# Duplicate route gone
# ---------------------------------------------------------------------------

def test_no_duplicate_delete_jobs_route():
    from app.routers.admin import router

    paths = [
        (getattr(route, "path", ""), tuple(sorted(getattr(route, "methods", []))))
        for route in router.routes
        if hasattr(route, "methods")
    ]
    n_get_delete_jobs = sum(
        1 for p, m in paths if p.endswith("/delete-jobs") and "GET" in m
    )
    assert n_get_delete_jobs == 1, paths

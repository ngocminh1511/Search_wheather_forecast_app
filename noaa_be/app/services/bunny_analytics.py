"""bunny_analytics.py — Bunny.net Statistics API client + hourly polling.

Bunny ACCOUNT API (different from Storage Zone API):
  Base URL: https://api.bunny.net
  Auth: AccessKey: {BUNNY_ACCOUNT_API_KEY}  (get from dash.bunny.net/account/settings)

Endpoints used:
  GET /pullzone/{id}/statistics?dateFrom=ISO&dateTo=ISO[&hourly=true]
    → TotalRequestsServed, TotalBandwidthUsed, CacheHitRate, ErrorRate,
      RequestsPerCountry, BandwidthUsedChart (hourly buckets), etc.
  GET /storagezone/{id}
    → StorageUsed (total bytes)

Master switch: requires BUNNY_ACCOUNT_API_KEY + BUNNY_PULL_ZONE_ID + BUNNY_STORAGE_ZONE_ID.
If any is missing, get_bunny_analytics_client() returns None.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from ..config import get_settings

log = logging.getLogger(__name__)


class BunnyAnalyticsClient:
    """HTTP client for Bunny Account API (Statistics + Storage Zone meta)."""

    BASE_URL = "https://api.bunny.net"

    def __init__(self, settings) -> None:
        import httpx as _httpx
        self._httpx = _httpx
        self.api_key = settings.BUNNY_ACCOUNT_API_KEY
        self.pullzone_id = settings.BUNNY_PULL_ZONE_ID
        self.storagezone_id = settings.BUNNY_STORAGE_ZONE_ID
        self._client = _httpx.Client(
            headers={
                "AccessKey": self.api_key,
                "Accept": "application/json",
                "User-Agent": "noaa-be-bunny-analytics/1.0",
            },
            timeout=30,
        )

    def close(self) -> None:
        self._client.close()

    def get_pullzone_stats(
        self,
        date_from: datetime,
        date_to: datetime,
        hourly: bool = True,
    ) -> dict:
        """Fetch pull-zone statistics over a date range.

        Bunny endpoint: GET /statistics?pullZone={id}&dateFrom=ISO&dateTo=ISO[&hourly=true]
        (NOT /pullzone/{id}/statistics — that returns 404)

        Returns Bunny's response dict with keys:
          TotalRequestsServed, TotalBandwidthUsed, CacheHitRate (%),
          OriginShieldBandwidthUsed, RequestsPerCountry (dict),
          BandwidthUsedChart, RequestsServedChart (timestamped buckets),
          Error3xxChart, Error4xxChart, Error5xxChart, ...
        Returns {} on error.
        """
        if not self.pullzone_id:
            return {}
        params = {
            "pullZone": self.pullzone_id,
            "dateFrom": date_from.strftime("%Y-%m-%dT%H:%M:%S"),
            "dateTo":   date_to.strftime("%Y-%m-%dT%H:%M:%S"),
            "hourly":   "true" if hourly else "false",
        }
        try:
            r = self._client.get(
                f"{self.BASE_URL}/statistics",
                params=params,
            )
            if r.status_code == 200:
                return r.json() or {}
            log.warning(
                "Bunny pullzone stats → %d: %s",
                r.status_code, r.text[:200],
            )
        except Exception as e:
            log.warning("Bunny pullzone stats exception: %s", e)
        return {}

    def get_storage_used(self) -> int:
        """Fetch StorageUsed (bytes) for the configured Storage Zone."""
        if not self.storagezone_id:
            return 0
        try:
            r = self._client.get(
                f"{self.BASE_URL}/storagezone/{self.storagezone_id}"
            )
            if r.status_code == 200:
                return int(r.json().get("StorageUsed", 0))
            log.warning(
                "Bunny storagezone meta → %d: %s",
                r.status_code, r.text[:200],
            )
        except Exception as e:
            log.warning("Bunny storagezone meta exception: %s", e)
        return 0


# ── Singleton ─────────────────────────────────────────────────────────

_singleton: Optional[BunnyAnalyticsClient] = None


def get_bunny_analytics_client() -> Optional[BunnyAnalyticsClient]:
    """Returns singleton client; None if disabled or misconfigured."""
    global _singleton
    cfg = get_settings()
    if not cfg.BUNNY_ENABLED:
        return None
    if not cfg.BUNNY_ACCOUNT_API_KEY:
        return None
    if not (cfg.BUNNY_PULL_ZONE_ID and cfg.BUNNY_STORAGE_ZONE_ID):
        log.warning(
            "Bunny analytics: ACCOUNT_API_KEY set but PULL_ZONE_ID or "
            "STORAGE_ZONE_ID missing → noop"
        )
        return None
    if _singleton is None:
        _singleton = BunnyAnalyticsClient(cfg)
        log.info("Bunny analytics client initialized")
    return _singleton


def reset_bunny_analytics_client() -> None:
    global _singleton
    if _singleton is not None:
        try:
            _singleton.close()
        except Exception:
            pass
        _singleton = None


# ── Hourly polling job ────────────────────────────────────────────────

def hourly_poll_job() -> None:
    """APScheduler job: fetch last hour's Bunny pull-zone stats, insert into DB.

    Runs every BUNNY_ANALYTICS_POLL_MIN minutes.
    Truncates timestamp to top of hour for stable PRIMARY KEY.
    """
    client = get_bunny_analytics_client()
    if client is None:
        log.debug("Bunny analytics polling skipped (not configured)")
        return

    now = datetime.now(timezone.utc)
    # Round DOWN to top of current hour for the slot we're filling
    slot = now.replace(minute=0, second=0, microsecond=0)
    # Bunny returns daily-by-default; request from start of hour to now
    date_from = slot
    date_to = slot + timedelta(hours=1)

    try:
        stats = client.get_pullzone_stats(date_from, date_to, hourly=True)
    except Exception as e:
        log.error("Bunny hourly poll exception: %s", e)
        return

    if not stats:
        log.debug("Bunny hourly poll returned empty for %s", slot.isoformat())
        return

    # Extract relevant fields (Bunny field names vary by API version)
    pulls = int(stats.get("TotalRequestsServed", 0) or 0)
    bandwidth = int(stats.get("TotalBandwidthUsed", 0) or 0)
    cache_hit = float(stats.get("CacheHitRate", 0) or 0)
    error_3xx = _sum_chart(stats.get("Error3xxChart"))
    error_4xx = _sum_chart(stats.get("Error4xxChart"))
    error_5xx = _sum_chart(stats.get("Error5xxChart"))
    countries = stats.get("GeoTrafficDistribution") or stats.get("RequestsPerCountry") or {}

    # Convert RequestsPerCountry to top-N JSON
    if isinstance(countries, dict):
        sorted_countries = sorted(
            countries.items(), key=lambda kv: kv[1], reverse=True,
        )[:10]
        top_countries_json = json.dumps(
            [{"code": c, "requests": n} for c, n in sorted_countries]
        )
    else:
        top_countries_json = "[]"

    from ..core.db import insert_bunny_analytics_hourly
    try:
        insert_bunny_analytics_hourly(
            timestamp_iso=slot.isoformat(),
            pull_requests=pulls,
            bandwidth_bytes=bandwidth,
            cache_hit_ratio=cache_hit / 100.0 if cache_hit > 1 else cache_hit,  # normalize % → ratio
            error_4xx=error_4xx,
            error_5xx=error_5xx,
            top_countries_json=top_countries_json,
            raw_json=json.dumps(stats)[:50000],  # truncate
        )
        log.info(
            "Bunny hourly poll %s: pulls=%d bandwidth=%d cache_hit=%.1f%% 5xx=%d",
            slot.strftime("%H:00 UTC"), pulls, bandwidth, cache_hit, error_5xx,
        )

        # Critical alert if 5xx rate > 1%
        if pulls > 0 and (error_5xx / pulls) > 0.01:
            from .telegram_reporter import send_critical
            send_critical(
                "Bunny 5xx spike",
                f"5xx rate: {error_5xx}/{pulls} = {error_5xx/pulls*100:.2f}% in last hour\n"
                f"Hour: {slot.strftime('%Y-%m-%d %H:00 UTC')}",
            )
    except Exception as e:
        log.error("Bunny hourly poll DB write failed: %s", e)


def _sum_chart(chart) -> int:
    """Bunny returns time-series charts as dict {timestamp: value} or list of dicts.
    Return total count across the period.
    """
    if not chart:
        return 0
    if isinstance(chart, dict):
        return int(sum(chart.values() or []) or 0)
    if isinstance(chart, list):
        try:
            return int(sum(item.get("Value", 0) for item in chart))
        except (AttributeError, TypeError):
            return 0
    return 0


# ── Daily aggregation helper ─────────────────────────────────────────

def daily_summarize(date_from: datetime, date_to: datetime) -> dict:
    """Aggregate hourly rows in [date_from, date_to) into a daily summary dict.

    Returns dict consumable by telegram_reporter.format_daily_report:
      {pulls, bandwidth_bytes, cache_hit_ratio, error_4xx, error_5xx,
       top_countries (list of (code, count)), peak_hour, peak_hour_reqs}
    Returns empty dict if no rows.
    """
    from ..core.db import get_bunny_analytics_between
    rows = get_bunny_analytics_between(date_from.isoformat(), date_to.isoformat())
    if not rows:
        return {}

    total_pulls = sum((r.get("pull_requests") or 0) for r in rows)
    total_bw = sum((r.get("bandwidth_bytes") or 0) for r in rows)
    total_4xx = sum((r.get("error_4xx") or 0) for r in rows)
    total_5xx = sum((r.get("error_5xx") or 0) for r in rows)

    # Weighted cache hit ratio
    if total_pulls > 0:
        weighted_hit = sum(
            (r.get("cache_hit_ratio") or 0) * (r.get("pull_requests") or 0)
            for r in rows
        )
        cache_hit_ratio = weighted_hit / total_pulls
    else:
        cache_hit_ratio = 0

    # Top countries: aggregate across all hours
    country_counter: dict[str, int] = {}
    for r in rows:
        try:
            cs = json.loads(r.get("top_countries_json") or "[]")
            for entry in cs:
                code = entry.get("code", "?")
                country_counter[code] = country_counter.get(code, 0) + entry.get("requests", 0)
        except (json.JSONDecodeError, TypeError):
            continue
    top_countries = sorted(country_counter.items(), key=lambda kv: kv[1], reverse=True)[:5]
    # Convert to (code, percentage)
    top_countries_pct = [
        (code, count / total_pulls if total_pulls else 0)
        for code, count in top_countries
    ]

    # Peak hour (most requests)
    peak_row = max(rows, key=lambda r: r.get("pull_requests") or 0)
    peak_hour_str = "?"
    try:
        ts = datetime.fromisoformat(peak_row["timestamp"].replace("Z", "+00:00"))
        peak_hour_str = ts.strftime("%H:00 UTC")
    except (ValueError, AttributeError, KeyError):
        pass

    return {
        "pulls": total_pulls,
        "bandwidth_bytes": total_bw,
        "cache_hit_ratio": cache_hit_ratio,
        "error_4xx": total_4xx,
        "error_5xx": total_5xx,
        "top_countries": top_countries_pct,
        "peak_hour": peak_hour_str,
        "peak_hour_reqs": peak_row.get("pull_requests", 0),
    }

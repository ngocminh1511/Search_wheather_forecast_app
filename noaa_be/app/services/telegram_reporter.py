"""telegram_reporter.py — Telegram Bot API client + message formatters.

Per-map alerts: detail từng stage (download/generate/push/cold_copy/finalize)
              với start_at + end_at + duration.
Per-cycle alerts: aggregated all 6 maps after cycle completes.
Daily reports: 24h pipeline + Bunny CDN analytics.
Critical alerts: real-time on pointer fail, 5xx spikes, low disk.

Master switch: TELEGRAM_ENABLED=0 → all calls noop.
Verbosity:
  0 = critical alerts + daily report only
  1 = +per-cycle alerts
  2 = +per-map alerts (default)
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from ..config import get_settings

log = logging.getLogger(__name__)


# ── Helper formatters ─────────────────────────────────────────────────

def _fmt_duration(seconds: float | None) -> str:
    """Format seconds as '1h23m45s' / '2m05s' / '38s' / '0.5s'."""
    if seconds is None:
        return "?"
    s = float(seconds)
    if s < 1:
        return f"{s:.2f}s"
    if s < 60:
        return f"{int(s)}s"
    if s < 3600:
        m = int(s // 60)
        sec = int(s % 60)
        return f"{m}m{sec:02d}s"
    h = int(s // 3600)
    m = int((s % 3600) // 60)
    sec = int(s % 60)
    return f"{h}h{m:02d}m{sec:02d}s"


def _fmt_bytes(b: int | None) -> str:
    """Format bytes as '1.23 GB' / '456 MB' / '78 KB'."""
    if b is None or b == 0:
        return "0 B"
    n = float(b)
    if n < 1024:
        return f"{int(n)} B"
    n /= 1024
    if n < 1024:
        return f"{n:.0f} KB"
    n /= 1024
    if n < 1024:
        return f"{n:.1f} MB"
    n /= 1024
    return f"{n:.2f} GB"


def _fmt_ts(iso: str | None) -> str:
    """Format ISO timestamp as 'HH:MM:SS' (UTC)."""
    if not iso:
        return "?"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%H:%M:%S")
    except (ValueError, AttributeError):
        return iso[:8] if isinstance(iso, str) else "?"


def _fmt_ts_full(iso: str | None) -> str:
    """Format ISO timestamp as 'YYYY-MM-DD HH:MM:SS UTC'."""
    if not iso:
        return "?"
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S UTC")
    except (ValueError, AttributeError):
        return iso


def _escape_md(text: str) -> str:
    """Escape Markdown special chars for Telegram (basic mode)."""
    if not isinstance(text, str):
        return str(text)
    # Telegram Markdown (legacy): escape `_*[`
    return text.replace("_", "\\_").replace("*", "\\*").replace("[", "\\[").replace("`", "\\`")


# Map type → emoji for visual clarity
_MAP_EMOJI = {
    "rain_basic": "🌧",
    "rain_advanced": "⛈",
    "temperature_feels_like": "🌡",
    "snow_depth": "❄",
    "wind_surface": "💨",
}


# ── Telegram client ───────────────────────────────────────────────────

class TelegramReporter:
    """HTTP client for Telegram Bot API. Lazy httpx import."""

    def __init__(self, settings):
        import httpx as _httpx
        self._httpx = _httpx
        self.token = settings.TELEGRAM_BOT_TOKEN
        self.chat_id = settings.TELEGRAM_CHAT_ID
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self._client = _httpx.Client(timeout=10)

    def close(self) -> None:
        self._client.close()

    def send(
        self,
        text: str,
        parse_mode: str = "Markdown",
        silent: bool = False,
    ) -> bool:
        """Send a message. Truncate to 4096 chars (Telegram limit)."""
        if not text:
            return False
        if len(text) > 4096:
            text = text[:4090] + "\n\n[truncated]"
        try:
            r = self._client.post(
                f"{self.base_url}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True,
                    "disable_notification": silent,
                },
            )
            if 200 <= r.status_code < 300:
                return True
            log.error("Telegram send failed %d: %s", r.status_code, r.text[:300])
            return False
        except Exception as e:
            log.error("Telegram send exception: %s", e)
            return False


# ── Singleton ──────────────────────────────────────────────────────────

_singleton: Optional[TelegramReporter] = None


def get_telegram_reporter() -> Optional[TelegramReporter]:
    """Return singleton; None if disabled or misconfigured."""
    global _singleton
    cfg = get_settings()
    if not cfg.TELEGRAM_ENABLED:
        return None
    if not (cfg.TELEGRAM_BOT_TOKEN and cfg.TELEGRAM_CHAT_ID):
        log.warning("TELEGRAM_ENABLED=1 but missing BOT_TOKEN or CHAT_ID → noop")
        return None
    if _singleton is None:
        _singleton = TelegramReporter(cfg)
        log.info("Telegram reporter initialized")
    return _singleton


def reset_telegram_reporter() -> None:
    global _singleton
    if _singleton is not None:
        try:
            _singleton.close()
        except Exception:
            pass
        _singleton = None


# ── Message formatters ────────────────────────────────────────────────

def format_per_map_report(m: dict) -> str:
    """Format per-map summary message.

    Expected keys in m: matches cycle_metrics columns.
    """
    map_type = m.get("map_type", "?")
    run_id = m.get("run_id", "?")
    emoji = _MAP_EMOJI.get(map_type, "🗺")

    lines = [
        f"✅ *{_escape_md(map_type)}* — cycle `{_escape_md(run_id)}` DONE",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🕐 *Started*:  {_fmt_ts_full(m.get('started_at'))}",
        f"🏁 *Finished*: {_fmt_ts_full(m.get('finished_at'))}",
        f"⏱ *Total wall*: {_fmt_duration(m.get('total_wall_seconds'))}",
        "",
        "⏱ *Stage breakdown* (start → end | duration):",
    ]

    stages = [
        ("📥 Download ", "download"),
        ("🛠 Generate ", "generate"),
        ("📤 Push     ", "push"),
        ("❄ Cold copy", "cold_copy"),
        ("🔄 Finalize ", "finalize"),
    ]
    for label, prefix in stages:
        sa = _fmt_ts(m.get(f"{prefix}_started_at"))
        ea = _fmt_ts(m.get(f"{prefix}_finished_at"))
        dur = _fmt_duration(m.get(f"{prefix}_seconds"))
        lines.append(f"  {label}  {sa} → {ea} | {dur}")

    lines.append("")
    lines.append(
        f"📦 *Frames*: {m.get('frames_generated', 0)} gen + "
        f"{m.get('frames_cold_copied', 0)} cold = {m.get('frames_total', 0)} total"
    )

    chunks_ok = m.get("chunks_uploaded_ok", 0) or 0
    chunks_fail = m.get("chunks_uploaded_failed", 0) or 0
    chunks_total = chunks_ok + chunks_fail
    chunks_str = f"{chunks_ok}/{chunks_total} ✓" if chunks_fail == 0 else f"{chunks_ok}/{chunks_total} ⚠ {chunks_fail} failed"
    lines.append(
        f"📤 *Push*: {_fmt_bytes(m.get('bytes_uploaded'))} ({chunks_str})"
    )

    cold_get = m.get("bytes_cold_get") or 0
    cold_put = m.get("bytes_cold_put") or 0
    if cold_get or cold_put:
        lines.append(
            f"❄ *Cold copy*: {_fmt_bytes(cold_get + cold_put)} Bunny GET+PUT"
        )

    peak = m.get("peak_local_staging_bytes")
    peak_at = _fmt_ts(m.get("peak_local_staging_at"))
    if peak:
        lines.append(f"💾 *Peak local STAGING*: {_fmt_bytes(peak)} @ {peak_at}")

    bunny_size = m.get("bunny_storage_after_bytes")
    if bunny_size:
        lines.append(f"💾 *Bunny storage*: {_fmt_bytes(bunny_size)}")

    switch_ok = m.get("pointer_switch_ok")
    switch_at = _fmt_ts(m.get("pointer_switched_at"))
    if switch_ok is not None:
        ok_str = "✓" if switch_ok else "✗"
        lines.append(f"🔄 *Pointer switch*: {ok_str} at {switch_at}")

    transient = m.get("transient_errors") or 0
    permanent = m.get("permanent_errors") or 0
    if transient or permanent:
        notes = []
        if transient:
            notes.append(f"{transient} transient (auto-recovered)")
        if permanent:
            notes.append(f"⚠ {permanent} permanent")
        lines.append(f"⚠ *Issues*: {', '.join(notes)}")

    return "\n".join(lines)


def format_per_cycle_report(rows: list[dict]) -> str:
    """Format aggregated per-cycle summary (after all maps done)."""
    if not rows:
        return ""

    run_id = rows[0]["run_id"]
    starts = [r["started_at"] for r in rows if r.get("started_at")]
    ends = [r["finished_at"] for r in rows if r.get("finished_at")]
    cycle_started = min(starts) if starts else None
    cycle_finished = max(ends) if ends else None

    # Wall-clock = last finish - first start (since maps run in parallel)
    wall_seconds = None
    if cycle_started and cycle_finished:
        try:
            t0 = datetime.fromisoformat(cycle_started.replace("Z", "+00:00"))
            t1 = datetime.fromisoformat(cycle_finished.replace("Z", "+00:00"))
            wall_seconds = (t1 - t0).total_seconds()
        except (ValueError, AttributeError):
            pass

    sum_cpu = sum((r.get("total_wall_seconds") or 0) for r in rows)
    total_push = sum((r.get("bytes_uploaded") or 0) for r in rows)
    total_cold = sum(((r.get("bytes_cold_get") or 0) + (r.get("bytes_cold_put") or 0)) for r in rows)
    peak_local = max((r.get("peak_local_staging_bytes") or 0) for r in rows)
    bunny_after = max((r.get("bunny_storage_after_bytes") or 0) for r in rows)
    switches_ok = sum(1 for r in rows if r.get("pointer_switch_ok"))
    transient = sum((r.get("transient_errors") or 0) for r in rows)
    permanent = sum((r.get("permanent_errors") or 0) for r in rows)

    # Slowest map
    slowest = max(rows, key=lambda r: (r.get("total_wall_seconds") or 0))
    slowest_map = slowest.get("map_type", "?")

    lines = [
        f"🎯 *Cycle `{_escape_md(run_id)}`* COMPLETE",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"🕐 *Cycle started*:  {_fmt_ts_full(cycle_started)}",
        f"🏁 *Cycle finished*: {_fmt_ts_full(cycle_finished)}",
        f"⏱ *Total wall*: {_fmt_duration(wall_seconds)} (max-of-maps, parallel)",
        f"⏱ *Sum CPU-eq*: {_fmt_duration(sum_cpu)}",
        "",
        "*Per-map breakdown* (start → end | duration | size):",
    ]

    for r in sorted(rows, key=lambda x: x.get("total_wall_seconds") or 0):
        emoji = _MAP_EMOJI.get(r.get("map_type", ""), "🗺")
        slowest_marker = " ⭐ slowest" if r.get("map_type") == slowest_map else ""
        lines.append(
            f"  {emoji} {r.get('map_type', '?'):<22} "
            f"{_fmt_ts(r.get('started_at'))} → {_fmt_ts(r.get('finished_at'))} | "
            f"{_fmt_duration(r.get('total_wall_seconds')):>8} | "
            f"{_fmt_bytes(r.get('bytes_uploaded'))}"
            f"{slowest_marker}"
        )

    lines.extend([
        "",
        "🔄 *Pointer switches* (timestamps):",
    ])
    for r in sorted(rows, key=lambda x: x.get("map_type", "")):
        emoji = _MAP_EMOJI.get(r.get("map_type", ""), "🗺")
        ok = "✓" if r.get("pointer_switch_ok") else "✗"
        lines.append(
            f"  {emoji} {r.get('map_type', '?'):<22} {_fmt_ts(r.get('pointer_switched_at'))} {ok}"
        )
    lines.append(f"  All {switches_ok}/{len(rows)} ✓")

    lines.extend([
        "",
        f"📤 *Total push*: {_fmt_bytes(total_push)} Bunny ingress",
        f"❄ *Cold transit*: {_fmt_bytes(total_cold)} (GET+PUT)",
        f"💾 *Bunny storage*: {_fmt_bytes(bunny_after)} total",
        f"💾 *Local peak*: {_fmt_bytes(peak_local)} STAGING",
    ])

    if transient or permanent:
        issues = []
        if transient:
            issues.append(f"{transient} transient")
        if permanent:
            issues.append(f"⚠ {permanent} permanent")
        lines.append(f"⚠ *Issues*: {', '.join(issues)}")

    return "\n".join(lines)


def format_daily_report(daily: dict) -> str:
    """Format daily aggregated report.

    daily dict expected keys:
      date_iso (YYYY-MM-DD)
      cycles: list of dicts with {run_id, cycle_hour, started_at, finished_at, wall_seconds}
      pipeline: dict with frames, push_bytes, cold_get_bytes, cold_put_bytes, peak_staging,
                bunny_storage, bunny_storage_delta
      bunny: dict with pulls, bandwidth_bytes, cache_hit_ratio, error_4xx, error_5xx,
             top_countries (list of (country, pct)), peak_hour, peak_hour_reqs
      tuning: dict with slowest_cycle_id, disk_free_gb, vs_yesterday
    """
    date_iso = daily.get("date_iso", "?")
    cycles = daily.get("cycles", [])
    pipe = daily.get("pipeline", {})
    bunny = daily.get("bunny", {})
    tuning = daily.get("tuning", {})

    n_cycles = len(cycles)
    total_wall = sum((c.get("wall_seconds") or 0) for c in cycles)
    avg_wall = total_wall / n_cycles if n_cycles else 0

    lines = [
        f"📊 *Daily Report* — {_escape_md(date_iso)} (UTC)",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"*Cycles*: {n_cycles}/4 ✓ | *Total wall active*: {_fmt_duration(total_wall)}",
        "",
        "🕐 *Per-cycle timestamps*:",
    ]

    slowest_idx = -1
    slowest_dur = 0
    for i, c in enumerate(cycles):
        dur = c.get("wall_seconds") or 0
        if dur > slowest_dur:
            slowest_dur = dur
            slowest_idx = i

    for i, c in enumerate(cycles):
        marker = " ⭐ slowest" if i == slowest_idx else ""
        lines.append(
            f"  `{c.get('cycle_label', '?')}`  {_fmt_ts_full(c.get('started_at'))} → "
            f"{_fmt_ts(c.get('finished_at'))} | {_fmt_duration(c.get('wall_seconds'))}{marker}"
        )

    lines.append(f"\n  Avg wall: {_fmt_duration(avg_wall)}")

    lines.extend([
        "",
        "🛠 *Pipeline metrics* (sum of cycles):",
        f"  Frames: {pipe.get('frames_generated', 0)} hot + {pipe.get('frames_cold_copied', 0)} cold = {pipe.get('frames_total', 0)}",
        f"  Push (ingress):     {_fmt_bytes(pipe.get('push_bytes'))}",
        f"  Cold transit:       {_fmt_bytes(pipe.get('cold_get_bytes'))} GET + {_fmt_bytes(pipe.get('cold_put_bytes'))} PUT",
        f"  Local peak STAGING: {_fmt_bytes(pipe.get('peak_staging'))}",
        f"  Bunny storage:      {_fmt_bytes(pipe.get('bunny_storage'))}",
    ])
    delta = pipe.get("bunny_storage_delta")
    if delta is not None:
        lines.append(f"    Δ vs yesterday: {'+' if delta >= 0 else ''}{_fmt_bytes(abs(delta))}")

    if bunny:
        lines.extend([
            "",
            "🌐 *Frontend (Bunny CDN)*:",
            f"  Pull requests:    {bunny.get('pulls', 0):,}",
            f"  Bandwidth served: {_fmt_bytes(bunny.get('bandwidth_bytes'))}",
            f"  Cache hit ratio:  {bunny.get('cache_hit_ratio', 0)*100:.1f}%",
            f"  Errors: {bunny.get('error_4xx', 0)} (4xx) | {bunny.get('error_5xx', 0)} (5xx)",
        ])
        countries = bunny.get("top_countries") or []
        if countries:
            countries_str = " | ".join(
                f"{c} {p*100:.0f}%" if isinstance(p, float) and p < 1 else f"{c} {p}%"
                for c, p in countries[:3]
            )
            lines.append(f"  Top countries: {countries_str}")
        peak_h = bunny.get("peak_hour")
        if peak_h:
            lines.append(f"  Peak hour: {peak_h} ({bunny.get('peak_hour_reqs', 0):,} reqs)")
    else:
        lines.append("\n🌐 *Frontend*: (Bunny analytics N/A)")

    if tuning:
        lines.extend([
            "",
            "📈 *Tuning signals*:",
        ])
        if tuning.get("slowest_cycle_label"):
            lines.append(f"  Slowest cycle: {tuning['slowest_cycle_label']} ({_fmt_duration(slowest_dur)})")
        if "disk_free_gb" in tuning:
            lines.append(f"  Disk free: {tuning['disk_free_gb']:.0f} GB")
        if tuning.get("vs_yesterday"):
            lines.append(f"  vs yesterday: {tuning['vs_yesterday']}")

    return "\n".join(lines)


def format_alert_critical(title: str, details: str) -> str:
    """Format a critical alert (pointer fail, permanent error, etc.)."""
    return (
        f"🚨 *CRITICAL* — {_escape_md(title)}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"{details}\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}"
    )


def format_alert_warning(title: str, details: str) -> str:
    """Format a warning alert (transient issues, soft thresholds)."""
    return (
        f"⚠️ *Warning* — {_escape_md(title)}\n"
        f"{details}\n"
        f"🕐 {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}"
    )


# ── High-level send helpers (respect verbosity) ───────────────────────

def send_per_map(metrics: dict) -> bool:
    """Send per-map alert (only if TELEGRAM_VERBOSITY >= 2)."""
    cfg = get_settings()
    if cfg.TELEGRAM_VERBOSITY < 2:
        return True  # skip silently
    rep = get_telegram_reporter()
    if rep is None:
        return True
    return rep.send(format_per_map_report(metrics))


def send_per_cycle(rows: list[dict]) -> bool:
    """Send per-cycle alert (only if TELEGRAM_VERBOSITY >= 1)."""
    cfg = get_settings()
    if cfg.TELEGRAM_VERBOSITY < 1:
        return True
    rep = get_telegram_reporter()
    if rep is None:
        return True
    return rep.send(format_per_cycle_report(rows))


def send_daily(daily: dict) -> bool:
    """Send daily report (always sent if TELEGRAM_ENABLED)."""
    rep = get_telegram_reporter()
    if rep is None:
        return True
    return rep.send(format_daily_report(daily))


def send_critical(title: str, details: str) -> bool:
    """Send critical alert (always sent if TELEGRAM_ENABLED)."""
    rep = get_telegram_reporter()
    if rep is None:
        return True
    return rep.send(format_alert_critical(title, details))


def send_warning(title: str, details: str) -> bool:
    """Send warning alert (only if TELEGRAM_VERBOSITY >= 1)."""
    cfg = get_settings()
    if cfg.TELEGRAM_VERBOSITY < 1:
        return True
    rep = get_telegram_reporter()
    if rep is None:
        return True
    return rep.send(format_alert_warning(title, details), silent=True)

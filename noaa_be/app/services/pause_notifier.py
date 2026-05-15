"""pause_notifier.py — Deduplicating Telegram alerts for pipeline pauses.

Gửi Telegram khi pipeline dừng vì bất kỳ lý do gì.
Dedup: cùng (key, reason, detail) → chỉ gửi 1 lần.
Gửi lại khi: reason/detail thay đổi, hoặc map đã recover rồi lỗi lần khác.

Luồng hoạt động:
  1. notify_pause(map_type, reason, title, detail) gọi khi bất kỳ lỗi nào xảy ra.
  2. Module so sánh fingerprint với lần gửi trước → suppress nếu giống.
  3. clear_pause(map_type) gọi khi map chạy thành công → reset fingerprint.
  4. notify_resource_throttle() dùng cooldown riêng (15 phút) thay vì fingerprint.
"""
from __future__ import annotations

import logging
import threading
import time

log = logging.getLogger(__name__)

# ── State ─────────────────────────────────────────────────────────────────
# fingerprint cuối cùng đã gửi, keyed by map_type (hoặc global key).
_last_fingerprint: dict[str, str] = {}

# Timestamp lần cuối gửi resource-throttle per resource type.
_resource_last_ts: dict[str, float] = {}

_lock = threading.Lock()

_RESOURCE_COOLDOWN_S: float = 900.0  # 15 phút giữa các resource-throttle warnings


# ── Public API ────────────────────────────────────────────────────────────

def notify_pause(
    key: str,
    reason: str,
    title: str,
    detail: str,
    level: str = "warning",   # "warning" | "critical"
) -> bool:
    """Gửi Telegram nếu trạng thái thay đổi so với lần gửi trước.

    Args:
        key:    Thường là map_type. Dùng string tùy ý cho global events.
        reason: Mã lý do dừng (vd: "job_error", "cancelled"). Kết hợp với
                80 ký tự đầu của detail tạo fingerprint.
        title:  Tiêu đề tin nhắn Telegram.
        detail: Nội dung chi tiết (lỗi, context).
        level:  "critical" → luôn gửi dù VERBOSITY=0.
                "warning"  → chỉ gửi nếu VERBOSITY >= 1.

    Returns:
        True nếu tin nhắn được gửi (hoặc suppress thành công, không cần gửi).
        False nếu gửi thất bại.
    """
    fingerprint = f"{reason}::{detail[:80]}"

    with _lock:
        if _last_fingerprint.get(key) == fingerprint:
            return True  # cùng trạng thái, không spam
        _last_fingerprint[key] = fingerprint

    _send(title, detail, level)
    return True


def notify_resource_throttle(
    resource: str,
    value: float,
    threshold: float,
) -> bool:
    """Gửi cảnh báo throttle do tài nguyên hệ thống, tối đa 1 lần / 15 phút.

    Args:
        resource:  Tên tài nguyên ("ram", "cpu", "disk", "iowait").
        value:     Giá trị hiện tại (%).
        threshold: Ngưỡng đã vượt.
    """
    key = f"resource::{resource}"
    now = time.monotonic()

    with _lock:
        last_ts = _resource_last_ts.get(key, 0.0)
        if now - last_ts < _RESOURCE_COOLDOWN_S:
            return True  # còn trong cooldown
        _resource_last_ts[key] = now

    labels = {
        "ram":    "RAM cao",
        "cpu":    "CPU cao",
        "disk":   "Disk sắp đầy",
        "iowait": "IO Wait cao",
    }
    label = labels.get(resource, resource.upper())
    title = f"⚙️ Throttle: {label}"
    detail = (
        f"`{resource.upper()}` = *{value:.1f}%* (ngưỡng {threshold:.0f}%) "
        f"→ pipeline workers đang bị throttle cho đến khi tài nguyên giảm."
    )
    _send(title, detail, level="warning")
    return True


def clear_pause(key: str) -> None:
    """Reset fingerprint khi map recover → lần lỗi kế tiếp sẽ gửi notification mới."""
    with _lock:
        _last_fingerprint.pop(key, None)


# ── Internal ──────────────────────────────────────────────────────────────

def _send(title: str, detail: str, level: str) -> bool:
    try:
        from .telegram_reporter import (
            send_critical,
            send_warning,
        )
        if level == "critical":
            return send_critical(title, detail)
        return send_warning(title, detail)
    except Exception as exc:
        log.warning("pause_notifier._send failed: %s", exc)
        return False

"""wind_temporal.py — Keyframe + temporal-delta Zstd encoding for wind_field tiles.

Format
------
Keyframe  (every KEYFRAME_INTERVAL frames):
    magic b'WFKF' | W u16 | H u16  (8 bytes)  +  Zstd( spatial_delta(U) ∥ spatial_delta(V) )

Delta frame:
    magic b'WFTD' | W u16 | H u16 | overflow u8  (9 bytes)  +  Zstd( dU ∥ dV )
    overflow=1  →  at least one channel clipped; client must request nearest keyframe.

File names:  f{NNN:03d}.wf   (keyframe)
             f{NNN:03d}.wfd  (delta)
Path layout: {output_dir}/{z}/{x}/{y}/f{NNN}.wf[d]
"""

from __future__ import annotations

import struct
import numpy as np
from pathlib import Path

KEYFRAME_INTERVAL: int = 8  # f001,f009,f017,… are keyframes; seek cost ≤ 7 deltas

# ── lazy-init Zstd contexts (safe under fork + spawn) ─────────────────────────
_zctx = None
_dctx = None


def _get_zctx():
    global _zctx
    if _zctx is None:
        try:
            import zstandard as _z
            _zctx = _z.ZstdCompressor(level=19, threads=1)
        except ImportError:
            raise RuntimeError("zstandard not installed — run: pip install zstandard")
    return _zctx


def _get_dctx():
    global _dctx
    if _dctx is None:
        try:
            import zstandard as _z
            _dctx = _z.ZstdDecompressor()
        except ImportError:
            raise RuntimeError("zstandard not installed — run: pip install zstandard")
    return _dctx


# ── spatial delta (for keyframes only) ────────────────────────────────────────

def _spatial_delta_encode(arr: np.ndarray) -> np.ndarray:
    """2D prediction residual: horizontal diff then vertical diff of horizontal diff.
    First column and first row stored raw as anchors — no prepend-zero bug."""
    d = arr.astype(np.int16)
    dx = np.empty_like(d)
    dx[:, 0] = d[:, 0]           # anchor: first column raw
    dx[:, 1:] = np.diff(d, axis=1)
    out = np.empty_like(dx)
    out[0, :] = dx[0, :]         # anchor: first row raw
    out[1:, :] = np.diff(dx, axis=0)
    return np.clip(out, -128, 127).astype(np.int8)


def _spatial_delta_decode(encoded: np.ndarray) -> np.ndarray:
    d = encoded.astype(np.int16)
    dx = np.cumsum(d, axis=0)    # undo vertical delta
    arr = np.cumsum(dx, axis=1)  # undo horizontal delta
    return np.clip(arr, -128, 127).astype(np.int8)


# ── encode ─────────────────────────────────────────────────────────────────────

def encode_keyframe(u: np.ndarray, v: np.ndarray) -> bytes:
    """Keyframe: full spatial-delta + Zstd.  Header: 8 bytes."""
    H, W = u.shape
    payload = np.stack(
        [_spatial_delta_encode(u), _spatial_delta_encode(v)],
        axis=-1,
    ).tobytes()
    return struct.pack('<4sHH', b'WFKF', W, H) + _get_zctx().compress(payload)


def encode_temporal_delta(
    u_prev: np.ndarray, v_prev: np.ndarray,
    u_curr: np.ndarray, v_curr: np.ndarray,
) -> bytes:
    """Delta frame: temporal diff + Zstd.  Header: 9 bytes (includes overflow flag)."""
    H, W = u_curr.shape
    du = u_curr.astype(np.int16) - u_prev.astype(np.int16)
    dv = v_curr.astype(np.int16) - v_prev.astype(np.int16)

    overflow = bool(
        (du > 127).any() or (du < -127).any() or
        (dv > 127).any() or (dv < -127).any()
    )
    payload = np.stack(
        [np.clip(du, -127, 127).astype(np.int8),
         np.clip(dv, -127, 127).astype(np.int8)],
        axis=-1,
    ).tobytes()
    return struct.pack('<4sHHB', b'WFTD', W, H, int(overflow)) + _get_zctx().compress(payload)


# ── decode ─────────────────────────────────────────────────────────────────────

def decode_keyframe(data: bytes) -> tuple[np.ndarray, np.ndarray]:
    magic, W, H = struct.unpack('<4sHH', data[:8])
    assert magic == b'WFKF', f"Expected WFKF, got {magic}"
    raw = np.frombuffer(_get_dctx().decompress(data[8:]), dtype=np.int8).reshape(H, W, 2)
    return _spatial_delta_decode(raw[:, :, 0]), _spatial_delta_decode(raw[:, :, 1])


def decode_delta(
    data: bytes,
    u_prev: np.ndarray,
    v_prev: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    magic, W, H, overflow = struct.unpack('<4sHHB', data[:9])
    assert magic == b'WFTD', f"Expected WFTD, got {magic}"
    if overflow:
        raise ValueError("overflow=1 — request nearest keyframe")
    raw = np.frombuffer(_get_dctx().decompress(data[9:]), dtype=np.int8).reshape(H, W, 2)
    u = np.clip(u_prev.astype(np.int16) + raw[:, :, 0], -128, 127).astype(np.int8)
    v = np.clip(v_prev.astype(np.int16) + raw[:, :, 1], -128, 127).astype(np.int8)
    return u, v


# ── tile writer ────────────────────────────────────────────────────────────────

def generate_wind_tiles_for_run(
    frames_uv: list[tuple[np.ndarray, np.ndarray]],
    output_dir: Path,
    z: int,
    x: int,
    y: int,
) -> int:
    """Encode all temporal frames for one (z, x, y) tile.

    frames_uv: ordered list of (u_int8, v_int8) arrays for consecutive frames.
    Returns total bytes written.
    """
    tile_dir = output_dir / str(z) / str(x) / str(y)
    tile_dir.mkdir(parents=True, exist_ok=True)

    total_bytes = 0
    u_prev = v_prev = None

    for i, (u, v) in enumerate(frames_uv):
        is_kf = (i % KEYFRAME_INTERVAL == 0)
        fff = i + 1  # 1-indexed to match existing fff convention

        if is_kf:
            data = encode_keyframe(u, v)
            path = tile_dir / f"f{fff:03d}.wf"
        else:
            assert u_prev is not None and v_prev is not None
            data = encode_temporal_delta(u_prev, v_prev, u, v)
            path = tile_dir / f"f{fff:03d}.wfd"

        path.write_bytes(data)
        total_bytes += len(data)
        u_prev, v_prev = u, v

    return total_bytes


# ── nearest keyframe helper (for client seek logic) ───────────────────────────

def keyframe_for(frame_index: int) -> int:
    """Return the 1-indexed fff of the nearest preceding keyframe for frame_index (0-indexed)."""
    return (frame_index // KEYFRAME_INTERVAL) * KEYFRAME_INTERVAL + 1

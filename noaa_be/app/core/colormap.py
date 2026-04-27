from __future__ import annotations

"""
colormap.py — per-map-type colorscale configuration and RGBA rendering.

apply_colormap(values, map_type) → RGBA uint8 ndarray (H × W × 4)

Rendering rules (fixed, never per-frame auto-scaled):
  Temperature  : continuous LinearSegmentedColormap on fixed -40..45°C range.
  Snow Depth   : stepped ListedColormap + log1p nonlinear stretch + per-bin alpha.
  Rain Basic   : stepped ListedColormap + per-bin alpha, transparent below 0.1 mm.
  Advanced Precip: stepped ListedColormap on continuous combined_prate 0-90 mm/h,
                   same bilinear warp + LANCZOS upscale mechanism as rain_basic.
"""

import logging

import numpy as np
import matplotlib
import matplotlib.colors as mcolors

matplotlib.use("Agg")  # non-interactive backend — must be before pyplot import
import matplotlib.pyplot as _plt  # noqa: F401 (triggers backend init)

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Colorscale definitions
# ---------------------------------------------------------------------------

# ── Temperature feels-like ────────────────────────────────────────────────
_TEMP_STOPS_CELSIUS = [-40, -35, -25, -15, 0, 5, 15, 20, 25, 35, 40, 45]
_TEMP_STOPS_COLORS = [
    (214,   0, 255),
    (170,   0, 255),
    ( 75,   0, 255),
    (  0,  60, 255),
    (  0, 200, 255),
    (  0, 255, 180),
    ( 80, 255,   0),
    (200, 255,   0),
    (255, 220,   0),
    (255, 120,   0),
    (255,  30,   0),
    (255,   0, 180),
]
_VMIN_TEMP = -40.0
_VMAX_TEMP =  45.0


def _build_temp_cmap() -> mcolors.LinearSegmentedColormap:
    r = _VMAX_TEMP - _VMIN_TEMP
    clist = [
        ((_TEMP_STOPS_CELSIUS[i] - _VMIN_TEMP) / r,
         tuple(c / 255.0 for c in _TEMP_STOPS_COLORS[i]))
        for i in range(len(_TEMP_STOPS_CELSIUS))
    ]
    return mcolors.LinearSegmentedColormap.from_list("temp_fixed", clist)


_CUSTOM_TEMP = _build_temp_cmap()

# ── Rain Basic ─────────────────────────────────────────────────────────────
_CUSTOM_RAIN_BASIC = mcolors.ListedColormap(
    [
        "#000000",  # 0  underflow — always transparent
        "#000000",  # 1  [0,   0.1) — transparent
        "#8FD8FF",  # 2  [0.1, 0.5)
        "#57B8FF",  # 3  [0.5, 1)
        "#2B8FFF",  # 4  [1,   2)
        "#1C5EFF",  # 5  [2,   5)
        "#4A46FF",  # 6  [5,  10)
        "#7A34FF",  # 7  [10, 20)
        "#AF29F2",  # 8  [20, 35)
        "#FF2FB4",  # 9  [35, 50)
        "#FF145A",  # 10 [50,  ∞)
    ],
    name="rain_basic",
)

# ── Snow Depth ──────────────────────────────────────────────────────────────
_CUSTOM_SNOW = mcolors.ListedColormap(
    [
        "#000000",  # 0  underflow
        "#000000",  # 1  [0,   1)
        "#F0FFFF",  # 2  [1,  25)
        "#CFF6FF",  # 3  [25, 50)
        "#9BE7FF",  # 4  [50,100)
        "#68CBFF",  # 5  [100,150)
        "#5A98FF",  # 6  [150,200)
        "#6A66FF",  # 7  [200,250)
        "#9444FF",  # 8  [250,300)
        "#C72EFF",  # 9  [300,350)
        "#FF34D2",  # 10 [350,400)
        "#FF1E8C",  # 11 [400,750)
        "#D81300",  # 12 [750,∞)
    ],
    name="snow_depth",
)

# ── Cloud colormaps ──────────────────────────────────────────────────────────
_CUSTOM_CLOUD_LOW = mcolors.LinearSegmentedColormap.from_list(
    "cloud_low",
    [(0.0, "#dce8f5"), (0.5, "#c5d8ec"), (1.0, "#a8c0d8")],
)
_CUSTOM_CLOUD_MID = mcolors.LinearSegmentedColormap.from_list(
    "cloud_mid",
    [(0.0, "#f5f0ff"), (0.5, "#e0d8f0"), (1.0, "#c8bce0")],
)
_CUSTOM_CLOUD_HIGH = mcolors.LinearSegmentedColormap.from_list(
    "cloud_high",
    [(0.0, "#ffffff"), (1.0, "#f8f8f8")],
)
_CUSTOM_CLOUD_TOTAL = mcolors.LinearSegmentedColormap.from_list(
    "cloud_total",
    [
        (0.00, "#ffffff"),
        (0.25, "#f7fbff"),
        (0.50, "#eef5fb"),
        (0.75, "#dde8f2"),
        (1.00, "#c7d6e3"),
    ],
)

# ── Advanced Precipitation Base ──────────────────────────────────────────────
#
# 19-slot ListedColormap:  index 0 = dry,  1-6 = rain,  7-12 = mixed,  13-18 = snow
#
# combined_prate is a CONTINUOUS float (same mechanism as rain_basic APCP):
#   rain  zone: prate in [0.10, 30]        stored as-is
#   mixed zone: prate+30 in [30.10, 60]    offset +30
#   snow  zone: prate+60 in [60.10, 90]    offset +60
#   dry: NaN → alpha=0
#
# Pipeline (identical to rain_basic):
#   bilinear warp PRATE  +  nearest warp CRAIN/CSNOW
#   → classify on Mercator → continuous combined_prate
#   → LANCZOS tile upscale (smooth)
#   → stepped colormap → distinct bands
#
# Bins encode 18 boundaries → digitize returns 0 (dry) .. 18:
#   0       = dry (<0.10 mm/h or NaN)
#   1..6    = rain  level 1..6   [0.10 / 0.25 / 0.50 / 1.0 / 2.5 / 5.0  mm/h]
#   7..12   = mixed level 1..6   [0.10 / 0.25 / 0.50 / 1.0 / 2.5 / 5.0  mm/h]
#   13..18  = snow  level 1..6   [0.10 / 0.25 / 0.50 / 1.0 / 2.5 / 5.0  mm/h]
#
_CUSTOM_ADV_PRECIP_BASE = mcolors.ListedColormap(
    [
        "#000000",  # 0   dry — always transparent
        # ── Rain 6 levels ──────────────────────────────────────────────────
        "#A8F0A8",  # 1   [0.10, 0.50)  xanh lá rất nhạt
        "#58D058",  # 2   [0.50, 1.00)  xanh lá
        "#AADD00",  # 3   [1.00, 5.00)  vàng xanh
        "#FFE000",  # 4   [5.00,10.00)  vàng
        "#FF6600",  # 5   [10.0,25.00)  đỏ cam
        "#CC0088",  # 6   [25.0,  inf)  tím hồng
        # ── Mixed 6 levels (Purple Scale) ──────────────────────────────────
        "#E6CCFF",  # 7   [30.10,30.50) tím rất nhạt
        "#CFA3FF",  # 8   [30.50,31.00) tím nhạt
        "#A85CFF",  # 9   [31.0, 35.0)  tím
        "#8A19FF",  # 10  [35.0, 40.0)  tím đậm
        "#6000CC",  # 11  [40.0, 55.0)  tím rất đậm
        "#3B0080",  # 12  [55.0,  inf)  tím đen
        # ── Snow 6 levels ──────────────────────────────────────────────────
        "#D6FFFF",  # 13  [60.10,60.50) cyan rất nhạt
        "#80EEFF",  # 14  [60.50,61.00) cyan
        "#33CCFF",  # 15  [61.0, 65.0)  xanh sáng
        "#0088FF",  # 16  [65.0, 70.0)  xanh dương
        "#0044CC",  # 17  [70.0, 85.0)  xanh đậm
        "#001880",  # 18  [85.0,  inf)  navy
    ],
    name="advanced_precipitation_base",
)

# Debug ptype colormap
_CUSTOM_PTYPE_DEBUG = mcolors.ListedColormap(
    ["#000000", "#0080ff", "#8000ff", "#ffffff"],
    name="ptype_debug",
)

# ── Wind Surface ─────────────────────────────────────────────────────────────
_WIND_STOPS_MS = [0, 2, 4, 6, 8, 10, 15, 20, 25, 30, 40]
_WIND_COLORS = [
    "#1b2c63",  # 0
    "#224a8f",  # 2
    "#2870b8",  # 4
    "#2d9ddb",  # 6
    "#2bb5a8",  # 8
    "#27b864",  # 10
    "#82c423",  # 15
    "#d1b611",  # 20
    "#d9740b",  # 25
    "#b52212",  # 30
    "#590b1c",  # 40
]

def _build_wind_cmap() -> mcolors.LinearSegmentedColormap:
    r = 40.0
    clist = [(_WIND_STOPS_MS[i] / r, _WIND_COLORS[i]) for i in range(len(_WIND_STOPS_MS))]
    return mcolors.LinearSegmentedColormap.from_list("wind_surface", clist)

_CUSTOM_WIND_SURFACE = _build_wind_cmap()


# Shared alpha lut for 19 slots (0=dry, 1-6=rain, 7-12=mixed, 13-18=snow)
_ADV_PRECIP_ALPHA = [
    0,                              # 0  dry / below visual threshold
    190, 200, 210, 220, 232, 242,  # 1-6  rain
    190, 200, 210, 220, 232, 242,  # 7-12 mixed
    190, 200, 210, 220, 232, 242,  # 13-18 snow
]

_ADV_PRECIP_BINS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]

_CONFIGS: dict[str, dict] = {
    # ── Temperature ─────────────────────────────────────────────────────
    "temperature_feels_like": {
        "cmap": _CUSTOM_TEMP,
        "vmin": _VMIN_TEMP,
        "vmax": _VMAX_TEMP,
        "unit": "°C",
        "alpha_mode": "fixed",
        "alpha": 220,
        "norm_mode": "power",
        "norm_gamma": 0.85,
    },
    # ── Rain Basic ───────────────────────────────────────────────────────
    "rain_basic": {
        "cmap": _CUSTOM_RAIN_BASIC,
        "vmin": 0.0,
        "vmax": 50.0,
        "unit": "mm",
        "alpha_mode": "banded_alpha",
        "norm_mode": "stepped",
        "norm_bins": [0, 0.1, 0.5, 1, 2, 5, 10, 20, 35, 50],
        "alpha_per_bin": [0, 0, 175, 190, 200, 215, 220, 230, 238, 242, 245],
    },
    # ── Advanced Precipitation Base ──────────────────────────────────────
    # norm_offset=1: np.digitize(value, _ADV_PRECIP_BINS) - 1.
    # index 0 → mapped to colormap 0 (dry, alpha=0).
    # Colormap uses NEAREST upscale (discrete scalar index 0..18).
    "advanced_precipitation_base": {
        "cmap": _CUSTOM_ADV_PRECIP_BASE,
        "vmin": 0.0,
        "vmax": 18.0,
        "unit": "mm/h",
        "alpha_mode": "banded_alpha",
        "norm_mode": "stepped",
        "norm_bins": _ADV_PRECIP_BINS,
        "norm_offset": 1,
        "alpha_per_bin": _ADV_PRECIP_ALPHA,
    },
    # precip_base — URL alias used by FE (path /tiles/rain_advanced/.../precip_base/)
    "precip_base": {
        "cmap": _CUSTOM_ADV_PRECIP_BASE,
        "vmin": 0.0,
        "vmax": 18.0,
        "unit": "mm/h",
        "alpha_mode": "banded_alpha",
        "norm_mode": "stepped",
        "norm_bins": _ADV_PRECIP_BINS,
        "norm_offset": 1,
        "alpha_per_bin": _ADV_PRECIP_ALPHA,
    },
    # rain_advanced — legacy alias
    "rain_advanced": {
        "cmap": _CUSTOM_ADV_PRECIP_BASE,
        "vmin": 0.0,
        "vmax": 18.0,
        "unit": "mm/h",
        "alpha_mode": "banded_alpha",
        "norm_mode": "stepped",
        "norm_bins": _ADV_PRECIP_BINS,
        "norm_offset": 1,
        "alpha_per_bin": _ADV_PRECIP_ALPHA,
    },
    # ── Debug: ptype map ─────────────────────────────────────────────────
    "precip_debug_ptype": {
        "cmap": _CUSTOM_PTYPE_DEBUG,
        "vmin": 0.0,
        "vmax": 3.0,
        "unit": "ptype",
        "alpha_mode": "banded_alpha",
        "norm_mode": "stepped",
        "norm_bins": [0, 1, 2, 3],
        "norm_offset": 1,
        "alpha_per_bin": [0, 220, 220, 220, 220],
    },
    # ── Cloud Total ──────────────────────────────────────────────────────
    "cloud_total": {
        "cmap": _CUSTOM_CLOUD_TOTAL,
        "vmin": 0.0,
        "vmax": 100.0,
        "unit": "%",
        "alpha_mode": "cloud_scale",
        "alpha": 165,
    },
    "cloud_layered_low": {
        "cmap": _CUSTOM_CLOUD_LOW,
        "vmin": 0.0,
        "vmax": 100.0,
        "unit": "%",
        "alpha_mode": "scale",
    },
    "cloud_layered_mid": {
        "cmap": _CUSTOM_CLOUD_MID,
        "vmin": 0.0,
        "vmax": 100.0,
        "unit": "%",
        "alpha_mode": "scale",
    },
    "cloud_layered_high": {
        "cmap": _CUSTOM_CLOUD_HIGH,
        "vmin": 0.0,
        "vmax": 100.0,
        "unit": "%",
        "alpha_mode": "scale",
    },
    # ── Snow Depth ──────────────────────────────────────────────────────
    "snow_depth": {
        "cmap": _CUSTOM_SNOW,
        "vmin": 0.0,
        "vmax": 750.0,
        "unit": "cm",
        "alpha_mode": "banded_alpha",
        "norm_mode": "stepped_snow",
        "norm_bins": [0, 1, 25, 50, 100, 150, 200, 250, 300, 350, 400, 750],
        "alpha_per_bin": [0, 0, 190, 195, 205, 210, 218, 224, 230, 235, 238, 242, 245],
    },
    # wind_animation — client-side colormap, no PNG tiles
    "wind_animation": {
        "cmap": matplotlib.colormaps["viridis"],
        "vmin": 0.0,
        "vmax": 30.0,
        "unit": "m/s",
        "alpha_mode": "fixed",
    },
    # ── Wind Surface ─────────────────────────────────────────────────────
    "wind_surface": {
        "cmap": _CUSTOM_WIND_SURFACE,
        "vmin": 0.0,
        "vmax": 40.0,
        "unit": "m/s",
        "alpha_mode": "wind_scale",
        "alpha": 160, # Tăng nhẹ lên 160 vì những vùng gió yếu đã bị mờ đi
    },
}

# cloud_layered sub-type lookup
_CLOUD_LAYERED_KEY = {
    "low_cloud":  "cloud_layered_low",
    "mid_cloud":  "cloud_layered_mid",
    "high_cloud": "cloud_layered_high",
}


def _get_config(map_type: str, product: str | None = None) -> dict:
    if map_type == "cloud_layered" and product:
        key = _CLOUD_LAYERED_KEY.get(product, "cloud_layered_low")
    else:
        key = map_type
    cfg = _CONFIGS.get(key)
    if cfg is None:
        raise ValueError(
            f"No colormap config for map_type={map_type!r}, product={product!r}")
    return cfg


# ---------------------------------------------------------------------------
# Internal rendering helpers
# ---------------------------------------------------------------------------

def _snow_display_transform(values: np.ndarray) -> np.ndarray:
    return np.log1p(np.clip(values, 0.0, 750.0))


def _snow_bins_transformed(bins: list) -> np.ndarray:
    return np.log1p(np.array(bins, dtype=np.float64))


def _apply_stepped_snow(
    values: np.ndarray,
    cfg: dict,
    cmap: mcolors.ListedColormap,
) -> tuple[np.ndarray, np.ndarray]:
    n_colors = len(cmap.colors)
    trans_values = _snow_display_transform(values.astype(np.float64))
    trans_bins = _snow_bins_transformed(cfg["norm_bins"])
    idx = np.digitize(trans_values, trans_bins)
    idx = np.clip(idx, 0, n_colors - 1)
    t = idx.astype(np.float64) / (n_colors - 1)
    rgba = (cmap(t) * 255).astype(np.uint8)
    return rgba, idx


def _apply_stepped(
    values: np.ndarray,
    cfg: dict,
    cmap: mcolors.ListedColormap,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Colorize a plain stepped (isoband) colormap.

    norm_offset (default 0): subtracted from the raw digitize result.
      - offset=0: digitize returns 0 for below-first-bin, 1..N for bins.
        Used by advanced_precipitation_base where bin 0 = dry.
      - offset=1: digitize returns 1 for below-first-bin; subtract 1 → 0.
        Used by rain_basic, snow_depth, precip_debug_ptype.
    """
    n_colors = len(cmap.colors)
    bins_arr = np.array(cfg["norm_bins"], dtype=np.float64)
    offset: int = cfg.get("norm_offset", 0)
    idx = np.digitize(values.astype(np.float64), bins_arr) - offset
    idx = np.clip(idx, 0, n_colors - 1)

    rgba = np.zeros((*idx.shape, 4), dtype=np.uint8)
    for i in range(n_colors):
        mask = (idx == i)
        if mask.any():
            color = cmap.colors[i]
            if isinstance(color, str):
                rgb = mcolors.to_rgb(color)
                rgba[mask] = [int(rgb[0]*255), int(rgb[1]*255), int(rgb[2]*255), 255]
            else:
                rgba[mask] = [int(color[0]*255), int(color[1]*255), int(color[2]*255), 255]

    return rgba, idx


def _apply_continuous(
    values: np.ndarray,
    cfg: dict,
    cmap: mcolors.Colormap,
    effective_alpha: int,
) -> np.ndarray:
    vmin: float = cfg["vmin"]
    vmax: float = cfg["vmax"]
    norm_mode: str = cfg.get("norm_mode", "linear")
    t = np.clip((values - vmin) / (vmax - vmin), 0.0, 1.0)
    if norm_mode == "power":
        norm_gamma: float = cfg.get("norm_gamma", 1.0)
        t = np.power(t, norm_gamma)
    rgba = (cmap(t) * 255).astype(np.uint8)
    rgba[..., 3] = effective_alpha
    return rgba


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def apply_colormap(
    values: np.ndarray,
    map_type: str,
    product: str | None = None,
    alpha: int = 200,
) -> np.ndarray:
    """
    Map a 2-D float array → RGBA uint8 (H × W × 4).
    NaN cells always get alpha=0 (transparent).
    """
    cfg = _get_config(map_type, product)
    cmap = cfg["cmap"]
    vmin: float = cfg["vmin"]
    vmax: float = cfg["vmax"]
    alpha_mode = cfg.get("alpha_mode", "fixed")
    norm_mode = cfg.get("norm_mode", "linear")
    effective_alpha: int = cfg.get("alpha", alpha)
    nan_mask = ~np.isfinite(values)

    # ── 1. Colorize ─────────────────────────────────────────────────────
    bin_idx: np.ndarray | None = None
    if norm_mode == "stepped_snow":
        rgba, bin_idx = _apply_stepped_snow(values, cfg, cmap)
    elif norm_mode == "stepped":
        rgba, bin_idx = _apply_stepped(values, cfg, cmap)
    else:  # linear / power
        rgba = _apply_continuous(values, cfg, cmap, effective_alpha)

    # ── 2. Alpha assignment ─────────────────────────────────────────────
    if alpha_mode == "banded_alpha":
        alpha_lut = np.array(cfg["alpha_per_bin"], dtype=np.uint8)
        n_colors_a = len(cmap.colors)
        idx_a = np.clip(bin_idx, 0, n_colors_a - 1)
        rgba[..., 3] = alpha_lut[idx_a]
    elif alpha_mode == "scale":
        norm_vals = np.clip((values - vmin) / (vmax - vmin), 0.0, 1.0)
        rgba[..., 3] = (norm_vals * effective_alpha).astype(np.uint8)
    elif alpha_mode == "cloud_scale":
        visible = np.clip((values - 5.0) / 95.0, 0.0, 1.0)
        alpha_norm = np.power(visible, 0.75)
        rgba[..., 3] = (alpha_norm * effective_alpha).astype(np.uint8)
    elif alpha_mode == "wind_scale":
        # Fade out calm winds smoothly. 0.5 m/s -> fully transparent. 15 m/s -> max alpha.
        visible = np.clip((values - 0.5) / 14.5, 0.0, 1.0)
        alpha_norm = np.power(visible, 0.6) 
        rgba[..., 3] = (alpha_norm * effective_alpha).astype(np.uint8)
    elif alpha_mode == "threshold":
        threshold: float = cfg.get("alpha_threshold", 0.001)
        rgba[..., 3] = np.where(
            values > threshold, effective_alpha, 0).astype(np.uint8)
    # else: "fixed" — alpha already baked in by _apply_continuous

    rgba[nan_mask, 3] = 0  # NaN → fully transparent

    # ── 3. Debug QA ─────────────────────────────────────────────────────
    if _log.isEnabledFor(logging.DEBUG):
        valid_mask = ~nan_mask
        flat_vals = values[valid_mask].flatten()
        if flat_vals.size > 0:
            sample_idx = np.linspace(
                0, flat_vals.size - 1, min(10, flat_vals.size), dtype=int)
            flat_rgba = rgba[valid_mask].reshape(-1, 4)
            for si in sample_idx:
                v = flat_vals[si]
                r, g, b, a = flat_rgba[si]
                _log.debug("layer=%s raw=%.3f rgb=(%d,%d,%d) alpha=%d",
                           map_type, v, r, g, b, a)

            if bin_idx is not None:
                flat_idx = bin_idx[valid_mask].flatten().astype(int)
                n_c = len(cmap.colors)
                bin_counts = np.bincount(flat_idx, minlength=n_c).tolist()
                total = flat_idx.size
                bin_pct = [round(c / total * 100, 1) for c in bin_counts]
                _log.debug("layer=%s bin_counts=%s bin_pct=%s",
                           map_type, bin_counts, bin_pct)

    return rgba


def get_legend_stops(map_type: str, product: str | None = None, n_stops: int = 10) -> list[dict]:
    """Return legend stops [{value, color_hex}] for the Flutter legend widget."""
    cfg = _get_config(map_type, product)
    cmap = cfg["cmap"]
    vmin: float = cfg["vmin"]
    vmax: float = cfg["vmax"]
    norm_mode: str = cfg.get("norm_mode", "linear")

    if norm_mode in ("stepped", "stepped_snow"):
        bins_arr = cfg.get("norm_bins", [])
        n_colors = len(cmap.colors)
        stops = []
        for i, boundary in enumerate(bins_arr):
            idx = min(i + 1, n_colors - 1)
            if hasattr(cmap, "colors"):
                color = cmap.colors[idx]
                if isinstance(color, str):
                    hex_color = color
                else:
                    hex_color = "#{:02x}{:02x}{:02x}".format(
                        int(color[0]*255), int(color[1]*255), int(color[2]*255))
            else:
                t = idx / (n_colors - 1)
                r, g, b, _ = cmap(t)
                hex_color = "#{:02x}{:02x}{:02x}".format(
                    int(r*255), int(g*255), int(b*255))
            stops.append({"value": float(boundary), "color_hex": hex_color})
        return stops

    norm_gamma: float = cfg.get("norm_gamma", 1.0)
    stops = []
    for i in range(n_stops):
        t_linear = i / (n_stops - 1)
        value = vmin + t_linear * (vmax - vmin)
        t_cmap = t_linear ** norm_gamma if norm_mode == "power" else t_linear
        r, g, b, _ = cmap(t_cmap)
        hex_color = "#{:02x}{:02x}{:02x}".format(
            int(r*255), int(g*255), int(b*255))
        stops.append({"value": round(value, 2), "color_hex": hex_color})
    return stops


def get_colormap_meta(map_type: str, product: str | None = None) -> dict:
    cfg = _get_config(map_type, product)
    return {
        "vmin": cfg["vmin"],
        "vmax": cfg["vmax"],
        "unit": cfg["unit"],
        "colorscale_name": getattr(cfg["cmap"], "name", "custom"),
    }


def get_precip_metadata_json() -> dict:
    """
    Return JSON metadata for advanced_precipitation_base to guide FE decoding.
    """
    cmap = _CUSTOM_ADV_PRECIP_BASE
    mapping = {}
    
    # 0 = dry
    mapping["0"] = {"type": "dry", "level": 0, "hex": "#00000000"}
    
    # 1..6 = rain
    for i in range(1, 7):
        mapping[str(i)] = {"type": "rain", "level": i, "hex": cmap.colors[i]}
        
    # 7..12 = mixed
    for i in range(7, 13):
        mapping[str(i)] = {"type": "mixed", "level": i - 6, "hex": cmap.colors[i]}
        
    # 13..18 = snow
    for i in range(13, 19):
        mapping[str(i)] = {"type": "snow", "level": i - 12, "hex": cmap.colors[i]}
        
    return {
        "layer": "advanced_precipitation_base",
        "unit": "mm/h",
        "fe_decoder_algorithm": (
            "function getPrecipType(r, g, b, a) {\n"
            "    if (a < 15) return 'dry';\n"
            "    if (b > r && b >= g) return 'snow';\n"
            "    if (r > g && b > g) return 'mixed';\n"
            "    return 'rain';\n"
            "}"
        ),
        "index_mapping": mapping
    }

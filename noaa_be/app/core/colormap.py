from __future__ import annotations

"""
colormap.py — per-map-type colorscale configuration and RGBA rendering.

Each map type has:
  - vmin / vmax : calibrated physical range (in display unit)
  - colorscale  : matplotlib colormap name or 'custom'
  - unit        : display unit string
  - stops       : pre-computed legend stops (value, #rrggbb)

apply_colormap(values, map_type) → RGBA uint8 ndarray (H × W × 4)
"""

import numpy as np
import matplotlib
import matplotlib.colors as mcolors

matplotlib.use("Agg")  # non-interactive backend — must be before pyplot import
import matplotlib.pyplot as _plt  # noqa: F401 (triggers backend init)


# ---------------------------------------------------------------------------
# Colorscale registry
# ---------------------------------------------------------------------------

_CUSTOM_RAIN_ADVANCED = mcolors.LinearSegmentedColormap.from_list(
    "rain_adv",
    [
        (0.00, "#f5f5f5"),
        (0.10, "#9ecae1"),
        (0.30, "#3182bd"),
        (0.55, "#74c476"),
        (0.75, "#fdae6b"),
        (0.90, "#e6550d"),
        (1.00, "#a50f15"),
    ],
)


_CONFIGS: dict[str, dict] = {
    "temperature_feels_like": {
        "cmap": matplotlib.colormaps["turbo"],
        "vmin": -40.0,
        "vmax": 45.0,
        "unit": "°C",
    },
    "rain_basic": {
        "cmap": matplotlib.colormaps["Blues"],
        "vmin": 0.0,
        "vmax": 50.0,
        "unit": "mm",
    },
    "rain_advanced": {
        "cmap": _CUSTOM_RAIN_ADVANCED,
        "vmin": 0.0,
        "vmax": 20.0,
        "unit": "mm/h",
    },
    "cloud_total": {
        "cmap": matplotlib.colormaps["Greys"],
        "vmin": 0.0,
        "vmax": 100.0,
        "unit": "%",
    },
    "cloud_layered_low": {
        "cmap": matplotlib.colormaps["Blues"],
        "vmin": 0.0,
        "vmax": 100.0,
        "unit": "%",
    },
    "cloud_layered_mid": {
        "cmap": matplotlib.colormaps["Purples"],
        "vmin": 0.0,
        "vmax": 100.0,
        "unit": "%",
    },
    "cloud_layered_high": {
        "cmap": matplotlib.colormaps["Greys"],
        "vmin": 0.0,
        "vmax": 100.0,
        "unit": "%",
    },
    "snow_depth": {
        "cmap": matplotlib.colormaps["YlGnBu"],
        "vmin": 0.0,
        "vmax": 200.0,
        "unit": "cm",
    },
    # wind_animation uses client-side colormap (Viridis) — no PNG tiles
    "wind_animation": {
        "cmap": matplotlib.colormaps["viridis"],
        "vmin": 0.0,
        "vmax": 30.0,
        "unit": "m/s",
    },
}

# cloud_layered sub-type lookup
_CLOUD_LAYERED_KEY = {
    "low_cloud": "cloud_layered_low",
    "mid_cloud": "cloud_layered_mid",
    "high_cloud": "cloud_layered_high",
}


def _get_config(map_type: str, product: str | None = None) -> dict:
    if map_type == "cloud_layered" and product:
        key = _CLOUD_LAYERED_KEY.get(product, "cloud_layered_low")
    else:
        key = map_type
    cfg = _CONFIGS.get(key)
    if cfg is None:
        raise ValueError(f"No colormap config for map_type={map_type!r}, product={product!r}")
    return cfg


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
    NaN cells get alpha=0 (transparent).
    alpha: base transparency (0-255) for non-NaN cells.
    """
    cfg = _get_config(map_type, product)
    cmap = cfg["cmap"]
    vmin: float = cfg["vmin"]
    vmax: float = cfg["vmax"]

    norm = mcolors.Normalize(vmin=vmin, vmax=vmax, clip=True)
    nan_mask = ~np.isfinite(values)

    rgba = (cmap(norm(values)) * 255).astype(np.uint8)  # H × W × 4
    rgba[..., 3] = alpha
    rgba[nan_mask, 3] = 0  # transparent where no data

    return rgba


def get_legend_stops(map_type: str, product: str | None = None, n_stops: int = 10) -> list[dict]:
    """Return legend stops [{value, color_hex}] for the Flutter legend widget."""
    cfg = _get_config(map_type, product)
    cmap = cfg["cmap"]
    vmin: float = cfg["vmin"]
    vmax: float = cfg["vmax"]

    stops = []
    for i in range(n_stops):
        t = i / (n_stops - 1)
        value = vmin + t * (vmax - vmin)
        r, g, b, _ = cmap(t)
        hex_color = "#{:02x}{:02x}{:02x}".format(int(r * 255), int(g * 255), int(b * 255))
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

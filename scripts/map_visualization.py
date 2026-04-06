from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import plotly.graph_objects as go
import xarray as xr


@dataclass
class GridFrame:
    fff: int
    lon: np.ndarray
    lat: np.ndarray
    values: np.ndarray
    variable: str


def _parse_file_info(file_name: str) -> Tuple[int, str] | None:
    match = re.search(r"\.f(\d{3})\.(.+)\.grib2$", file_name)
    if not match:
        return None
    return int(match.group(1)), match.group(2)


def _get_lat_lon_names(ds: xr.Dataset) -> Tuple[str, str]:
    lat_candidates = ["latitude", "lat"]
    lon_candidates = ["longitude", "lon"]

    lat_name = next((name for name in lat_candidates if name in ds.coords), None)
    lon_name = next((name for name in lon_candidates if name in ds.coords), None)

    if lat_name is None or lon_name is None:
        raise ValueError("Cannot find latitude/longitude coordinates in dataset")

    return lat_name, lon_name


def _open_grib2_first_field(file_path: Path) -> Tuple[np.ndarray, np.ndarray, np.ndarray, str]:
    ds = xr.open_dataset(file_path, engine="cfgrib")
    try:
        if not ds.data_vars:
            raise ValueError(f"No data variable found in {file_path.name}")

        variable = list(ds.data_vars)[0]
        lat_name, lon_name = _get_lat_lon_names(ds)

        arr = ds[variable].values
        lat = ds[lat_name].values
        lon = ds[lon_name].values

        if arr.ndim > 2:
            arr = np.squeeze(arr)
        if arr.ndim != 2:
            raise ValueError(f"Unsupported array dimension {arr.ndim} for {file_path.name}")

        return lon, lat, arr, variable
    finally:
        ds.close()


def load_map_series(
    base_dir: str,
    map_type: str,
    downsample: int = 4,
) -> Dict[str, List[GridFrame]]:
    data_dir = Path(base_dir) / "data" / map_type
    files = sorted(data_dir.glob("*.grib2"))

    series: Dict[str, List[GridFrame]] = {}
    for file_path in files:
        info = _parse_file_info(file_path.name)
        if info is None:
            continue
        fff, product = info

        if map_type == "wind_animation" and product == "wind_1000m":
            continue

        try:
            lon, lat, values, variable = _open_grib2_first_field(file_path)
        except Exception as exc:  # noqa: BLE001
            print(f"Skip {file_path.name}: {exc}")
            continue

        step = max(1, int(downsample))
        lon_ds = lon[::step]
        lat_ds = lat[::step]
        values_ds = values[::step, ::step]

        series.setdefault(product, []).append(
            GridFrame(
                fff=fff,
                lon=lon_ds,
                lat=lat_ds,
                values=values_ds,
                variable=variable,
            )
        )

    for product in series:
        series[product].sort(key=lambda frame: frame.fff)

    return series


def build_slider_figure(
    map_type: str,
    product: str,
    frames_data: List[GridFrame],
    max_frames: int | None = None,
) -> go.Figure:
    if not frames_data:
        raise ValueError(f"No frames available for {map_type}/{product}")

    if max_frames is not None and max_frames > 0:
        frames_data = frames_data[:max_frames]

    first = frames_data[0]
    z_min = float(np.nanmin([np.nanmin(frame.values) for frame in frames_data]))
    z_max = float(np.nanmax([np.nanmax(frame.values) for frame in frames_data]))

    fig = go.Figure(
        data=[
            go.Heatmap(
                z=first.values,
                x=first.lon,
                y=first.lat,
                colorscale="Turbo",
                zmin=z_min,
                zmax=z_max,
                colorbar={"title": first.variable},
            )
        ]
    )

    plotly_frames = []
    steps = []

    for frame in frames_data:
        frame_name = f"fff_{frame.fff:03d}"
        plotly_frames.append(
            go.Frame(
                name=frame_name,
                data=[go.Heatmap(z=frame.values, x=frame.lon, y=frame.lat)],
                layout=go.Layout(title=f"{map_type} | {product} | f{frame.fff:03d}"),
            )
        )
        steps.append(
            {
                "label": f"f{frame.fff:03d}",
                "method": "animate",
                "args": [[frame_name], {"mode": "immediate", "frame": {"duration": 0, "redraw": True}}],
            }
        )

    fig.frames = plotly_frames
    fig.update_layout(
        title=f"{map_type} | {product} | f{first.fff:03d}",
        xaxis_title="Longitude",
        yaxis_title="Latitude",
        height=620,
        sliders=[
            {
                "currentvalue": {"prefix": "Forecast hour: "},
                "pad": {"t": 40},
                "steps": steps,
            }
        ],
        updatemenus=[
            {
                "type": "buttons",
                "showactive": False,
                "x": 0.0,
                "y": 1.12,
                "buttons": [
                    {
                        "label": "Play",
                        "method": "animate",
                        "args": [None, {"frame": {"duration": 350, "redraw": True}, "fromcurrent": True}],
                    },
                    {
                        "label": "Pause",
                        "method": "animate",
                        "args": [[None], {"mode": "immediate", "frame": {"duration": 0, "redraw": False}}],
                    },
                ],
            }
        ],
    )

    return fig


def visualize_map_type(
    base_dir: str,
    map_type: str,
    downsample: int = 4,
    max_frames: int | None = 80,
) -> List[go.Figure]:
    product_series = load_map_series(base_dir=base_dir, map_type=map_type, downsample=downsample)

    if not product_series:
        print(f"No GRIB2 files found/parsed for map_type: {map_type}")
        return []

    figures: List[go.Figure] = []
    for product, frames in product_series.items():
        if not frames:
            continue
        fig = build_slider_figure(
            map_type=map_type,
            product=product,
            frames_data=frames,
            max_frames=max_frames,
        )
        figures.append(fig)
        fig.show()

    return figures

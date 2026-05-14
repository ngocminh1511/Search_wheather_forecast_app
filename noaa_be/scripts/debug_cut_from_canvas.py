#!/usr/bin/env python3
"""Debug helper — cut tiles from an existing Mercator canvas.

Run from repository root:

    python scripts/debug_cut_from_canvas.py

This avoids the rasterio reprojection step and helps identify whether
tile absence is due to reprojection (rasterio/ecCodes) or later stages
(colormap / Pillow / file writing).
"""

from __future__ import annotations

import sys
from pathlib import Path
import traceback
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

def main() -> None:
    try:
        from app.core.tile_cutter import cut_and_save_from_canvas

        merc_path = ROOT / "test_merc.npy"
        if not merc_path.exists():
            print("Missing test_merc.npy at", merc_path)
            return

        merc = np.load(str(merc_path)).astype(np.float32)
        canvas_size = int(merc.shape[0])
        MERC_HALF = 20037508.3427892
        px_per_meter = canvas_size / (2.0 * MERC_HALF)

        out_dir = ROOT / "debug_test_out"
        out_dir.mkdir(parents=True, exist_ok=True)

        print(f"Canvas size={canvas_size}, px_per_meter={px_per_meter:.6e}")
        print("Starting cut_and_save_from_canvas (zoom 0..2) …")

        summary = cut_and_save_from_canvas(
            merc=merc,
            px_per_meter=px_per_meter,
            cmap_type="wind_surface",
            cmap_product=None,
            run_id="debug_run",
            fff=0,
            product="wind_base",
            output_dir=out_dir,
            zoom_min=0,
            zoom_max=2,
            workers=2,
            skip_existing=False,
        )

        print("Result summary:")
        print(summary)

        manifest_path = out_dir / "000" / "wind_base" / "manifest.json"
        if manifest_path.exists():
            print("Manifest contents:\n", manifest_path.read_text())
        else:
            print("No manifest written; check debug_test_out for files")

    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()

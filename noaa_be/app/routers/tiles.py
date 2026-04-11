from __future__ import annotations

"""
tiles.py — serve pre-generated PNG tiles or generate lazily for z >= TILE_ZOOM_EAGER_MAX+1.

Route: GET /tiles/{map_type}/{run_id}/{fff}/{product}/{z}/{x}/{y}.png
"""

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import FileResponse

from ..config import get_settings
from ..services.tile_generator import get_tile_lazy

router = APIRouter(prefix="/tiles")

_CACHE_CONTROL = "public, max-age=86400, immutable"


@router.get(
    "/{map_type}/{run_id}/{fff}/{product_name}/{z}/{x}/{y}.png",
    responses={200: {"content": {"image/png": {}}}},
)
def get_tile(
    map_type: str,
    run_id: str,
    fff: int,
    product_name: str,
    z: int,
    x: int,
    y: int,
) -> Response:
    cfg = get_settings()

    if z < 0 or z > 10:
        raise HTTPException(status_code=400, detail="Zoom must be 0..10")

    # Pregenerated tile
    tile_path = cfg.TILES_DIR / map_type / run_id / f"{fff:03d}" / product_name / str(z) / str(x) / f"{y}.png"
    if tile_path.exists():
        return FileResponse(
            path=str(tile_path),
            media_type="image/png",
            headers={"Cache-Control": _CACHE_CONTROL},
        )

    # Lazy generation for z > TILE_ZOOM_EAGER_MAX
    if z > cfg.TILE_ZOOM_EAGER_MAX:
        png_bytes = get_tile_lazy(
            map_type=map_type,
            run_id=run_id,
            fff=fff,
            product_name=product_name,
            z=z, x=x, y=y,
            tiles_dir=cfg.TILES_DIR,
            data_dir=cfg.DATA_DIR,
        )
        if png_bytes is None:
            raise HTTPException(status_code=404, detail="Tile not available (source GRIB missing)")
        return Response(
            content=png_bytes,
            media_type="image/png",
            headers={"Cache-Control": _CACHE_CONTROL},
        )

    raise HTTPException(status_code=404, detail="Tile not found")

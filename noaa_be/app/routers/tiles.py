from __future__ import annotations

"""noaa_be.app.routers.tiles

Serve pre-generated PNG tiles or generate lazily for z >= TILE_ZOOM_EAGER_MAX+1.

Route: GET /tiles/{map_type}/{run_id}/{fff}/{product}/{z}/{x}/{y}.png
"""

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import FileResponse

from ..config import get_settings

router = APIRouter(prefix="/tiles")

_CACHE_CONTROL = "public, max-age=86400, immutable"


@router.get(
    "/{map_type}/{run_id}/{fff}/{product_name}/{z}/{x}/{y}.{ext}",
    responses={
        200: {
            "content": {
                "image/png": {},
                "application/octet-stream": {}
            }
        }
    },
)
def get_tile(
    map_type: str,
    run_id: str,
    fff: int,
    product_name: str,
    z: int,
    x: int,
    y: int,
    ext: str,
) -> Response:
    cfg = get_settings()

    # Keep a hard safety cap at z=10 unless you explicitly relax it.
    # (Higher zooms can be very expensive and may degrade quality due to 8192px canvas cap.)
    max_z = min(int(getattr(cfg, "TILE_ZOOM_LAZY_MAX", 10)), 10)
    if z < 0 or z > max_z:
        raise HTTPException(status_code=400, detail=f"Zoom must be 0..{max_z}")

    # Pregenerated tile
    if ext not in ("png", "bin"):
        raise HTTPException(status_code=400, detail="Extension must be png or bin")
        
    tile_path = cfg.TILES_DIR / map_type / run_id / \
        f"{fff:03d}" / product_name / str(z) / str(x) / f"{y}.{ext}"
        
    media_type = "image/png" if ext == "png" else "application/octet-stream"
    
    if tile_path.exists():
        return FileResponse(
            path=str(tile_path),
            media_type=media_type,
            headers={"Cache-Control": _CACHE_CONTROL},
        )

    # API now only serves pre-built tiles. No lazy generation allowed.
    raise HTTPException(status_code=404, detail="Tile not found")

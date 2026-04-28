from __future__ import annotations

"""noaa_be.app.routers.tiles

Serve pre-generated PNG tiles or generate lazily for z >= TILE_ZOOM_EAGER_MAX+1.

Route: GET /tiles/{map_type}/{run_id}/{fff}/{product}/{z}/{x}/{y}.png
"""

from fastapi import APIRouter, HTTPException, Response
from fastapi.responses import FileResponse

from ..config import get_settings
from ..core.chunk_format import TileLookup, ChunkReader

router = APIRouter(prefix="/tiles")

_CACHE_CONTROL = "public, max-age=86400, immutable"


@router.get(
    "/{map_type}/{run_id}/{fff}/{product_name}/{z}/{x}/{y}.{ext}",
    responses={
        200: {
            "content": {
                "image/png": {},
                "image/webp": {},
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
    if ext not in ("png", "webp", "bin"):
        raise HTTPException(status_code=400, detail="Extension must be png, webp or bin")
        
    base_dir = cfg.TILES_DIR / map_type / run_id / f"{fff:03d}" / product_name
    if ext == "png":
        media_type = "image/png"
    elif ext == "webp":
        media_type = "image/webp"
    else:
        media_type = "application/octet-stream"
    
    # 1. Try to read from chunk
    chunk_path = TileLookup.get_chunk_path(base_dir, z, x, y)
    
    if chunk_path.exists():
        try:
            reader = ChunkReader(chunk_path)
            tile_bytes = reader.get_tile(x, y)
            if tile_bytes is not None:
                return Response(
                    content=tile_bytes,
                    media_type=media_type,
                    headers={"Cache-Control": _CACHE_CONTROL},
                )
        except Exception as e:
            # Fall through to check loose tiles or 404
            pass
            
    # 2. Fallback to old loose tiles (for backwards compatibility during transition)
    tile_path = base_dir / str(z) / str(x) / f"{y}.{ext}"
    if tile_path.exists():
        return FileResponse(
            path=str(tile_path),
            media_type=media_type,
            headers={"Cache-Control": _CACHE_CONTROL},
        )

    # API now only serves pre-built tiles. No lazy generation allowed.
    raise HTTPException(status_code=404, detail="Tile not found")

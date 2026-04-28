import math
import mercantile
import numpy as np
from typing import Tuple, List, Generator

# EPSG:3857 half-extent in metres
_MERC_HALF: float = 20037508.3427892
TILE_SIZE = 256
CHUNK_SIZE = 8 # 8x8 tiles per chunk

class Metatile:
    def __init__(self, z: int, cx: int, cy: int):
        self.z = z
        self.cx = cx
        self.cy = cy
        
        self.max_tiles = 2 ** z
        self.x_min = cx * CHUNK_SIZE
        self.x_max = min((cx + 1) * CHUNK_SIZE, self.max_tiles)
        self.y_min = cy * CHUNK_SIZE
        self.y_max = min((cy + 1) * CHUNK_SIZE, self.max_tiles)
        
        self.tiles_x = self.x_max - self.x_min
        self.tiles_y = self.y_max - self.y_min
        
        self.pixel_width = self.tiles_x * TILE_SIZE
        self.pixel_height = self.tiles_y * TILE_SIZE

    def is_empty(self) -> bool:
        return self.tiles_x <= 0 or self.tiles_y <= 0

    def get_mercator_bounds(self) -> Tuple[float, float, float, float]:
        """Returns (left, bottom, right, top) in EPSG:3857 metres"""
        top_left = mercantile.xy_bounds(mercantile.Tile(x=self.x_min, y=self.y_min, z=self.z))
        bottom_right = mercantile.xy_bounds(mercantile.Tile(x=self.x_max - 1, y=self.y_max - 1, z=self.z))
        
        return (top_left.left, bottom_right.bottom, bottom_right.right, top_left.top)

    def get_master_canvas_slice(self, canvas_size: int, px_per_meter: float) -> Tuple[int, int, int, int]:
        """
        Calculates the (x0, y0, x1, y1) pixel coordinates to slice from the master Mercator canvas.
        """
        left, bottom, right, top = self.get_mercator_bounds()
        
        col_left = (left + _MERC_HALF) * px_per_meter
        col_right = (right + _MERC_HALF) * px_per_meter
        row_top = (_MERC_HALF - top) * px_per_meter
        row_bot = (_MERC_HALF - bottom) * px_per_meter
        
        x0 = max(0, int(math.floor(col_left)))
        x1 = min(canvas_size, int(math.ceil(col_right)))
        y0 = max(0, int(math.floor(row_top)))
        y1 = min(canvas_size, int(math.ceil(row_bot)))
        
        return x0, y0, x1, y1

    def iter_tiles(self) -> Generator[Tuple[int, int], None, None]:
        """Yields all (x, y) tile coordinates in this metatile."""
        for y in range(self.y_min, self.y_max):
            for x in range(self.x_min, self.x_max):
                yield x, y

def iter_metatiles(zoom_min: int, zoom_max: int) -> Generator[Metatile, None, None]:
    """Yields all Metatiles for the given zoom range."""
    for z in range(zoom_min, zoom_max + 1):
        num_tiles = 2 ** z
        num_chunks = math.ceil(num_tiles / CHUNK_SIZE)
        
        for cy in range(num_chunks):
            for cx in range(num_chunks):
                yield Metatile(z, cx, cy)

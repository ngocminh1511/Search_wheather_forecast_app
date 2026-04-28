import struct
import io
import logging
from pathlib import Path
from typing import Dict, Tuple, Optional

log = logging.getLogger(__name__)

# Magic header for the chunk format
MAGIC = b"CHK1"

class ChunkWriter:
    """
    Writes a custom binary .chunk file containing multiple tiles.
    Format:
      - Magic header (4 bytes: 'CHK1')
      - Zoom level (uint8)
      - chunk_x (uint32)
      - chunk_y (uint32)
      - tile_count (uint16)
      - Index Table: tile_count * 10 bytes
          - dx (uint8): x offset within chunk
          - dy (uint8): y offset within chunk
          - offset (uint32): offset from start of file to tile data
          - length (uint32): length of tile data in bytes
      - Tile Data: concatenated byte payloads
    """
    def __init__(self, filepath: Path, z: int, chunk_x: int, chunk_y: int):
        self.filepath = filepath
        self.z = z
        self.chunk_x = chunk_x
        self.chunk_y = chunk_y
        self.tiles: Dict[Tuple[int, int], bytes] = {}

    def add_tile(self, x: int, y: int, data: bytes):
        """Add a tile to the chunk."""
        self.tiles[(x, y)] = data

    def write(self):
        """Write all added tiles to the .chunk file."""
        if not self.tiles:
            return # Don't write empty chunks
            
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        
        tile_count = len(self.tiles)
        
        # Calculate offset where tile data begins
        # 4 (magic) + 1 (z) + 4 (cx) + 4 (cy) + 2 (count) = 15 bytes for header
        header_size = 15
        index_entry_size = 10
        data_start_offset = header_size + (tile_count * index_entry_size)
        
        with open(self.filepath, "wb") as f:
            # 1. Write Header
            f.write(MAGIC)
            f.write(struct.pack("<B I I H", self.z, self.chunk_x, self.chunk_y, tile_count))
            
            # 2. Prepare Index Table and Data
            current_offset = data_start_offset
            index_bytes = bytearray()
            data_bytes = bytearray()
            
            # Sort tiles to ensure deterministic output
            for (x, y) in sorted(self.tiles.keys()):
                data = self.tiles[(x, y)]
                length = len(data)
                
                dx = x - (self.chunk_x * 8) # Assuming 8x8 metatile chunks
                dy = y - (self.chunk_y * 8)
                
                # Write index entry
                index_bytes.extend(struct.pack("<B B I I", dx, dy, current_offset, length))
                
                # Append data
                data_bytes.extend(data)
                current_offset += length
                
            # 3. Write Index Table
            f.write(index_bytes)
            
            # 4. Write Tile Data
            f.write(data_bytes)
            
        log.debug(f"Wrote chunk {self.filepath.name} with {tile_count} tiles. Size: {len(data_bytes) + data_start_offset} bytes.")

class ChunkReader:
    """Reads a specific tile from a .chunk file."""
    def __init__(self, filepath: Path):
        self.filepath = filepath
        self._index: Dict[Tuple[int, int], Tuple[int, int]] = {}
        self._initialized = False
        
    def _read_index(self):
        if self._initialized:
            return
            
        if not self.filepath.exists():
            raise FileNotFoundError(f"Chunk file not found: {self.filepath}")
            
        with open(self.filepath, "rb") as f:
            magic = f.read(4)
            if magic != MAGIC:
                raise ValueError(f"Invalid chunk magic: {magic}")
                
            header = f.read(11)
            if len(header) < 11:
                raise ValueError("Incomplete chunk header")
                
            z, cx, cy, count = struct.unpack("<B I I H", header)
            
            # Read index table
            index_data = f.read(count * 10)
            for i in range(count):
                entry_start = i * 10
                dx, dy, offset, length = struct.unpack("<B B I I", index_data[entry_start:entry_start+10])
                # Store absolute x, y
                x = cx * 8 + dx
                y = cy * 8 + dy
                self._index[(x, y)] = (offset, length)
                
        self._initialized = True

    def get_tile(self, x: int, y: int) -> Optional[bytes]:
        """Extract tile bytes. Returns None if tile not in chunk."""
        self._read_index()
        
        if (x, y) not in self._index:
            return None
            
        offset, length = self._index[(x, y)]
        
        with open(self.filepath, "rb") as f:
            f.seek(offset)
            return f.read(length)

class TileLookup:
    """Helper to map a z/x/y tile request to the corresponding chunk file."""
    
    # 8x8 tiles per chunk = 64 tiles. This matches the metatile dimension (2048x2048px).
    CHUNK_SIZE = 8 
    
    @classmethod
    def get_chunk_coords(cls, x: int, y: int) -> Tuple[int, int]:
        return x // cls.CHUNK_SIZE, y // cls.CHUNK_SIZE
        
    @classmethod
    def get_chunk_path(cls, base_dir: Path, z: int, x: int, y: int) -> Path:
        """
        Returns the path to the chunk file that would contain this tile.
        Format: base_dir / str(z) / f"{chunk_x}_{chunk_y}.chunk"
        """
        cx, cy = cls.get_chunk_coords(x, y)
        return base_dir / str(z) / f"{cx}_{cy}.chunk"

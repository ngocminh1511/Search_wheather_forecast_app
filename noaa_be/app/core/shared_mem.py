import logging
import numpy as np
try:
    from multiprocessing import shared_memory
    _HAS_SHM = True
except ImportError:
    _HAS_SHM = False

log = logging.getLogger(__name__)

class SharedCanvas:
    """
    Helper to manage multiprocessing shared memory for large numpy arrays.
    Used to pass the 256MB Mercator canvas from Build workers to Cut workers
    without pickling overhead or RAM duplication.
    """
    def __init__(self, name: str, shape: tuple, dtype: np.dtype):
        self.name = name
        self.shape = shape
        self.dtype = dtype
        self.shm = None
        self.array = None

    @classmethod
    def create(cls, array: np.ndarray) -> 'SharedCanvas':
        if not _HAS_SHM:
            raise RuntimeError("multiprocessing.shared_memory is not available")
            
        shm = shared_memory.SharedMemory(create=True, size=array.nbytes)
        shared_arr = np.ndarray(array.shape, dtype=array.dtype, buffer=shm.buf)
        np.copyto(shared_arr, array)
        
        instance = cls(shm.name, array.shape, array.dtype)
        instance.shm = shm
        instance.array = shared_arr
        return instance

    @classmethod
    def attach(cls, name: str, shape: tuple, dtype: np.dtype) -> 'SharedCanvas':
        if not _HAS_SHM:
            raise RuntimeError("multiprocessing.shared_memory is not available")
            
        shm = shared_memory.SharedMemory(name=name)
        shared_arr = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
        
        instance = cls(name, shape, dtype)
        instance.shm = shm
        instance.array = shared_arr
        return instance

    def close(self):
        """Close access to the shared memory from this process."""
        if self.shm is not None:
            self.shm.close()
            self.array = None

    def unlink(self):
        """Request the OS to destroy the shared memory block (call only once when completely done)."""
        if self.shm is not None:
            try:
                self.shm.unlink()
            except FileNotFoundError:
                pass # Already unlinked
            self.shm = None

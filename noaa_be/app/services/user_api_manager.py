import os
import sys
import time
import queue
import logging
import threading
import subprocess
from collections import deque
from pathlib import Path
from typing import Dict, Any, List

# Maintain maximum of 2000 lines of logs
_LOGS_MAX_LEN = 2000
_log_buffer = deque(maxlen=_LOGS_MAX_LEN)
_process: subprocess.Popen = None
_monitor_thread: threading.Thread = None
_stop_event: threading.Event = threading.Event()
_start_time: float = 0.0

logger = logging.getLogger(__name__)

def _read_stream(stream, prefix=""):
    """Reads lines from a stream and adds them to the log buffer."""
    for line in iter(stream.readline, ""):
        if line:
            line_str = line.strip()
            # If the output comes from another log formatter, try to keep it clean
            _log_buffer.append(f"{prefix}{line_str}")
    stream.close()

def _process_monitor():
    """Background thread to read subprocess output non-blocking."""
    global _process
    if not _process:
        return
        
    try:
        # Use two threads to read stdout and stderr to prevent deadlocks
        out_thread = threading.Thread(target=_read_stream, args=(_process.stdout, ""), daemon=True)
        err_thread = threading.Thread(target=_read_stream, args=(_process.stderr, "ERROR: "), daemon=True)
        
        out_thread.start()
        err_thread.start()
        
        # Wait for the process to exit or be stopped
        while _process and _process.poll() is None and not _stop_event.is_set():
            time.sleep(0.5)
            
    except Exception as e:
        logger.error(f"Error in user API monitor thread: {e}")

def start_user_api() -> Dict[str, Any]:
    """Start the User API subprocess if it is not already running."""
    global _process, _monitor_thread, _stop_event, _start_time
    
    if _process and _process.poll() is None:
        return {"status": "error", "message": "User API is already running"}
        
    _stop_event.clear()
    
    # Resolve the path to api_main.py (it's in the parent directory of 'app')
    base_dir = Path(__file__).resolve().parent.parent.parent
    api_main_path = base_dir / "api_main.py"
    
    if not api_main_path.exists():
        return {"status": "error", "message": f"Could not find {api_main_path}"}
        
    try:
        # Run the process
        # On Windows, we might need shell=True or appropriate executable
        python_exe = sys.executable
        env = os.environ.copy()
        
        _log_buffer.append("--- Starting User API ---")
        
        _process = subprocess.Popen(
            [python_exe, str(api_main_path)],
            cwd=str(base_dir),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,  # Line buffered
            env=env
        )
        
        _start_time = time.time()
        
        # Start the monitoring thread
        _monitor_thread = threading.Thread(target=_process_monitor, daemon=True)
        _monitor_thread.start()
        
        return {"status": "ok", "message": "User API started successfully", "pid": _process.pid}
        
    except Exception as e:
        logger.error(f"Failed to start User API: {e}")
        return {"status": "error", "message": str(e)}

def stop_user_api() -> Dict[str, Any]:
    """Stop the User API subprocess gracefully, with a fallback to kill."""
    global _process, _stop_event, _start_time
    
    if not _process or _process.poll() is not None:
        return {"status": "ignored", "message": "User API is not running"}
        
    try:
        _stop_event.set()
        _log_buffer.append("--- Stopping User API gracefully ---")
        _process.terminate()
        
        # Wait up to 5 seconds for graceful shutdown
        try:
            _process.wait(timeout=5)
            _log_buffer.append("--- User API stopped ---")
        except subprocess.TimeoutExpired:
            # Fallback to force kill if it hangs
            _log_buffer.append("--- Graceful stop timed out, forcing kill ---")
            _process.kill()
            _process.wait(timeout=2)
            _log_buffer.append("--- User API killed ---")
            
        _process = None
        _start_time = 0.0
        return {"status": "ok", "message": "User API stopped"}
        
    except Exception as e:
        logger.error(f"Error stopping User API: {e}")
        return {"status": "error", "message": str(e)}

def get_user_api_status() -> Dict[str, Any]:
    """Get the current running status of the User API."""
    global _process, _start_time
    
    is_running = _process is not None and _process.poll() is None
    uptime = time.time() - _start_time if is_running else 0
    
    return {
        "is_running": is_running,
        "pid": _process.pid if is_running else None,
        "uptime_s": uptime
    }

def get_user_api_logs(limit: int = 100) -> List[str]:
    """Get the most recent log lines from the User API."""
    # Convert deque to list and return the last 'limit' items
    all_logs = list(_log_buffer)
    if limit > 0:
        return all_logs[-limit:]
    return all_logs

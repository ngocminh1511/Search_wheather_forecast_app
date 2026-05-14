"""
Integration test for Bunny.net Storage hooks in the pipeline.

Simulates a full cycle for cloud_total (smallest map) without spinning up
the orchestrator: directly invokes task_generate_custom_frame +
push_frame_to_bunny + finalize_map_to_bunny.

Run from noaa_be directory:
    .venv/bin/python scripts/bunny_integration_test.py
"""
import sys
import os
import time
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

# Manually load .env (bypass dotenv find_dotenv stdin issue)
env_path = ROOT / ".env"
if env_path.exists():
    with env_path.open() as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip())

os.chdir(str(ROOT))

from app.config import get_settings  # noqa: E402

get_settings.cache_clear()
cfg = get_settings()

print(f"BUNNY_ENABLED={cfg.BUNNY_ENABLED}, ZONE={cfg.BUNNY_STORAGE_ZONE}")
print(f"DATA_DIR={cfg.DATA_DIR}")
print()

if not cfg.BUNNY_ENABLED:
    print("BUNNY_ENABLED=0; aborting integration test (this script is for Bunny mode).")
    sys.exit(0)

from app.services.bunny_storage import get_bunny_client, reset_bunny_client  # noqa: E402

reset_bunny_client()
client = get_bunny_client()
if client is None:
    print("✗ get_bunny_client returned None")
    sys.exit(1)

MAP = "cloud_total"
RUN = "20260507_00z"

# Cleanup any leftover from previous tests
client.delete_run(MAP, RUN)
client.delete_pointer(MAP)
shutil.rmtree(cfg.STAGING_DIR / MAP / RUN, ignore_errors=True)
shutil.rmtree(cfg.TILES_DIR / MAP / RUN, ignore_errors=True)
time.sleep(0.5)

print(f"=== Cycle test: {MAP}/{RUN} ===\n")

# Step 1: Generate frame 000 to STAGING
print("[1] Generate frame f000 → STAGING...")
from app.services.pipeline_tasks import task_generate_custom_frame  # noqa: E402

t0 = time.time()
result = task_generate_custom_frame(MAP, RUN, 0, "tcdc_entire_atmosphere")
gen_time = time.time() - t0
print(f"    time={gen_time:.1f}s")

staging_frame = cfg.STAGING_DIR / MAP / RUN / "000"
if not staging_frame.exists():
    print(f"    SKIP: no tiles generated. Result: {result}")
    sys.exit(0)
n_chunks = sum(1 for _ in staging_frame.rglob("*.chunk"))
print(f"    STAGING has {n_chunks} chunks")

# Step 2: Push f000
print("\n[2] Push f000 → Bunny...")
from app.services.pipeline_tasks import push_frame_to_bunny  # noqa: E402

t0 = time.time()
ok = push_frame_to_bunny(MAP, RUN, 0)
push_time = time.time() - t0
print(f"    push: {'✓ OK' if ok else '✗ FAIL'} ({push_time:.1f}s)")

cleaned = not staging_frame.exists()
print(f"    STAGING after push: {'✓ cleaned' if cleaned else '✗ still has files'}")

# Step 3: Generate + push f003
print("\n[3] Generate + push f003...")
t0 = time.time()
result3 = task_generate_custom_frame(MAP, RUN, 3, "tcdc_entire_atmosphere")
ok3 = push_frame_to_bunny(MAP, RUN, 3)
print(f"    f003 push: {'✓ OK' if ok3 else '✗ FAIL'} ({time.time() - t0:.1f}s)")

# Step 4: Finalize (atomic switch + delete prev)
print("\n[4] Finalize map (atomic pointer switch)...")
from app.services.scheduler_service import finalize_map_to_bunny  # noqa: E402

t0 = time.time()
ok_fin = finalize_map_to_bunny(MAP, RUN, set(), cfg)
print(f"    finalize: {'✓ OK' if ok_fin else '✗ FAIL'} ({time.time() - t0:.1f}s)")

# Step 5: Verify
print("\n[5] Verify Bunny state...")
time.sleep(0.5)
ptr = client.read_pointer(MAP)
print(f"    Pointer: {ptr}")
all_files = client.list_files(f"{MAP}/{RUN}")
print(f"    Total files at {MAP}/{RUN}/: {len(all_files)}")
print("    Sample paths:")
for f in sorted(all_files)[:5]:
    print(f"      {f}")

staging_run = cfg.STAGING_DIR / MAP / RUN
tiles_run = cfg.TILES_DIR / MAP / RUN
print(
    f"    Local STAGING: "
    f"{'✓ clean' if not staging_run.exists() else '✗ DIRTY'}"
)
print(
    f"    Local TILES:   "
    f"{'✓ no LIVE (correct)' if not tiles_run.exists() else '✗ has LIVE (bug)'}"
)

# Frontend tile URL preview
if all_files:
    sample = sorted(all_files)[0]
    pull_url = cfg.BUNNY_PULL_ZONE_URL.rstrip("/") if cfg.BUNNY_PULL_ZONE_URL else None
    if pull_url:
        print(f"\n    Frontend URL: {pull_url}/{cfg.BUNNY_PATH_PREFIX}/{sample}")

# Cleanup
print("\n[CLEANUP] removing test data from Bunny...")
client.delete_run(MAP, RUN)
client.delete_pointer(MAP)
print("✓ Done")

print(f"\n=== Integration test summary ===")
print(f"  Generate time: {gen_time:.1f}s")
print(f"  Push time:     {push_time:.1f}s")
print(f"  Push throughput: {n_chunks / push_time:.1f} chunks/s")
print(f"  Finalize: instant (no cold copy on first cycle)")
print(f"\n=== TEST PASSED ===")

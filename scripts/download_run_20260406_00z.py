from datetime import date
from pathlib import Path
import json
import os

from noaa_map_pipeline import download_all_maps_for_run, analyze_data_folder, MAP_SPECS


# Local defaults for easy tuning.
DEFAULT_RUN_DATE = "2026-04-06"  # format: YYYY-MM-DD
DEFAULT_RUN_HOUR = 0  # 0, 6, 12, 18 are standard GFS cycles
DEFAULT_MODE = "full"  # full = full forecast range from f000 onward
DEFAULT_RPM_LIMIT = 100


def resolve_runtime_config() -> tuple[date, int, str, int]:
    run_date_str = os.getenv("NOAA_RUN_DATE", DEFAULT_RUN_DATE)
    run_hour = int(os.getenv("NOAA_RUN_HOUR", str(DEFAULT_RUN_HOUR)))
    mode = os.getenv("NOAA_MODE", DEFAULT_MODE)
    rpm_limit = int(os.getenv("NOAA_RPM_LIMIT", str(DEFAULT_RPM_LIMIT)))

    run_date = date.fromisoformat(run_date_str)
    if run_hour < 0 or run_hour > 23:
        raise ValueError("NOAA_RUN_HOUR must be between 0 and 23")
    if mode not in ("f00_only", "init_only", "full"):
        raise ValueError("NOAA_MODE must be 'f00_only', 'init_only', or 'full'")
    if rpm_limit <= 0:
        raise ValueError("NOAA_RPM_LIMIT must be > 0")

    return run_date, run_hour, mode, rpm_limit


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    run_date, run_hour, mode, rpm_limit = resolve_runtime_config()

    print(
        f"Run config => date={run_date.isoformat()}, hour={run_hour:02d}, "
        f"mode={mode}, rpm_limit={rpm_limit}"
    )

    results = download_all_maps_for_run(
        base_dir=str(root),
        run_date=run_date,
        run_hour=run_hour,
        mode=mode,
        rpm_limit=rpm_limit,
    )

    compact = {}
    for map_type in MAP_SPECS:
        summary = analyze_data_folder(str(root), map_type)
        downloaded = [d for d in results[map_type]["downloads"] if d["status"] == "downloaded"]
        compact[map_type] = {
            "requested": len(results[map_type]["downloads"]),
            "downloaded": len(downloaded),
            "files_present": summary["file_count"],
            "total_size_mb": summary["total_size_mb"],
        }

    out_file = root / "data" / f"run_{run_date.strftime('%Y%m%d')}_{run_hour:02d}z_summary.json"
    out_file.write_text(json.dumps(compact, indent=2), encoding="utf-8")

    print(json.dumps(compact, indent=2))
    print(f"Summary written to: {out_file}")

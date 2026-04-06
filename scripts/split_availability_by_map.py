from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
from typing import Dict


DEFAULT_RUN_DATE = "2026-04-06"
DEFAULT_RUN_HOUR = 0


def resolve_runtime_config() -> tuple[date, int]:
    run_date_str = os.getenv("NOAA_RUN_DATE", DEFAULT_RUN_DATE)
    run_hour = int(os.getenv("NOAA_RUN_HOUR", str(DEFAULT_RUN_HOUR)))

    run_date = date.fromisoformat(run_date_str)
    if run_hour < 0 or run_hour > 23:
        raise ValueError("NOAA_RUN_HOUR must be between 0 and 23")

    return run_date, run_hour


def load_availability_file(data_dir: Path, run_date: date, run_hour: int) -> Dict:
    in_file = data_dir / f"availability_{run_date.strftime('%Y%m%d')}_{run_hour:02d}z.json"
    if not in_file.exists():
        raise FileNotFoundError(f"Input availability file not found: {in_file}")

    return json.loads(in_file.read_text(encoding="utf-8"))


def split_availability_by_map(root: Path, payload: Dict, run_date: date, run_hour: int) -> None:
    output_root = root / "available"
    output_root.mkdir(parents=True, exist_ok=True)

    map_types = payload.get("map_types", {})

    for map_type, map_info in map_types.items():
        map_dir = output_root / map_type
        map_dir.mkdir(parents=True, exist_ok=True)

        out_payload = {
            "run_date": payload.get("run_date", run_date.isoformat()),
            "run_hour": payload.get("run_hour", run_hour),
            "max_fff": payload.get("max_fff"),
            "fff_existing": payload.get("fff_existing", []),
            "fff_existing_segments": payload.get("fff_existing_segments", []),
            "map_type": map_type,
            "map_info": map_info,
        }

        out_file = map_dir / f"availability_{run_date.strftime('%Y%m%d')}_{run_hour:02d}z_{map_type}.json"
        out_file.write_text(json.dumps(out_payload, indent=2), encoding="utf-8")
        print(f"Wrote: {out_file}")


def main() -> None:
    run_date, run_hour = resolve_runtime_config()
    root = Path(__file__).resolve().parents[1]
    data_dir = root / "available"

    payload = load_availability_file(data_dir=data_dir, run_date=run_date, run_hour=run_hour)
    split_availability_by_map(root=root, payload=payload, run_date=run_date, run_hour=run_hour)


if __name__ == "__main__":
    main()

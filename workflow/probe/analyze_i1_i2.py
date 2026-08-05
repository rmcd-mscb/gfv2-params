"""Read the I1/I2 probe records and answer both questions numerically.

I1 -- submission count: how many distinct SLURM job ids did N rule instances become?
I2 -- import storm: were instances started together (storm risk) or serialised
      (grouping bought nothing), and did geo-import wall-time degrade?

Usage:
    pixi run --as-is -e default python workflow/probe/analyze_i1_i2.py <arm_label>
"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

from gfv2_params.config import load_base_config


def main() -> int:
    label = sys.argv[1] if len(sys.argv) > 1 else "unlabelled"
    base = load_base_config(None, fabric="tjc")
    probe_dir = Path(base["data_root"]) / "tjc" / "_i1i2_probe"

    records = []
    for p in sorted(probe_dir.glob("import_*.json")):
        records.append(json.loads(p.read_text()))
    if not records:
        print(f"[{label}] NO RECORDS in {probe_dir} -- the arm did not complete.")
        return 1

    job_ids = {r["slurm_job_id"] for r in records}
    hosts = {r["host"] for r in records}
    starts = sorted(r["t_start_epoch"] for r in records)
    geo = [r["import_geo_s"] for r in records]
    total = [r["import_total_s"] for r in records]

    # Concurrency proxy: how many instances began within 2s of the earliest.
    # A group that runs its members together shows a tight cluster here; one that
    # serialises them shows a spread.
    burst = sum(1 for s in starts if s - starts[0] <= 2.0)
    span = starts[-1] - starts[0]

    print(f"=== I1/I2 arm: {label} ===")
    print(f"  instances completed   : {len(records)}")
    print(f"  DISTINCT SLURM job ids: {len(job_ids)}   <-- I1: submissions")
    print(f"  distinct hosts        : {len(hosts)}")
    print(f"  start span            : {span:.1f}s")
    print(f"  started within 2s     : {burst}/{len(records)}   <-- I2: concurrency")
    print(f"  geo import   median   : {statistics.median(geo):.3f}s  "
          f"max {max(geo):.3f}s")
    print(f"  total import median   : {statistics.median(total):.3f}s  "
          f"max {max(total):.3f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

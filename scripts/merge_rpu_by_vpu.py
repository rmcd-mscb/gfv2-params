"""Merge Regional Processing Unit (RPU) rasters by VPU and dataset type.

Thin CLI shell over the ``gfv2_params.shared_rasters.merge_rpu_by_vpu``
library entrypoint. Preserves the original argparse interface so existing
sbatch jobs (slurm_batch/merge_rpu_by_vpu*.batch) keep working unchanged.

For unified DAG-style invocation across all shared rasters, prefer:
    python scripts/build_shared_rasters.py --config configs/shared_rasters.yml
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging
from gfv2_params.shared_rasters import SharedRastersContext
from gfv2_params.shared_rasters.merge_rpu_by_vpu import build


def main():
    parser = argparse.ArgumentParser(description="Merge NHD rasters by VPU and dataset type.")
    parser.add_argument("--config", required=True, help="Path to merge_rpu_by_vpu.yml (or merge_rpu_by_vpu_twi.yml)")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output files")
    args = parser.parse_args()

    logger = configure_logging("merge_rpu_by_vpu")

    base = load_base_config(fabric=args.fabric)
    data_root = Path(base["data_root"])
    ctx = SharedRastersContext(
        data_root=data_root,
        vpus=[args.vpu],
        output_dir=data_root / "work",
        force=args.force,
    )
    build({"manifest": args.config}, ctx, logger)


if __name__ == "__main__":
    main()

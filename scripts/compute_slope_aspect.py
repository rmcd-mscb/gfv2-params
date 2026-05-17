"""Compute slope and aspect rasters from a DEM using richdem.

Thin CLI shell over the ``gfv2_params.shared_rasters.compute_slope_aspect``
library entrypoint. Preserves the original argparse interface so existing
sbatch jobs (slurm_batch/compute_slope_aspect.batch) keep working unchanged.

For unified DAG-style invocation across all shared rasters, prefer:
    python scripts/build_shared_rasters.py --config configs/shared_rasters.yml
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.shared_rasters import SharedRastersContext
from gfv2_params.shared_rasters.compute_slope_aspect import build


def main():
    parser = argparse.ArgumentParser(description="Compute slope and aspect rasters from DEM.")
    parser.add_argument("--config", required=True, help="Path to slope_aspect.yml")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing output files")
    args = parser.parse_args()

    logger = configure_logging("compute_slope_aspect")

    config = load_config(Path(args.config), vpu=args.vpu, fabric=args.fabric)
    data_root = Path(config["data_root"])
    ctx = SharedRastersContext(
        data_root=data_root,
        vpus=[args.vpu],
        output_dir=data_root / "work",
        force=args.force,
    )
    step_cfg = {
        "input_dir": config["input_dir"],
        "output_dir": config["output_dir"],
    }
    build(step_cfg, ctx, logger)


if __name__ == "__main__":
    main()

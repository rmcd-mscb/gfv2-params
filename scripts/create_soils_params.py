"""Create soils and soil_moist_max parameters from raster data.

Thin CLI shell over ``gfv2_params.zonal_runners.run_soils_batch``. For unified
DAG-style dispatch across every Part 2 param type, prefer:

    sbatch slurm_batch/submit_zonal_params.sh <batches_dir> <fabric>
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.zonal_runners import run_soils_batch


def main():
    parser = argparse.ArgumentParser(description="Create soils parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID")
    args = parser.parse_args()

    logger = configure_logging("create_soils_params")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    run_soils_batch(config, args.batch_id, logger)


if __name__ == "__main__":
    main()

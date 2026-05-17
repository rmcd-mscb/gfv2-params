"""Pre-compute CONUS-wide polygon-to-polygon weights for ssflux.

Runs WeightGenP2P between the full merged fabric and the lithology
shapefile. Writes a single weight table that batch jobs can subset.

Thin CLI shell over ``gfv2_params.zonal_runners.run_build_weights``. For
unified DAG-style dispatch (with the ssflux array + merge chained on
afterok), prefer:

    sbatch slurm_batch/submit_zonal_params.sh <batches_dir> <fabric>
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config, load_config
from gfv2_params.log import configure_logging
from gfv2_params.zonal_runners import run_build_weights


def main():
    parser = argparse.ArgumentParser(description="Pre-compute P2P weights for ssflux.")
    parser.add_argument("--config", required=True, help="Path to ssflux config YAML")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing weight file")
    args = parser.parse_args()

    logger = configure_logging("build_weights")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    base = load_base_config(
        Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    run_build_weights(config, Path(base["data_root"]), logger, force=args.force)


if __name__ == "__main__":
    main()

"""Merge per-batch parameter CSVs into a single file, sorted by id_feature.

Validates completeness: raises on duplicate IDs, warns on gaps.

Thin CLI shell over ``gfv2_params.zonal_runners.run_merge``. For unified
DAG-style dispatch (with each per-param merge chained afterok the matching
array), prefer:

    sbatch slurm_batch/submit_zonal_params.sh <batches_dir> <fabric>
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.zonal_runners import run_merge


def main():
    parser = argparse.ArgumentParser(description="Merge per-batch parameter CSVs.")
    parser.add_argument("--config", required=True, help="Path to YAML config file")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    args = parser.parse_args()

    logger = configure_logging("merge_params")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    run_merge(config, logger)


if __name__ == "__main__":
    main()

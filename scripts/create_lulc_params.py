"""Create LULC parameters from raster data via crosswalk.

Computes per-HRU: cov_type, srain_intcp, wrain_intcp, snow_intcp,
covden_sum, covden_win, retention from a categorical LULC raster and a
continuous canopy density raster, using a crosswalk CSV to map LULC classes
to NHM parameters.

When a ``keep_raster`` is configured (FORE-SCE, NHM v1.1), a raster-derived
retention mean is computed via zonal stats and included in the output.
When no ``keep_raster`` is present (NLCD, NALCMS), per-HRU retention is
synthesised from the crosswalk's ``evergreen_retention`` column as a
weighted average across LULC classes.

Thin CLI shell over ``gfv2_params.zonal_runners.run_lulc_batch``. For
unified DAG-style dispatch across every Part 2 param type, prefer:

    sbatch slurm_batch/submit_zonal_params.sh <batches_dir> <fabric>
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.zonal_runners import run_lulc_batch


def main():
    parser = argparse.ArgumentParser(description="Create LULC parameters from raster data.")
    parser.add_argument("--config", required=True, help="Path to LULC step config YAML")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--fabric", default=None, help="Fabric name (overrides FABRIC env / default_fabric)")
    parser.add_argument("--batch_id", type=int, required=True, help="Batch ID")
    args = parser.parse_args()

    logger = configure_logging("create_lulc_params")
    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
        fabric=args.fabric,
    )
    run_lulc_batch(config, args.batch_id, logger)


if __name__ == "__main__":
    main()

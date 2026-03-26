"""Pre-compute derived rasters (soil_moist_max) from source inputs.

Eliminates race conditions when multiple SLURM batch jobs would
otherwise try to create the same derived rasters simultaneously.
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import mult_rasters, resample


def main():
    parser = argparse.ArgumentParser(description="Build derived rasters from source inputs.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    logger = configure_logging("build_derived_rasters")

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = Path(base["data_root"])

    # Source inputs
    rd_rast = data_root / "input" / "lulc_veg" / "RootDepth.tif"
    awc_rast = data_root / "input" / "soils_litho" / "AWC.tif"

    # Derived outputs
    derived_dir = data_root / "work" / "derived_rasters"
    derived_dir.mkdir(parents=True, exist_ok=True)
    intermediate_rast = derived_dir / "rd_250_intermediate.tif"
    rd_resampled = derived_dir / "rd_250_raw.tif"
    soil_moist_max_rast = derived_dir / "soil_moist_max.tif"

    if not rd_rast.exists():
        raise FileNotFoundError(f"RootDepth raster not found: {rd_rast}")
    if not awc_rast.exists():
        raise FileNotFoundError(f"AWC raster not found: {awc_rast}")

    # Step 1: Resample RootDepth to match AWC grid
    if not rd_resampled.exists() or args.force:
        logger.info("Resampling RootDepth to AWC grid...")
        resample(str(rd_rast), str(awc_rast), str(intermediate_rast), str(rd_resampled))
        logger.info("Written: %s", rd_resampled)
    else:
        logger.info("Resampled RootDepth already exists: %s", rd_resampled)

    # Step 2: Multiply resampled RootDepth x AWC -> soil_moist_max
    if not soil_moist_max_rast.exists() or args.force:
        logger.info("Computing soil_moist_max = RootDepth * AWC...")
        mult_rasters(str(rd_resampled), str(awc_rast), str(soil_moist_max_rast))
        logger.info("Written: %s", soil_moist_max_rast)
    else:
        logger.info("soil_moist_max raster already exists: %s", soil_moist_max_rast)

    logger.info("Derived rasters complete")


if __name__ == "__main__":
    main()

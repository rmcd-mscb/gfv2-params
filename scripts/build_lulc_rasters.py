"""Pre-compute LULC derived rasters (radiation transmission).

Resamples canopy and keep rasters to match the LULC grid, then
computes the radiation transmission coefficient raster:
    radtrn = (cnpy * keep / 100) where lulc >= tree_threshold, else 0.
"""

import argparse
from pathlib import Path

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging
from gfv2_params.raster_ops import compute_radtrn, resample


def main():
    parser = argparse.ArgumentParser(description="Build LULC derived rasters.")
    parser.add_argument("--config", required=True, help="Path to LULC step config YAML")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    args = parser.parse_args()

    logger = configure_logging("build_lulc_rasters")

    config = load_config(
        Path(args.config),
        base_config_path=Path(args.base_config) if args.base_config else None,
    )

    lulc_raster = Path(config["source_raster"])
    cnpy_raster = Path(config["canopy_raster"])
    radtrn_raster = Path(config["radtrn_raster"])

    # keep_raster is optional (NLCD/NALCMS may not have one)
    keep_raster_str = config.get("keep_raster")
    keep_raster = Path(keep_raster_str) if keep_raster_str else None

    if not lulc_raster.exists():
        raise FileNotFoundError(f"LULC raster not found: {lulc_raster}")
    if not cnpy_raster.exists():
        raise FileNotFoundError(f"Canopy raster not found: {cnpy_raster}")

    derived_dir = radtrn_raster.parent
    derived_dir.mkdir(parents=True, exist_ok=True)

    # Step 1: Resample CNPY to LULC grid
    cnpy_resampled = derived_dir / f"cnpy_resampled_{config.get('lulc_source', 'lulc')}.tif"
    if not cnpy_resampled.exists() or args.force:
        logger.info("Resampling canopy raster to LULC grid...")
        intermediate = derived_dir / "cnpy_resample_intermediate.tif"
        resample(str(cnpy_raster), str(lulc_raster), str(intermediate), str(cnpy_resampled))
        logger.info("Written: %s", cnpy_resampled)
    else:
        logger.info("Resampled canopy raster already exists: %s", cnpy_resampled)

    # Step 2: Resample keep to LULC grid and compute radtrn (only if keep raster exists)
    if keep_raster is not None:
        if not keep_raster.exists():
            raise FileNotFoundError(f"Keep raster not found: {keep_raster}")

        keep_resampled = derived_dir / f"keep_resampled_{config.get('lulc_source', 'lulc')}.tif"
        if not keep_resampled.exists() or args.force:
            logger.info("Resampling keep raster to LULC grid...")
            intermediate = derived_dir / "keep_resample_intermediate.tif"
            resample(str(keep_raster), str(lulc_raster), str(intermediate), str(keep_resampled))
            logger.info("Written: %s", keep_resampled)
        else:
            logger.info("Resampled keep raster already exists: %s", keep_resampled)

        # Step 3: Compute radiation transmission
        if not radtrn_raster.exists() or args.force:
            logger.info("Computing radiation transmission raster...")
            compute_radtrn(
                str(lulc_raster),
                str(cnpy_resampled),
                str(keep_resampled),
                str(radtrn_raster),
            )
            logger.info("Written: %s", radtrn_raster)
        else:
            logger.info("Radiation transmission raster already exists: %s", radtrn_raster)
    else:
        logger.info("No keep raster configured; skipping radtrn computation")

    logger.info("LULC derived rasters complete")


if __name__ == "__main__":
    main()

"""Compute slope and aspect rasters from a DEM using richdem."""

import argparse
from pathlib import Path

import richdem as rd
import rioxarray

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging

# The per-VPU merged DEM tiles declare nodata=-9999 in their metadata, but the
# actual nodata pixel value is -99.99.  This mismatch arises because the source
# RPU data is in centimetres (nodata=-9999 cm) and merge_rpu_by_vpu.py divides
# by 100 to convert to metres without updating the nodata declaration
# (-9999 cm / 100 = -99.99 m).  Consequently every "nodata" pixel in the output
# tile holds the value -99.99, not -9999.
#
# Everything downstream must use -99.99 as the effective nodata value:
#   • build_vrt.py uses srcNodata="-99.99" so GDAL treats those pixels as
#     transparent when building the CONUS VRT.
#   • compute_slope_aspect (this script) must pass no_data=-99.99 to RichDEM so
#     it ignores the VPU rectangular fill region rather than treating it as
#     valid flat terrain (which produces spurious slope=0 / aspect=0 output).
DEM_NODATA = -99.99


def main():
    parser = argparse.ArgumentParser(description="Compute slope and aspect rasters from DEM.")
    parser.add_argument("--config", required=True, help="Path to config YAML file")
    parser.add_argument("--vpu", required=True, help="VPU code, e.g., 01")
    parser.add_argument("--force", action="store_true",
                        help="Overwrite existing output files")
    args = parser.parse_args()

    logger = configure_logging("compute_slope_aspect")

    config = load_config(Path(args.config), vpu=args.vpu)
    input_dir = Path(config["input_dir"])
    output_dir = Path(config["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    dem_path = input_dir / args.vpu / f"NEDSnapshot_merged_{args.vpu}.tif"
    dem_fixed_path = input_dir / args.vpu / f"NEDSnapshot_merged_fixed_{args.vpu}.tif"
    slope_out = output_dir / args.vpu / f"NEDSnapshot_merged_slope_{args.vpu}.tif"
    aspect_out = output_dir / args.vpu / f"NEDSnapshot_merged_aspect_{args.vpu}.tif"

    if not dem_path.exists():
        raise FileNotFoundError(f"DEM not found: {dem_path}")

    if not args.force and slope_out.exists() and aspect_out.exists():
        logger.info("Outputs already exist, skipping (use --force to overwrite): %s", slope_out)
        return

    # The _fixed_ tile fills declared-nodata (-9999) with -9999 so that the
    # rioxarray masked array round-trip is lossless.  The -99.99 fill pixels are
    # intentionally left as-is: the elevation VRT reads these tiles and relies on
    # srcNodata="-99.99" (in build_vrt.py) to treat them as transparent.
    logger.info("Creating fixed nodata NEDSnapshot")
    da = rioxarray.open_rasterio(dem_path, masked=True).squeeze()
    da_fixed = da.fillna(-9999)
    da_fixed.rio.write_nodata(-9999, inplace=True)
    da_fixed.rio.to_raster(dem_fixed_path)

    # Load the raw DEM for terrain analysis.  Pass DEM_NODATA=-99.99 so RichDEM
    # recognises the actual fill pixels as nodata and does not compute slope/aspect
    # for the VPU rectangular padding region.
    logger.info("Loading DEM: %s", dem_path)
    dem = rd.LoadGDAL(str(dem_path), no_data=DEM_NODATA)

    logger.info("Computing slope (degrees)...")
    slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
    slope_out.parent.mkdir(parents=True, exist_ok=True)
    rd.SaveGDAL(str(slope_out), slope)
    logger.info("Slope raster saved to: %s", slope_out)

    logger.info("Computing aspect...")
    aspect = rd.TerrainAttribute(dem, attrib="aspect")
    rd.SaveGDAL(str(aspect_out), aspect)
    logger.info("Aspect raster saved to: %s", aspect_out)


if __name__ == "__main__":
    main()

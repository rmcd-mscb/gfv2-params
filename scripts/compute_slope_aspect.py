"""Compute slope and aspect rasters from a DEM using richdem."""

import argparse
from pathlib import Path

import richdem as rd
import rioxarray

from gfv2_params.config import load_config
from gfv2_params.log import configure_logging

# The per-VPU merged DEM tiles (written by merge_rpu_by_vpu.py) declare and use
# nodata=-99.99: the source RPU data is in centimetres (nodata=-9999 cm), divided
# by 100 to convert to metres (-99.99 m), and the nodata declaration is updated
# to match (-99.99) by merge_rpu_by_vpu.py.
#
# Downstream nodata conventions:
#   • rd.LoadGDAL must use no_data=-99.99 so RichDEM masks the VPU rectangular
#     fill region rather than treating it as valid flat terrain, which would
#     produce spurious slope=0 / aspect=0 output.
#   • The _fixed_ tile is written with fillna(-9999) / write_nodata(-9999) so
#     build_vrt.py can use srcNodata="-9999" for the elevation VRT — the same
#     value RichDEM SaveGDAL writes for slope/aspect tiles.
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

    # Always regenerate the _fixed_ tile — it is a fast rioxarray copy and its
    # nodata convention must stay in sync with build_vrt.py's srcNodata value.
    # The _fixed_ tile is a re-encoded GeoTIFF of the merged DEM with nodata=-9999.
    # merge_rpu_by_vpu.py now correctly declares nodata=-99.99, so open_rasterio
    # with masked=True will represent those pixels as NaN; fillna(-9999) converts
    # them to -9999 and write_nodata(-9999) declares that in the output.  This
    # aligns with build_vrt.py's srcNodata="-9999" used for the elevation VRT.
    logger.info("Creating fixed nodata NEDSnapshot")
    da = rioxarray.open_rasterio(dem_path, masked=True).squeeze()
    da_fixed = da.fillna(-9999)
    da_fixed.rio.write_nodata(-9999, inplace=True)
    da_fixed.rio.to_raster(dem_fixed_path)

    if not args.force and slope_out.exists() and aspect_out.exists():
        logger.info("Slope/aspect outputs already exist, skipping (use --force to overwrite): %s", slope_out)
        return

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

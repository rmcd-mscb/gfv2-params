"""Build CONUS-wide VRT files from per-VPU merged GeoTIFFs.

Creates GDAL virtual rasters that reference per-VPU source files,
allowing them to be read as a single CONUS-wide raster without
duplicating data on disk.
"""

import argparse
from pathlib import Path

from osgeo import gdal

from gfv2_params.config import load_base_config
from gfv2_params.log import configure_logging

RASTER_TYPES = {
    "elevation": "NEDSnapshot_merged_fixed_*.tif",
    "slope": "NEDSnapshot_merged_slope_*.tif",
    "aspect": "NEDSnapshot_merged_aspect_*.tif",
}

# All three VRT types use srcNodata="-9999" because:
#   elevation: _fixed_ tiles are written by compute_slope_aspect.py with fillna(-9999)
#              and write_nodata(-9999), so -9999 is the fill value GDAL must treat as
#              transparent when compositing VPU tiles in the VRT.
#   slope/aspect: RichDEM SaveGDAL always writes -9999 as its nodata value.
VRT_SRCNODATA = "-9999"


def main():
    parser = argparse.ArgumentParser(description="Build CONUS-wide VRTs from per-VPU rasters.")
    parser.add_argument("--base_config", default=None, help="Path to base_config.yml")
    args = parser.parse_args()

    logger = configure_logging("build_vrt")

    base = load_base_config(Path(args.base_config) if args.base_config else None)
    data_root = Path(base["data_root"])
    nhd_merged_dir = data_root / "work" / "nhd_merged"

    if not nhd_merged_dir.exists():
        raise FileNotFoundError(f"NHD merged directory not found: {nhd_merged_dir}")

    built_count = 0
    for vrt_name, pattern in RASTER_TYPES.items():
        source_files = sorted(nhd_merged_dir.glob(f"*/{pattern}"))
        if not source_files:
            logger.warning("No source files found for %s (pattern: */%s)", vrt_name, pattern)
            continue

        vrt_path = nhd_merged_dir / f"{vrt_name}.vrt"
        logger.info("Building %s from %d source files", vrt_path, len(source_files))

        # srcNodata tells GDAL to treat that pixel value as transparent when
        # compositing overlapping VPU source tiles.  All three types use -9999:
        # elevation _fixed_ tiles are written with nodata=-9999 by compute_slope_aspect;
        # slope/aspect tiles use -9999 because RichDEM SaveGDAL always writes that value.
        vrt_options = gdal.BuildVRTOptions(resolution="highest", srcNodata=VRT_SRCNODATA)
        vrt_ds = gdal.BuildVRT(str(vrt_path), [str(f) for f in source_files], options=vrt_options)
        if vrt_ds is None:
            raise RuntimeError(f"gdal.BuildVRT failed for {vrt_name}")
        vrt_ds.FlushCache()
        del vrt_ds

        built_count += 1
        logger.info("Written: %s (%d sources)", vrt_path, len(source_files))

    if built_count == 0:
        raise RuntimeError(
            f"No VRTs were built. Check that {nhd_merged_dir} contains "
            "per-VPU subdirectories with merged GeoTIFFs."
        )
    logger.info("VRT build complete: %d of %d types built", built_count, len(RASTER_TYPES))


if __name__ == "__main__":
    main()

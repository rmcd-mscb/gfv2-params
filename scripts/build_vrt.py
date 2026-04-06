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

# Per-type fill values used as srcNodata in the VRT.
# elevation: _fixed_ tiles have -99.99 fill (raw nodata -9999 divided by 100 during merge)
# slope/aspect: RichDEM SaveGDAL writes -9999 fill (RichDEM's own nodata convention)
RASTER_NODATA = {
    "elevation": "-99.99",
    "slope": "-9999",
    "aspect": "-9999",
}


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

        # srcNodata makes GDAL treat the VPU-tile fill value as transparent when
        # compositing overlapping sources. Elevation _fixed_ tiles use -99.99 fill
        # (raw -9999 cm divided by 100 during merge, nodata declaration not updated).
        # Slope/aspect tiles are written by RichDEM SaveGDAL which uses -9999 as its
        # own nodata convention regardless of what was passed to LoadGDAL.
        vrt_options = gdal.BuildVRTOptions(resolution="highest", srcNodata=RASTER_NODATA[vrt_name])
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

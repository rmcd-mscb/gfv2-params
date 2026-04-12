"""Build VRT files from per-VPU merged GeoTIFFs and optional fill layers.

Creates GDAL virtual rasters that reference per-VPU source files,
allowing them to be read as a single raster without duplicating data
on disk.  If a ``copernicus_fill`` subdirectory exists under
``nhd_merged/``, its tiles are listed as lower-priority fill sources
before the primary NHDPlus VPU tiles.  GDAL VRT compositing is
last-source-wins, so NHDPlus takes priority and fill sources only
contribute where NHDPlus has nodata.
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

    # Fill subdirectories whose tiles should be listed BEFORE the primary
    # NHDPlus VPU tiles.  GDAL VRT uses last-source-wins for overlapping
    # pixels, so listing NHDPlus last ensures it takes priority and fill
    # sources only contribute where NHDPlus has nodata.
    FILL_DIRS = {"copernicus_fill"}

    built_count = 0
    for vrt_name, pattern in RASTER_TYPES.items():
        # Primary NHDPlus VPU tiles (listed last = highest priority)
        primary_files = sorted(
            f for f in nhd_merged_dir.glob(f"*/{pattern}")
            if f.parent.name not in FILL_DIRS
        )
        # Fill tiles (listed first = lowest priority)
        fill_files = []
        for fill_dir_name in sorted(FILL_DIRS):
            fill_files.extend(sorted(nhd_merged_dir.glob(f"{fill_dir_name}/{pattern}")))

        source_files = fill_files + primary_files
        if not source_files:
            logger.warning("No source files found for %s (pattern: */%s)", vrt_name, pattern)
            continue

        vrt_path = nhd_merged_dir / f"{vrt_name}.vrt"
        n_fill = len(fill_files)
        fill_msg = f" + {n_fill} fill" if n_fill else ""
        logger.info("Building %s from %d source files (%d primary%s)",
                     vrt_path, len(source_files), len(primary_files), fill_msg)

        # srcNodata: see VRT_SRCNODATA constant above for rationale.
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

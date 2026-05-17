"""CONUS VRT assembly from per-VPU merged GeoTIFFs and optional fill layers.

Library entrypoint for the shared-raster orchestrator. The thin CLI shell at
scripts/build_vrt.py delegates here so existing invocations keep working
unchanged.

Creates GDAL virtual rasters that reference per-VPU source files, allowing
them to be read as a single CONUS raster without duplicating data on disk.
If a ``copernicus_fill`` subdirectory exists under ``nhd_merged/``, its
tiles are listed as lower-priority fill sources before the primary NHDPlus
VPU tiles. GDAL VRT compositing is last-source-wins, so NHDPlus takes
priority and fill sources only contribute where NHDPlus has nodata.

CONUS-once: this builder does not iterate ctx.vpus. The per-VPU sources are
discovered by globbing the nhd_merged directory.
"""

from __future__ import annotations

from pathlib import Path

from osgeo import gdal

from .context import SharedRastersContext

# Maps VRT name -> (glob pattern, srcNodata for BuildVRT).
# srcNodata rationale:
#   elevation: _fixed_ tiles are written by compute_slope_aspect with
#              fillna(-9999) and write_nodata(-9999).
#   slope/aspect: RichDEM SaveGDAL always writes -9999 (NED-based, raw DEM).
#   fdr: NHDPlus FDR tiles are Byte rasters with nodata=255 (D8 codes are 1-128).
#   twi: merge_rpu_by_vpu's TWI case writes float32 and remaps the source
#        -FLT_MAX sentinel to -9999. **DO NOT SWAP** this to the open-source
#        Twi_hydrodem_*.tif produced by compute_dem_derivatives: PRMS
#        parameter extraction (carea_max, smidx_coef) thresholds TWI at
#        calibrated values (8.0, 15.6) that depend on the original ArcPy TWI
#        distribution shape. Swapping the source would invalidate those
#        thresholds. See PR #54 discussion.
RASTER_TYPES = {
    "elevation": ("NEDSnapshot_merged_fixed_*.tif", "-9999"),
    "slope":     ("NEDSnapshot_merged_slope_*.tif", "-9999"),
    "aspect":    ("NEDSnapshot_merged_aspect_*.tif", "-9999"),
    "fdr":       ("Fdr_merged_*.tif", "255"),
    "twi":       ("Twi_merged_*.tif", "-9999"),
}

# Fill subdirectories whose tiles should be listed BEFORE the primary NHDPlus
# VPU tiles. GDAL VRT uses last-source-wins for overlapping pixels, so listing
# NHDPlus last ensures it takes priority and fill sources only contribute
# where NHDPlus has nodata.
FILL_DIRS = {"copernicus_fill"}


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Build CONUS-wide VRTs for elevation, slope, aspect, fdr, twi.

    step_cfg keys (optional):
      nhd_merged_dir — directory of per-VPU rasters. Defaults to
                       ``{data_root}/work/nhd_merged``.

    Returns a dict mapping VRT short name (``elevation``, ``slope``, ...) to
    the built VRT path. Recorded in ctx.paths for any downstream consumers.
    """
    nhd_merged_dir = Path(step_cfg.get(
        "nhd_merged_dir", ctx.data_root / "work" / "nhd_merged",
    ))
    if not nhd_merged_dir.exists():
        raise FileNotFoundError(f"NHD merged directory not found: {nhd_merged_dir}")

    produced: dict = {}
    built_count = 0
    for vrt_name, (pattern, src_nodata) in RASTER_TYPES.items():
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

        # srcNodata: see RASTER_TYPES table above for per-type rationale.
        vrt_options = gdal.BuildVRTOptions(resolution="highest", srcNodata=src_nodata)
        vrt_ds = gdal.BuildVRT(str(vrt_path), [str(f) for f in source_files], options=vrt_options)
        if vrt_ds is None:
            raise RuntimeError(f"gdal.BuildVRT failed for {vrt_name}")
        vrt_ds.FlushCache()
        del vrt_ds

        built_count += 1
        produced[f"{vrt_name}_vrt"] = vrt_path
        logger.info("Written: %s (%d sources)", vrt_path, len(source_files))

    if built_count == 0:
        raise RuntimeError(
            f"No VRTs were built. Check that {nhd_merged_dir} contains "
            "per-VPU subdirectories with merged GeoTIFFs."
        )
    logger.info("VRT build complete: %d of %d types built", built_count, len(RASTER_TYPES))
    return produced

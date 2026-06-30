"""CONUS VRT assembly from per-VPU merged GeoTIFFs and optional fill layers.

Library entrypoint for the ``build_vrt`` step in the shared-raster
orchestrator (``scripts/build_shared_rasters.py``). Registered via the
BUILDERS dict in ``shared_rasters/__init__.py`` and called by the
orchestrator's STEP_ORDER walk.

Creates GDAL virtual rasters that reference per-VPU source files, allowing
them to be read as a single CONUS raster without duplicating data on disk.
Reads per-VPU sources from ``ctx.per_vpu_dir`` (one subdir per VPU). If a
``borders_dir`` (Copernicus fill) exists alongside, its tiles are listed as
lower-priority fill sources before the primary NHDPlus VPU tiles. GDAL VRT
compositing is last-source-wins, so NHDPlus takes priority and fill sources
only contribute where NHDPlus has nodata. VRTs are written to ``ctx.vrt_dir``.

CONUS-once: this builder does not iterate ctx.vpus. The per-VPU sources are
discovered by globbing the per_vpu directory.
"""

from __future__ import annotations

from pathlib import Path

from osgeo import gdal, osr

from .context import SharedRastersContext

# Maps VRT name -> (glob pattern, srcNodata for BuildVRT).
# srcNodata rationale:
#   elevation: _fixed_ tiles are written by compute_slope_aspect with
#              fillna(-9999) and write_nodata(-9999).
#   slope/aspect: RichDEM SaveGDAL always writes -9999 (NED-based, raw DEM).
#   fdr: NHDPlus FDR tiles are Byte rasters with nodata=255 (D8 codes are 1-128).
#   twi: merge_rpu_by_vpu's TWI case writes float32 and remaps the source
#        -FLT_MAX sentinel to -9999. The absolute thresholds (8.0, 15.6) in
#        carea_map are calibrated to the ArcPy TWI distribution, so
#        ``threshold_mode: absolute`` still requires twi.vrt as the source.
#        However, ``threshold_mode: percentile`` (the ``twi_reference`` shared-
#        raster step + ``carea_map threshold_mode: percentile`` in
#        depstor_rasters.yml) derives the cutoff from the data and makes the
#        open-source ``twi_hydrodem.vrt`` a fully supported first-class source —
#        see the entry below and
#        docs/superpowers/specs/2026-05-21-carea-smidx-twi-percentile-design.md.
RASTER_TYPES = {
    "elevation": ("NEDSnapshot_merged_fixed_*.tif", "-9999"),
    "slope":     ("NEDSnapshot_merged_slope_*.tif", "-9999"),
    "aspect":    ("NEDSnapshot_merged_aspect_*.tif", "-9999"),
    "fdr":       ("Fdr_merged_*.tif", "255"),
    "twi":       ("Twi_merged_*.tif", "-9999"),
    # Open-source WhiteboxTools TWI (issue #94): CONUS-complete, drop-in grid
    # with fdr.vrt. Tiles report an "unnamed" Albers CRS, so the VRT must be
    # stamped with a named EPSG:5070 to satisfy carea_map's CRS-equality check.
    "twi_hydrodem": ("Twi_hydrodem_*.tif", "-9999"),
}

# VRT types whose source tiles carry an unnamed/implicit CRS and must be
# stamped with an explicit EPSG so strict CRS-equality checks downstream pass.
_SRS_OVERRIDES = {"twi_hydrodem": "EPSG:5070"}

# Overview pyramid for each VRT, written as an external ``.vrt.ovr`` so a
# full-extent QGIS render reads a coarse level instead of decimating the
# full-resolution CONUS grid on every pan/zoom. Continuous surfaces use
# bilinear decimation; categorical FDR (D8 codes) and the circular aspect
# field (0/360 wrap) must use nearest so values aren't averaged.
_OVERVIEW_LEVELS = [2, 4, 8, 16, 32, 64, 128, 256]
_NEAREST_OVERVIEW_VRTS = {"fdr", "aspect"}


def _srs_override(vrt_name: str) -> str | None:
    """EPSG string to force onto the built VRT, or None to keep source CRS."""
    return _SRS_OVERRIDES.get(vrt_name)


def _overview_resampling(vrt_name: str) -> str:
    return "nearest" if vrt_name in _NEAREST_OVERVIEW_VRTS else "bilinear"


def _add_vrt_overviews(vrt_path: Path, resampling: str, logger) -> None:
    """Build an external overview pyramid (``.vrt.ovr``) for ``vrt_path``."""
    ds = gdal.Open(str(vrt_path), gdal.GA_ReadOnly)
    if ds is None:
        raise RuntimeError(f"could not reopen {vrt_path} to build overviews")
    if ds.BuildOverviews(resampling.upper(), _OVERVIEW_LEVELS) != gdal.CE_None:
        raise RuntimeError(
            f"BuildOverviews({resampling}) failed for {vrt_path} — {gdal.GetLastErrorMsg()}"
        )
    ds.FlushCache()
    del ds
    logger.info("Built %s overviews (%s) for %s",
                len(_OVERVIEW_LEVELS), resampling, vrt_path)


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Build CONUS-wide VRTs for elevation, slope, aspect, fdr, twi.

    step_cfg keys (all optional; defaults reference context properties):
      per_vpu_dir  — per-VPU NHDPlus raster directory. Default ``ctx.per_vpu_dir``.
      borders_dir  — Copernicus border-DEM fill directory. Default ``ctx.borders_dir``.
      vrt_dir      — output directory for CONUS VRTs. Default ``ctx.vrt_dir``.

    Returns a dict mapping VRT short name (``elevation``, ``slope``, ...) to
    the built VRT path. Recorded in ctx.paths for any downstream consumers.
    """
    per_vpu_dir = Path(step_cfg.get("per_vpu_dir", ctx.per_vpu_dir))
    borders_dir = Path(step_cfg.get("borders_dir", ctx.borders_dir))
    vrt_dir = Path(step_cfg.get("vrt_dir", ctx.vrt_dir))

    if not per_vpu_dir.exists():
        raise FileNotFoundError(f"per_vpu directory not found: {per_vpu_dir}")
    vrt_dir.mkdir(parents=True, exist_ok=True)

    produced: dict = {}
    built_count = 0
    for vrt_name, (pattern, src_nodata) in RASTER_TYPES.items():
        # Primary NHDPlus VPU tiles (listed last = highest priority)
        primary_files = sorted(per_vpu_dir.glob(f"*/{pattern}"))
        # Fill tiles (listed first = lowest priority). borders_dir is a flat
        # directory of tiles; the legacy `copernicus_fill/` subdirectory under
        # nhd_merged is now a peer of per_vpu_dir, not nested under it.
        fill_files = sorted(borders_dir.glob(pattern)) if borders_dir.exists() else []

        source_files = fill_files + primary_files
        if not source_files:
            logger.warning("No source files found for %s (pattern: */%s)", vrt_name, pattern)
            continue

        vrt_path = vrt_dir / f"{vrt_name}.vrt"
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

        epsg = _srs_override(vrt_name)
        if epsg is not None:
            srs = osr.SpatialReference()
            srs.SetFromUserInput(epsg)
            ds = gdal.Open(str(vrt_path), gdal.GA_Update)
            if ds is None:
                raise RuntimeError(f"could not reopen {vrt_path} to stamp {epsg}")
            if ds.SetProjection(srs.ExportToWkt()) != gdal.CE_None:
                raise RuntimeError(f"SetProjection({epsg}) failed for {vrt_path}")
            ds.FlushCache()
            del ds
            logger.info("Stamped %s with %s", vrt_path, epsg)

        _add_vrt_overviews(vrt_path, _overview_resampling(vrt_name), logger)

        built_count += 1
        produced[f"{vrt_name}_vrt"] = vrt_path
        logger.info("Written: %s (%d sources)", vrt_path, len(source_files))

    if built_count == 0:
        raise RuntimeError(
            f"No VRTs were built. Check that {per_vpu_dir} contains "
            "per-VPU subdirectories with merged GeoTIFFs."
        )
    logger.info("VRT build complete: %d of %d types built", built_count, len(RASTER_TYPES))
    return produced

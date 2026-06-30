"""Per-VPU slope + aspect rasters from merged NEDSnapshot DEM.

Library entrypoint for the ``compute_slope_aspect`` step in the shared-raster
orchestrator (``scripts/build_shared_rasters.py``). Registered via the
BUILDERS dict in ``shared_rasters/__init__.py`` and called by the
orchestrator's STEP_ORDER walk.

Per-VPU iteration happens inside this builder rather than in the orchestrator
walker — matches the SharedRastersContext.vpus convention. Set ``--vpus`` at
the CLI to scope a partial run; this builder honours that scope.
"""

from __future__ import annotations

from pathlib import Path

import richdem as rd
import rioxarray

from .cog import cog_temp, to_cog
from .context import SharedRastersContext

# The per-VPU merged DEM tiles (written by merge_rpu_by_vpu) declare and use
# nodata=-99.99: the source RPU data is in centimetres (nodata=-9999 cm),
# divided by 100 to convert to metres (-99.99 m). Downstream nodata conventions:
#   * rd.LoadGDAL must use no_data=-99.99 so RichDEM masks the VPU rectangular
#     fill region rather than treating it as valid flat terrain (which would
#     produce spurious slope=0 / aspect=0 output).
#   * The _fixed_ tile is written with fillna(-9999) + write_nodata(-9999) so
#     build_vrt can use srcNodata="-9999" for the elevation VRT — same value
#     RichDEM SaveGDAL writes for slope/aspect tiles.
DEM_NODATA = -99.99


def _process_vpu(vpu: str, input_dir: Path, output_dir: Path, force: bool, logger) -> None:
    dem_path = input_dir / vpu / f"NEDSnapshot_merged_{vpu}.tif"
    dem_fixed_path = input_dir / vpu / f"NEDSnapshot_merged_fixed_{vpu}.tif"
    slope_out = output_dir / vpu / f"NEDSnapshot_merged_slope_{vpu}.tif"
    aspect_out = output_dir / vpu / f"NEDSnapshot_merged_aspect_{vpu}.tif"

    if not dem_path.exists():
        raise FileNotFoundError(f"DEM not found: {dem_path}")

    # Always regenerate the _fixed_ tile — it is a fast rioxarray copy and its
    # nodata convention must stay in sync with build_vrt's srcNodata value.
    # The _fixed_ tile is the elevation VRT source, consumed only by
    # GDAL/rasterio/QGIS (never WBT), so it is written as a COG (tiled 512 +
    # overviews + ZSTD). rioxarray writes a plain temp; to_cog reorganizes it
    # into the COG layout, then the temp is removed.
    logger.info("[VPU %s] creating fixed-nodata NEDSnapshot (COG)", vpu)
    da = rioxarray.open_rasterio(dem_path, masked=True).squeeze()
    da_fixed = da.fillna(-9999)
    da_fixed.rio.write_nodata(-9999, inplace=True)
    with cog_temp(dem_fixed_path) as fixed_tmp:
        da_fixed.rio.to_raster(fixed_tmp)
        to_cog(fixed_tmp, dem_fixed_path, overview_resampling="BILINEAR", predictor=3)

    if not force and slope_out.exists() and aspect_out.exists():
        logger.info("[VPU %s] slope/aspect exist, skipping (use --force to overwrite): %s",
                    vpu, slope_out)
        return

    logger.info("[VPU %s] loading DEM: %s", vpu, dem_path)
    dem = rd.LoadGDAL(str(dem_path), no_data=DEM_NODATA)

    # RichDEM SaveGDAL writes a plain (striped, uncompressed) GeoTIFF; convert
    # each to a COG. Slope is continuous -> bilinear overviews; aspect is a
    # circular 0-360 field whose 0/360 seam must not be averaged -> nearest.
    slope_out.parent.mkdir(parents=True, exist_ok=True)

    logger.info("[VPU %s] computing slope (degrees)...", vpu)
    slope = rd.TerrainAttribute(dem, attrib="slope_degrees")
    with cog_temp(slope_out) as slope_tmp:
        rd.SaveGDAL(str(slope_tmp), slope)
        to_cog(slope_tmp, slope_out, overview_resampling="BILINEAR", predictor=3)
    logger.info("[VPU %s] slope saved (COG): %s", vpu, slope_out)

    logger.info("[VPU %s] computing aspect...", vpu)
    aspect = rd.TerrainAttribute(dem, attrib="aspect")
    with cog_temp(aspect_out) as aspect_tmp:
        rd.SaveGDAL(str(aspect_tmp), aspect)
        to_cog(aspect_tmp, aspect_out, overview_resampling="NEAREST", predictor=3)
    logger.info("[VPU %s] aspect saved (COG): %s", vpu, aspect_out)


def build(step_cfg: dict, ctx: SharedRastersContext, logger) -> dict:
    """Compute slope + aspect for every VPU in ``ctx.vpus``.

    step_cfg keys (all optional; defaults reference ``ctx.per_vpu_dir``):
      input_dir  — per-VPU DEM source directory
      output_dir — per-VPU slope/aspect output directory

    Returns an empty dict — per-VPU outputs are not registered in ctx.paths
    (downstream consumers re-template per-VPU paths off conventional patterns).
    """
    input_dir = Path(step_cfg.get("input_dir", ctx.per_vpu_dir))
    output_dir = Path(step_cfg.get("output_dir", ctx.per_vpu_dir))
    output_dir.mkdir(parents=True, exist_ok=True)

    if not ctx.vpus:
        logger.warning("compute_slope_aspect: ctx.vpus is empty, nothing to do")
        return {}

    for vpu in ctx.vpus:
        _process_vpu(vpu, input_dir, output_dir, ctx.force, logger)

    return {}

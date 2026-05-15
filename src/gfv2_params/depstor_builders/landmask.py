"""Build land_mask.tif by rasterising the HRU fabric onto the template grid.

Authoritative land/domain mask for every other depstor builder. Replaces the
old template-DEM-nodata mask: the hydro-conditioned DEM carries valid (often
garbage) elevations over coastal ocean, so its nodata footprint bulged into the
sea and those bulges leaked into the dense outputs.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from ..depstor import RasterInfo, rasterize_binary, write_uint8_binary
from .context import BuildContext


def _load_hru(path: Path, layer: str, logger):
    try:
        return gpd.read_file(path, layer=layer, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer)


def rasterize_land_mask(
    template_path: Path,
    hru_gpkg: Path,
    hru_layer: str,
    output_path: Path,
    logger,
) -> tuple:
    """Rasterise the HRU fabric to a uint8 1/255 land mask at the template grid."""
    info = RasterInfo.from_path(template_path)
    hru_gdf = _load_hru(hru_gpkg, hru_layer, logger)
    hru_gdf = hru_gdf[hru_gdf.geometry.notna() & ~hru_gdf.geometry.is_empty]
    # all_touched=True: inclusive at the outer coastline so thin edge HRUs are
    # not clipped. create_zonal_params is the precise arbiter downstream.
    binary = rasterize_binary(hru_gdf, info, all_touched=True)
    write_uint8_binary(binary, info, output_path)
    return binary, len(hru_gdf)


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    output_path = ctx.resolve_output(step_cfg["output"])

    if not ctx.template_path.exists():
        raise FileNotFoundError(f"Template raster not found: {ctx.template_path}")
    if not ctx.hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {ctx.hru_gpkg}")

    logger.info("--- landmask ---")
    logger.info("  HRU fabric: %s (layer=%s)", ctx.hru_gpkg, ctx.hru_layer)
    logger.info("  Output    : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output already exists — skipping (pass --force to rebuild)")
        return {"landmask": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    logger.info("  Template grid: %dx%d, CRS=%s", info.width, info.height, info.crs)

    binary, n_polys = rasterize_land_mask(
        ctx.template_path, ctx.hru_gpkg, ctx.hru_layer, output_path, logger
    )
    n_land = int((binary == 1).sum())
    logger.info(
        "  Rasterised %d HRU polygons | %d land cells (%.2f%% of grid)",
        n_polys, n_land, 100 * n_land / binary.size,
    )
    return {"landmask": output_path}

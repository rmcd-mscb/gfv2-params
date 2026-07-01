"""Rasterise waterbody polygons + scipy connected-component labels."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from ..depstor import (
    RasterInfo,
    clump_regions,
    rasterize_binary,
    read_land_mask,
    write_int32_regions,
    write_uint8_binary,
)
from ..nhd_ftypes import EXCLUDE_WATERBODY_FTYPES
from .context import BuildContext


def _load_waterbodies(path: Path, layer: str | None, logger):
    try:
        return gpd.read_file(path, layer=layer, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer)


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError(
            "waterbody step needs `waterbody_gpkg` and `waterbody_layer` in fabric profile."
        )
    outputs = step_cfg["outputs"]
    binary_path = ctx.resolve_output(outputs["binary"])
    regions_path = ctx.resolve_output(outputs["regions"])
    landmask_path = ctx.require("landmask")
    min_area = float(step_cfg.get("min_area_threshold", 900.0))

    if not ctx.waterbody_gpkg.exists():
        raise FileNotFoundError(f"Waterbody gpkg not found: {ctx.waterbody_gpkg}")

    logger.info("--- waterbody ---")
    logger.info("  Waterbody gpkg: %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  Binary out    : %s", binary_path)
    logger.info("  Regions out   : %s", regions_path)
    logger.info("  Min area      : %.1f m^2", min_area)

    if binary_path.exists() and regions_path.exists() and not ctx.force:
        logger.info("  Both outputs exist — skipping (pass --force to rebuild)")
        return {"wbody_binary": binary_path, "wbody_regions": regions_path}

    info = RasterInfo.from_path(ctx.template_path)
    wb_gdf = _load_waterbodies(ctx.waterbody_gpkg, ctx.waterbody_layer, logger)
    if wb_gdf.crs != info.crs:
        logger.info("  Reprojecting wbodies from %s to %s", wb_gdf.crs, info.crs)
        wb_gdf = wb_gdf.to_crs(info.crs)
    wb_gdf = wb_gdf[wb_gdf.geometry.notna() & ~wb_gdf.geometry.is_empty]

    if "FTYPE" in wb_gdf.columns:
        n_pre = len(wb_gdf)
        wb_gdf = wb_gdf[~wb_gdf["FTYPE"].isin(EXCLUDE_WATERBODY_FTYPES)].copy()
        n_excluded = n_pre - len(wb_gdf)
        if n_excluded:
            logger.info(
                "  excluded %d Ice Mass waterbodies (not depression storage; "
                "treated as land)", n_excluded,
            )
    else:
        raise KeyError(
            "waterbody layer has no FTYPE column — cannot exclude Ice Mass "
            "(EXCLUDE_WATERBODY_FTYPES); refusing to write a raster that would "
            "misclassify glacier/permanent-ice cells as depression storage. A "
            "genuinely FTYPE-less waterbody layer is an upstream data problem "
            "(check the source gpkg), not something this pipeline should paper "
            "over."
        )

    n_before = len(wb_gdf)
    wb_gdf = wb_gdf[wb_gdf.geometry.area >= min_area].copy()
    logger.info("  Loaded %d wbodies, kept %d after >= %.1f m^2 filter", n_before, len(wb_gdf), min_area)

    binary = rasterize_binary(wb_gdf, info, all_touched=False)
    binary[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    n_in = int((binary == 1).sum())
    logger.info("  %d wbody cells after land mask", n_in)
    write_uint8_binary(binary, info, binary_path)

    regions = clump_regions(binary)
    n_regions = int(regions.max())
    logger.info("  Labeled %d connected components (8-connectivity)", n_regions)
    write_int32_regions(regions, info, regions_path)

    return {"wbody_binary": binary_path, "wbody_regions": regions_path}

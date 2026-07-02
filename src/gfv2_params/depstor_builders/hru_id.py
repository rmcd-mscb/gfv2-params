"""Build hru_id.tif: per-cell HRU id (nat_hru_id) rasterised onto the template.

The open-source equivalent of the legacy `nhrug`. Consumed by `routing_hru`
(to label depressions by HRU) and `same_hru_drains` (the same-HRU test). This
is a raster-space HRU identity used only for the same-HRU restriction; per-HRU
parameter COUNTS still use gdptools zonal weights downstream.
"""
from __future__ import annotations

import geopandas as gpd

from ..depstor import RasterInfo, rasterize_ids, write_int32_regions
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    output_path = ctx.resolve_output(step_cfg["output"])
    if not ctx.template_path.exists():
        raise FileNotFoundError(f"Template raster not found: {ctx.template_path}")
    if not ctx.hru_gpkg.exists():
        raise FileNotFoundError(f"HRU fabric gpkg not found: {ctx.hru_gpkg}")

    logger.info("--- hru_id ---")
    logger.info("  HRU fabric: %s (layer=%s, id=%s)", ctx.hru_gpkg, ctx.hru_layer, ctx.id_feature)
    logger.info("  Output    : %s", output_path)
    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"hru_id": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    gdf = gpd.read_file(ctx.hru_gpkg, layer=ctx.hru_layer)
    gdf = gdf[gdf.geometry.notna() & ~gdf.geometry.is_empty]
    if (gdf[ctx.id_feature] <= 0).any():
        raise ValueError(f"{ctx.id_feature} must be positive (0 is the no-HRU sentinel).")
    ids = rasterize_ids(gdf, ctx.id_feature, info)
    write_int32_regions(ids, info, output_path)
    n = int((ids > 0).sum())
    logger.info("  Rasterised %d HRUs | %d labelled cells (%.2f%%)", len(gdf), n, 100 * n / ids.size)
    return {"hru_id": output_path}

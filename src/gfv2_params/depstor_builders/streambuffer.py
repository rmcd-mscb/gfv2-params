"""Buffer stream segments and burn them to a uint8 binary raster."""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd

from ..depstor import RasterInfo, rasterize_binary, read_land_mask, write_uint8_binary
from .context import BuildContext


def _load_segments(path: Path, layer: str | None, logger):
    try:
        return gpd.read_file(path, layer=layer, use_arrow=True)
    except ImportError:
        logger.warning(
            "PyArrow unavailable for vector load; falling back to fiona. "
            "Install pyarrow for faster reads."
        )
        return gpd.read_file(path, layer=layer)


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.segments_gpkg is None:
        raise KeyError("streambuffer step needs `segments_gpkg` in fabric profile.")
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")
    buffer_distance = float(step_cfg.get("buffer_distance", 60))

    if not ctx.segments_gpkg.exists():
        raise FileNotFoundError(f"Segments gpkg not found: {ctx.segments_gpkg}")

    logger.info("--- streambuffer ---")
    logger.info("  Segments gpkg: %s (layer=%s)", ctx.segments_gpkg, ctx.segments_layer)
    logger.info("  Output       : %s", output_path)
    logger.info("  Buffer       : %s metres", buffer_distance)

    if output_path.exists() and not ctx.force:
        logger.info("  Output already exists — skipping (pass --force to rebuild)")
        return {"stream_buffer": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    seg_gdf = _load_segments(ctx.segments_gpkg, ctx.segments_layer, logger)
    seg_gdf = seg_gdf[seg_gdf.geometry.notna() & ~seg_gdf.geometry.is_empty].copy()
    if seg_gdf.crs != info.crs:
        logger.info("  Reprojecting segments from %s to %s", seg_gdf.crs, info.crs)
        seg_gdf = seg_gdf.to_crs(info.crs)
    seg_gdf["geometry"] = seg_gdf.geometry.buffer(buffer_distance)

    binary = rasterize_binary(seg_gdf, info, all_touched=True)
    binary[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    write_uint8_binary(binary, info, output_path)
    n_in = int((binary == 1).sum())
    logger.info(
        "  %d cells inside stream buffer (%.4f%%)",
        n_in, 100 * n_in / binary.size,
    )

    return {"stream_buffer": output_path}

"""Threshold the NLCD fractional-impervious source to a uint8 binary raster."""

from __future__ import annotations

from pathlib import Path

import rasterio
from osgeo import gdal, gdalconst

from ..depstor import RasterInfo, read_land_mask, threshold_above, write_uint8_binary
from .context import BuildContext


def _warp_to_template(src_path: Path, info: RasterInfo, out_path: Path) -> None:
    """Warp src_path to the template grid (EPSG:5070, 30m, exact bounds).

    Bilinear resampling — source is a continuous 0-100 percentage. gdal.Warp
    auto-detects the source nodata and excludes it from the kernel.
    """
    output_bounds = (info.bounds.left, info.bounds.bottom, info.bounds.right, info.bounds.top)
    warp_ds = gdal.Warp(
        str(out_path),
        str(src_path),
        format="GTiff",
        outputBounds=output_bounds,
        width=info.width,
        height=info.height,
        dstSRS=info.crs.to_string(),
        resampleAlg=gdalconst.GRA_Bilinear,
        outputType=gdal.GDT_Float32,
        creationOptions=[
            "COMPRESS=LZW", "TILED=YES",
            "BLOCKXSIZE=512", "BLOCKYSIZE=512", "BIGTIFF=YES",
        ],
    )
    if warp_ds is None:
        raise RuntimeError(
            f"gdal.Warp failed: {src_path} -> {out_path} ({gdal.GetLastErrorMsg()})"
        )
    warp_ds.FlushCache()
    del warp_ds


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.imperv_source is None:
        raise KeyError(
            "imperv step requires `source` (path to NLCD fractional-impervious "
            "raster) at the step level or `imperv_source` in fabric profile."
        )
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")
    threshold = float(step_cfg.get("threshold", 50))

    if not ctx.imperv_source.exists():
        raise FileNotFoundError(f"Imperv source raster not found: {ctx.imperv_source}")

    logger.info("--- imperv ---")
    logger.info("  Source   : %s", ctx.imperv_source)
    logger.info("  Output   : %s", output_path)
    logger.info("  Threshold: %s%% (cells >= threshold marked impervious)", threshold)

    if output_path.exists() and not ctx.force:
        logger.info("  Output already exists — skipping (pass --force to rebuild)")
        return {"imperv": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    warped_path = output_path.with_suffix(".warped.tif")
    try:
        _warp_to_template(ctx.imperv_source, info, warped_path)
        with rasterio.open(warped_path) as src:
            data = src.read(1)
            src_nodata = src.nodata
        binary = threshold_above(data, threshold, src_nodata)
        binary[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
        write_uint8_binary(binary, info, output_path)
        n_imp = int((binary == 1).sum())
        logger.info(
            "  %d / %d cells impervious (%.2f%%)",
            n_imp, binary.size, 100 * n_imp / binary.size,
        )
    finally:
        if warped_path.exists():
            warped_path.unlink()

    return {"imperv": output_path}

"""WhiteboxTools Watershed from dprst pour-points on the staged FDR."""

from __future__ import annotations

import os

import numpy as np
import rasterio
from osgeo import gdal

from ..depstor import RasterInfo, read_land_mask, write_uint8_binary
from ..wbt import find_whitebox_tools_binary, run_streamed
from .context import BuildContext


def _align_fdr_to_dprst_grid(fdr_path, dprst_path, out_path, logger) -> None:
    """Materialise the FDR onto the dprst grid as a WBT-readable GeoTIFF.

    Streams via gdal.Warp (block-by-block, bounded RAM) rather than an in-memory
    rioxarray.reproject_match: the latter materialised the full 16.9-billion-cell
    CONUS array plus float intermediates and OOM-killed the step at ~400 GB on a
    uint8 source that is only ~17 GB. The FDR clip already shares the dprst grid,
    so this is a near-identity nearest-neighbour resample that just realises the
    VRT into a concrete raster WBT can read.
    """
    logger.info("  Aligning FDR to dprst grid (gdal.Warp, streaming)...")
    gdal.UseExceptions()
    with rasterio.open(dprst_path) as d:
        b = d.bounds
        width, height, dst_srs = d.width, d.height, d.crs.to_wkt()
    with rasterio.open(fdr_path) as f:
        src_nodata = f.nodata
    if out_path.exists():
        out_path.unlink()  # clear any stale/0-byte file from a prior crash
    ds = gdal.Warp(
        str(out_path),
        str(fdr_path),
        options=gdal.WarpOptions(
            format="GTiff",
            outputBounds=[b.left, b.bottom, b.right, b.top],
            width=width,
            height=height,
            dstSRS=dst_srs,
            resampleAlg="near",  # D8 codes are categorical — never interpolate
            outputType=gdal.GDT_Byte,
            srcNodata=src_nodata,
            dstNodata=255,
            multithread=True,
            warpMemoryLimit=2_000_000_000,  # 2 GB warp buffer caps peak RAM
            # NOTE: no PREDICTOR — WBT silently corrupts LZW+predictor=2 inputs
            # (see CLAUDE.md / whitebox_predictor2 gotcha).
            creationOptions=["COMPRESS=LZW", "TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256", "BIGTIFF=YES"],
        ),
    )
    if ds is None:
        raise RuntimeError(f"gdal.Warp produced no dataset for {out_path} — FDR alignment failed.")
    ds = None  # flush/close


def _prepare_pour_points(dprst_path, out_path, logger) -> None:
    """Convert dprst_binary.tif (1=pour, 255=nodata) into 0/1 (nodata=0).

    WBT's Watershed reads the raw values and treats every non-zero value as a
    pour-point — it ignores the GeoTIFF nodata tag. The 255 cells therefore
    leak in unless we re-encode.
    """
    logger.info("  Converting dprst_binary.tif (1/255) -> 0/1 pour-points (nodata=0)...")
    with rasterio.open(dprst_path) as src:
        data = src.read(1)
        profile = src.profile.copy()
    pour = np.where(data == 1, np.uint8(1), np.uint8(0))
    profile.update(nodata=0, compress="LZW")
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(pour, 1)


def _run_whitebox_watershed(fdr_path, pour_pts_path, output_path, logger) -> None:
    runner = find_whitebox_tools_binary()
    logger.info("  WhiteboxTools binary: %s", runner)
    cmd = [
        runner,
        f"--wd={os.getcwd()}",
        "--max_procs=-1",
        "-r=Watershed",
        f"--d8_pntr={fdr_path}",
        f"--pour_pts={pour_pts_path}",
        f"--output={output_path}",
        "--esri_pntr",
        "-v",
    ]
    logger.info("  Running: %s", " ".join(cmd))
    run_streamed(cmd, tool="Watershed", logger=logger)


def _watershed_to_binary(watershed_path, landmask_path, info, out_path, logger) -> None:
    with rasterio.open(watershed_path) as src:
        data = src.read(1)
        src_nodata = src.nodata
    if src_nodata is None:
        valid = data > 0
    elif isinstance(src_nodata, float) and np.isnan(src_nodata):
        valid = ~np.isnan(data)
    else:
        valid = data != src_nodata
    binary = np.where(valid, np.uint8(1), np.uint8(255))
    binary[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    n_in = int((binary == 1).sum())
    pct = 100 * n_in / binary.size
    # >50% coverage almost certainly means the pour-points nodata bug — PR #56.
    if pct > 50:
        logger.warning(
            "Drains-to-dprst coverage is %.2f%% of the grid — unusually high. "
            "Check pour-points nodata=0 (not 255) and FDR alignment.",
            pct,
        )
    write_uint8_binary(binary, info, out_path)
    logger.info(
        "  Drains-to-dprst mask written: %s (%d cells, %.4f%% of grid)",
        out_path, n_in, pct,
    )


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.fdr_raster is None:
        raise KeyError("routing step needs `fdr_raster` in fabric profile.")
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")
    dprst_path = ctx.require("dprst")
    keep_intermediates = bool(step_cfg.get("keep_intermediates", False))

    if not ctx.fdr_raster.exists():
        raise FileNotFoundError(f"FDR raster not found: {ctx.fdr_raster}")

    logger.info("--- routing ---")
    logger.info("  FDR    : %s", ctx.fdr_raster)
    logger.info("  Output : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"drains_to_dprst": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fdr_aligned = output_path.parent / "fdr_aligned.tif"
    pour_pts = output_path.parent / "dprst_pourpts.tif"
    watershed_raw = output_path.parent / "hru_to_dprst_labels.tif"

    try:
        _align_fdr_to_dprst_grid(ctx.fdr_raster, dprst_path, fdr_aligned, logger)
        _prepare_pour_points(dprst_path, pour_pts, logger)
        _run_whitebox_watershed(fdr_aligned, pour_pts, watershed_raw, logger)
        _watershed_to_binary(watershed_raw, landmask_path, info, output_path, logger)
    finally:
        if not keep_intermediates:
            for p in (fdr_aligned, pour_pts, watershed_raw):
                if p.exists():
                    p.unlink()

    return {"drains_to_dprst": output_path}

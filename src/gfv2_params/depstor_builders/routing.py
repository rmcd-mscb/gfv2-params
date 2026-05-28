"""WhiteboxTools Watershed from dprst pour-points, tiled per VPU.

Routing the full-CONUS FDR + pour-points through WBT Watershed OOMs (WBT loads
every raster as f64; ~3 x 135 GB > the 503 GB node ceiling). NHDPlus VPU
boundaries follow drainage divides, so each VPU's contributing area is local: we
route each VPU in isolation (FDR masked to the VPU) and mosaic the per-VPU
results — see docs/superpowers/specs/2026-05-28-depstor-per-vpu-routing-design.md.
"""

from __future__ import annotations

import numpy as np
import rasterio
from osgeo import gdal
from rasterio.windows import Window
from rasterio.windows import transform as window_transform

from ..depstor import (
    RasterInfo,
    assign_vpu_drains,
    mask_fdr_to_vpu,
    read_land_mask,
    vpu_bbox,
    vpu_codes_present,
    vpu_pour_points,
    write_uint8_binary,
)
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
        out_path.unlink()
    ds = gdal.Warp(
        str(out_path),
        str(fdr_path),
        options=gdal.WarpOptions(
            format="GTiff",
            outputBounds=[b.left, b.bottom, b.right, b.top],
            width=width,
            height=height,
            dstSRS=dst_srs,
            resampleAlg="near",
            outputType=gdal.GDT_Byte,
            srcNodata=src_nodata,
            dstNodata=255,
            multithread=True,
            warpMemoryLimit=2_000_000_000,
            creationOptions=["COMPRESS=LZW", "TILED=YES", "BLOCKXSIZE=256", "BLOCKYSIZE=256", "BIGTIFF=YES"],
        ),
    )
    if ds is None:
        raise RuntimeError(f"gdal.Warp produced no dataset for {out_path} — FDR alignment failed.")
    ds = None  # flush/close


def _run_whitebox_watershed(fdr_path, pour_pts_path, output_path, logger) -> None:
    import os

    runner = find_whitebox_tools_binary()
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
    run_streamed(cmd, tool="Watershed", logger=logger)


def _write_window_tif(arr, window, info, out_path, nodata) -> None:
    """Write a windowed uint8 raster carrying the window's geotransform."""
    profile = {
        "driver": "GTiff", "height": arr.shape[0], "width": arr.shape[1], "count": 1,
        "dtype": "uint8", "crs": info.crs,
        "transform": window_transform(window, info.transform),
        "nodata": nodata, "compress": "LZW", "tiled": True,
        "blockxsize": 256, "blockysize": 256, "BIGTIFF": "YES",
    }
    with rasterio.open(out_path, "w", **profile) as dst:
        dst.write(arr, 1)


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.fdr_raster is None:
        raise KeyError("routing step needs `fdr_raster` in fabric profile.")
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")
    dprst_path = ctx.require("dprst")
    vpu_id_path = ctx.require("vpu_id")
    keep_intermediates = bool(step_cfg.get("keep_intermediates", False))

    if not ctx.fdr_raster.exists():
        raise FileNotFoundError(f"FDR raster not found: {ctx.fdr_raster}")

    logger.info("--- routing (per-VPU tiled) ---")
    logger.info("  FDR    : %s", ctx.fdr_raster)
    logger.info("  vpu_id : %s", vpu_id_path)
    logger.info("  Output : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"drains_to_dprst": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fdr_aligned = output_path.parent / "fdr_aligned.tif"

    _align_fdr_to_dprst_grid(ctx.fdr_raster, dprst_path, fdr_aligned, logger)

    with rasterio.open(vpu_id_path) as src:
        vpu_id = src.read(1)
    codes = vpu_codes_present(vpu_id)
    logger.info("  Tiling routing over %d VPU(s): %s", len(codes), codes)

    drains = np.full((info.height, info.width), np.uint8(255), dtype=np.uint8)

    with rasterio.open(fdr_aligned) as fdr_src, rasterio.open(dprst_path) as dprst_src:
        for code in codes:
            bbox = vpu_bbox(vpu_id, code)
            r0, r1, c0, c1 = bbox
            window = Window(c0, r0, c1 - c0, r1 - r0)
            vpu_win = vpu_id[r0:r1, c0:c1]
            fdr_win = fdr_src.read(1, window=window)
            dprst_win = dprst_src.read(1, window=window)

            fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code, nodata=255)
            pour = vpu_pour_points(dprst_win, vpu_win, code)

            tile_fdr = output_path.parent / f"_fdr_vpu{code}.tif"
            tile_pour = output_path.parent / f"_pour_vpu{code}.tif"
            tile_ws = output_path.parent / f"_ws_vpu{code}.tif"
            try:
                _write_window_tif(fdr_masked, window, info, tile_fdr, nodata=255)
                _write_window_tif(pour, window, info, tile_pour, nodata=0)
                _run_whitebox_watershed(tile_fdr, tile_pour, tile_ws, logger)
                with rasterio.open(tile_ws) as ws_src:
                    ws_win = ws_src.read(1)
                    ws_nodata = ws_src.nodata
                assign_vpu_drains(drains, vpu_id, code, bbox, ws_win, ws_nodata)
                n_vpu = int((drains[r0:r1, c0:c1][vpu_win == code] == 1).sum())
                logger.info("  VPU %d: %d cells drain to dprst", code, n_vpu)
            finally:
                if not keep_intermediates:
                    for p in (tile_fdr, tile_pour, tile_ws):
                        if p.exists():
                            p.unlink()

    del vpu_id  # free the CONUS uint8 partition before the final mask
    if not keep_intermediates and fdr_aligned.exists():
        fdr_aligned.unlink()

    drains[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    n_in = int((drains == 1).sum())
    pct = 100 * n_in / drains.size
    if pct > 50:
        logger.warning(
            "Drains-to-dprst coverage is %.2f%% of the grid — unusually high. "
            "Check pour-points (nodata=0) and FDR/vpu_id alignment.", pct,
        )
    write_uint8_binary(drains, info, output_path)
    logger.info(
        "  Drains-to-dprst mask written: %s (%d cells, %.4f%% of grid)",
        output_path, n_in, pct,
    )
    return {"drains_to_dprst": output_path}

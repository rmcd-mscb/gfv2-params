"""Upslope-of-depression routing, tiled per VPU.

For each cell, marks whether its ESRI-D8 flow path reaches a depression
pour-point (`drains_to_dprst.tif`). The per-tile computation is the in-process
`d8_routing.drains_to_dprst_kernel` — a cycle-safe O(N) traversal that replaced
WhiteboxTools `Watershed`, which hung on CONUS VPU 2 (a flow-cycle /
pathological-trace stall, not OOM). See
docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md.

NHDPlus VPU boundaries follow drainage divides, so each VPU's contributing area
is local: we route each VPU in isolation (FDR masked to the VPU via vpu_id) and
mosaic the per-VPU results into the CONUS grid. Memory note: this keeps the
whole-CONUS `vpu_id` + `drains` arrays in RAM (~34 GB); the file-based
~6-9 GB workstation variant is tracked in issue #129.
"""

from __future__ import annotations

import numpy as np
import rasterio
from osgeo import gdal
from rasterio.windows import Window

from ..d8_routing import drains_to_dprst_kernel
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

    try:
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

                # In-process D8 traversal (replaces WBT Watershed). Output is
                # 1 where the cell drains to a pour-point, else 0, so
                # assign_vpu_drains treats 0 as nodata.
                ws_win = drains_to_dprst_kernel(fdr_masked, pour, fdr_nodata=255)
                assign_vpu_drains(drains, vpu_id, code, bbox, ws_win, ws_nodata=0)
                n_vpu = int((drains[r0:r1, c0:c1][vpu_win == code] == 1).sum())
                logger.info("  VPU %d: %d cells drain to dprst", code, n_vpu)
    finally:
        if not keep_intermediates and fdr_aligned.exists():
            fdr_aligned.unlink()

    del vpu_id  # free the CONUS uint8 partition before the final mask

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

"""HRU-labeled, barrier-aware upslope routing → drains_to_dprst_hru.tif.

Same per-VPU D8 tiling as `routing`, but each depression cell is labelled by its
HRU id (hru_id.tif) and the labeled kernel attributes every draining cell to the
HRU of the depression it reaches. On-stream waterbodies are barriers. Written
per-VPU windowed: the int32 output is ~4x the binary drains, so it is never held
whole-CONUS.
"""
from __future__ import annotations

import numpy as np
import rasterio
from rasterio.windows import Window

from ..d8_routing import drains_to_dprst_labeled_kernel
from ..depstor import (
    RasterInfo,
    align_fdr_to_dprst_grid,
    assert_raster_aligned,
    mask_fdr_to_vpu,
    read_aligned_uint8,
    vpu_bbox,
    vpu_codes_present,
    vpu_pour_points,
)
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.fdr_raster is None or not ctx.fdr_raster.exists():
        raise FileNotFoundError(f"routing_hru needs fdr_raster: {ctx.fdr_raster}")
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")
    dprst_path = ctx.require("dprst")
    onstream_path = ctx.require("onstream")
    vpu_id_path = ctx.require("vpu_id")
    hru_id_path = ctx.require("hru_id")
    keep_intermediates = bool(step_cfg.get("keep_intermediates", False))

    logger.info("--- routing_hru (per-VPU labeled) ---")
    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"drains_to_dprst_hru": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fdr_aligned = output_path.parent / "fdr_aligned_hru.tif"

    try:
        align_fdr_to_dprst_grid(ctx.fdr_raster, dprst_path, fdr_aligned, logger)
        vpu_id = read_aligned_uint8(vpu_id_path, info)
        codes = vpu_codes_present(vpu_id)

        profile = dict(
            driver="GTiff", height=info.height, width=info.width, count=1,
            dtype="int32", crs=info.crs, transform=info.transform, nodata=0,
            compress="LZW", tiled=True, blockxsize=256, blockysize=256,
        )
        profile["BIGTIFF"] = "YES"
        with rasterio.open(output_path, "w+", **profile) as dst, \
                rasterio.open(fdr_aligned) as fdr_src, \
                rasterio.open(dprst_path) as dprst_src, \
                rasterio.open(onstream_path) as onstream_src, \
                rasterio.open(hru_id_path) as hru_src, \
                rasterio.open(landmask_path) as land_src:
            # dprst/onstream/hru_id/landmask are windowed by vpu_id's grid; a
            # same-shape but differently-georeferenced raster would be read at
            # the wrong origin silently. Assert all are on the template grid
            # (as routing.py does for dprst/onstream).
            assert_raster_aligned(dprst_src, info, "dprst")
            assert_raster_aligned(onstream_src, info, "onstream")
            assert_raster_aligned(hru_src, info, "hru_id")
            assert_raster_aligned(land_src, info, "landmask")
            n_total = 0
            for code in codes:
                bbox = vpu_bbox(vpu_id, code)
                r0, r1, c0, c1 = bbox
                window = Window(c0, r0, c1 - c0, r1 - r0)
                vpu_win = vpu_id[r0:r1, c0:c1]
                fdr_win = fdr_src.read(1, window=window)
                dprst_win = dprst_src.read(1, window=window)
                onstream_win = onstream_src.read(1, window=window)
                hru_win = hru_src.read(1, window=window)
                land_win = land_src.read(1, window=window)

                fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code, nodata=255)
                label = np.where((dprst_win == 1) & (vpu_win == code), hru_win, 0).astype(np.int32)
                barrier = vpu_pour_points(onstream_win, vpu_win, code)
                out, n_cycles = drains_to_dprst_labeled_kernel(fdr_masked, label, barrier, fdr_nodata=255)
                if n_cycles:
                    logger.warning("  VPU %d: %d flow cycle(s) — cells non-draining", code, n_cycles)

                # read-modify-write only this VPU's cells (bboxes overlap at corners),
                # and only where land_mask.tif confirms the cell is land (never use
                # FDR/hydro-DEM nodata as a land mask — see CLAUDE.md).
                existing = dst.read(1, window=window)
                sel = (vpu_win == code) & (out > 0) & (land_win == 1)
                existing[sel] = out[sel]
                dst.write(existing, 1, window=window)
                n_sel = int(sel.sum())
                n_total += n_sel
                logger.info("  VPU %d: %d labelled drain cells", code, n_sel)
    finally:
        if not keep_intermediates and fdr_aligned.exists():
            fdr_aligned.unlink()

    if n_total == 0:
        raise RuntimeError(
            f"drains_to_dprst_hru is all-nodata after routing {len(codes)} VPU(s) — "
            "an all-empty mask is never a valid product. Check that dprst, vpu_id, "
            "hru_id, and the FDR are aligned to the same template grid."
        )
    return {"drains_to_dprst_hru": output_path}

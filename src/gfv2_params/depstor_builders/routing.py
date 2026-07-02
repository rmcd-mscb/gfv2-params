"""Upslope-of-depression routing, tiled per VPU.

For each cell, marks whether its ESRI-D8 flow path reaches a depression
pour-point (`drains_to_dprst.tif`). The per-tile computation is the in-process
`d8_routing.drains_to_dprst_kernel` — a cycle-safe O(N) traversal that replaced
WhiteboxTools `Watershed`, which hung on CONUS VPU 2 (a flow-cycle /
pathological-trace stall, not OOM). See
docs/superpowers/specs/2026-05-29-depstor-d8-routing-kernel-design.md.

On-stream waterbody cells (`onstream_binary.tif`, emitted by the `dprst` step)
are passed to the kernel as traversal barriers: a flow path that reaches an
on-stream waterbody before it reaches a dprst pour-point stops there and is
marked non-draining, so land captured by an on-stream lake is not attributed
to a downstream depression — traversal stops at the first waterbody on each
flow path. This makes `drains_to_dprst` a strict subtraction from the
pre-barrier raster (coverage can only decrease).

NHDPlus VPU boundaries follow drainage divides, so each VPU's contributing area
is local: we route each VPU in isolation (FDR masked to the VPU via vpu_id) and
mosaic the per-VPU results into the CONUS grid. Memory note: this keeps the
whole-CONUS `vpu_id` + `drains` arrays in RAM (~34 GB); the file-based
~6-9 GB workstation variant is tracked in issue #129.
"""

from __future__ import annotations

import numpy as np
import rasterio
from rasterio.windows import Window

from ..d8_routing import drains_to_dprst_kernel
from ..depstor import (
    RasterInfo,
    align_fdr_to_dprst_grid,
    assert_raster_aligned,
    assign_vpu_drains,
    mask_fdr_to_vpu,
    read_aligned_uint8,
    read_land_mask,
    vpu_bbox,
    vpu_codes_present,
    vpu_pour_points,
    write_uint8_binary,
)
from .context import BuildContext

# ESRI-D8 flow codes plus the nodata/sink value (255). Any other value in the
# FDR is unexpected and is silently treated as a sink by the kernel — surface it.
_VALID_FDR_VALUES = frozenset({1, 2, 4, 8, 16, 32, 64, 128, 255})


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.fdr_raster is None:
        raise KeyError("routing step needs `fdr_raster` in fabric profile.")
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")
    dprst_path = ctx.require("dprst")
    onstream_path = ctx.require("onstream")
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
        align_fdr_to_dprst_grid(ctx.fdr_raster, dprst_path, fdr_aligned, logger)

        # read_aligned_uint8 asserts vpu_id is on the exact template grid;
        # a mismatch would silently mosaic into the wrong CONUS region.
        vpu_id = read_aligned_uint8(vpu_id_path, info)
        codes = vpu_codes_present(vpu_id)
        logger.info("  Tiling routing over %d VPU(s): %s", len(codes), codes)

        drains = np.full((info.height, info.width), np.uint8(255), dtype=np.uint8)

        with rasterio.open(fdr_aligned) as fdr_src, \
                rasterio.open(dprst_path) as dprst_src, \
                rasterio.open(onstream_path) as onstream_src:
            # dprst/onstream are windowed by vpu_id's grid; a same-shape but
            # differently-georeferenced raster would be read at the wrong origin
            # silently. Assert both are on the template grid (as carea_map does).
            assert_raster_aligned(dprst_src, info, "dprst")
            assert_raster_aligned(onstream_src, info, "onstream")
            for code in codes:
                bbox = vpu_bbox(vpu_id, code)
                r0, r1, c0, c1 = bbox
                window = Window(c0, r0, c1 - c0, r1 - r0)
                vpu_win = vpu_id[r0:r1, c0:c1]
                fdr_win = fdr_src.read(1, window=window)
                dprst_win = dprst_src.read(1, window=window)
                onstream_win = onstream_src.read(1, window=window)

                fdr_masked = mask_fdr_to_vpu(fdr_win, vpu_win, code, nodata=255)
                pour = vpu_pour_points(dprst_win, vpu_win, code)
                # On-stream waterbodies of THIS vpu are traversal barriers, so
                # land captured by an on-stream lake is not attributed to a
                # downstream dprst. vpu_pour_points is the generic mask∩VPU op.
                barrier = vpu_pour_points(onstream_win, vpu_win, code)

                unexpected = set(np.unique(fdr_masked).tolist()) - _VALID_FDR_VALUES
                if unexpected:
                    logger.warning(
                        "  VPU %d: FDR window has unexpected code(s) %s — treated "
                        "as sinks; check FDR encoding / nodata.", code, sorted(unexpected),
                    )

                # In-process D8 traversal (replaces WBT Watershed). Output is
                # 1 where the cell drains to a pour-point, else 0, so
                # assign_vpu_drains treats 0 as nodata. Barrier cells (on-stream
                # waterbodies) stop the traversal before it reaches a pour.
                ws_win, n_cycles = drains_to_dprst_kernel(
                    fdr_masked, pour, barrier, fdr_nodata=255
                )
                if n_cycles:
                    logger.warning(
                        "  VPU %d: %d flow cycle(s) in FDR — those cells marked "
                        "non-draining (hydro-conditioned-DEM defect).", code, n_cycles,
                    )
                assign_vpu_drains(drains, vpu_id, code, bbox, ws_win, ws_nodata=0)
                n_vpu = int((drains[r0:r1, c0:c1][vpu_win == code] == 1).sum())
                n_barrier = int((barrier == 1).sum())
                if n_barrier:
                    logger.info("  VPU %d: %d on-stream barrier cell(s)", code, n_barrier)
                if n_vpu == 0:
                    logger.warning(
                        "  VPU %d: 0 cells drain to dprst (%d on-stream barrier "
                        "cell(s)) — expected for a VPU with no depressions or where "
                        "barriers intercept every path, else check "
                        "dprst/vpu_id/onstream alignment.", code, n_barrier,
                    )
                else:
                    logger.info("  VPU %d: %d cells drain to dprst", code, n_vpu)
    finally:
        if not keep_intermediates and fdr_aligned.exists():
            fdr_aligned.unlink()

    del vpu_id  # free the CONUS uint8 partition before the final mask

    drains[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    n_in = int((drains == 1).sum())
    pct = 100 * n_in / drains.size
    if n_in == 0:
        raise RuntimeError(
            f"drains_to_dprst is all-nodata after routing {len(codes)} VPU(s) — "
            "an all-empty mask is never a valid product. Check that dprst, vpu_id, "
            "and the FDR are aligned to the same template grid."
        )
    if pct > 50:
        logger.warning(
            "Drains-to-dprst coverage is %.2f%% of the grid — unusually high. "
            "Check the pour-point mask (kernel background=0), FDR, and vpu_id "
            "alignment.", pct,
        )
    write_uint8_binary(drains, info, output_path)
    logger.info(
        "  Drains-to-dprst mask written: %s (%d cells, %.4f%% of grid)",
        output_path, n_in, pct,
    )
    return {"drains_to_dprst": output_path}

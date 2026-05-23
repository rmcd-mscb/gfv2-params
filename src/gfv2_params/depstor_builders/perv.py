"""Pervious-area binary raster: land AND NOT imperv AND NOT dprst."""

from __future__ import annotations

import numpy as np
import rasterio
from rasterio.windows import Window

from ..depstor import (
    RasterInfo,
    assert_raster_aligned,
    uint8_binary_profile,
)
from .context import BuildContext

STRIP_ROWS = 1024


def compute_perv_binary(
    imperv: np.ndarray, dprst: np.ndarray, land_valid: np.ndarray
) -> np.ndarray:
    """1 where land AND NOT impervious AND NOT depression-storage, else 255.

    `land_valid` is the HRU-fabric land mask. Required, not optional: this
    function defaults every cell to pervious, so omitting the mask would
    classify the whole ocean as pervious.
    """
    exclude = imperv == 1
    exclude |= dprst == 1
    exclude |= ~land_valid
    out = np.full_like(imperv, 1)
    out[exclude] = 255
    return out


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")
    imperv_path = ctx.require("imperv")
    dprst_path = ctx.require("dprst")

    logger.info("--- perv ---")
    logger.info("  Output: %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"perv": output_path}

    info = RasterInfo.from_path(ctx.template_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    n_perv = 0
    profile = uint8_binary_profile(info)

    with rasterio.open(landmask_path) as landmask_src, \
         rasterio.open(imperv_path) as imperv_src, \
         rasterio.open(dprst_path) as dprst_src, \
         rasterio.open(output_path, "w", **profile) as dst:
        assert_raster_aligned(landmask_src, info, "land_mask")
        assert_raster_aligned(imperv_src, info, "imperv")
        assert_raster_aligned(dprst_src, info, "dprst")

        for row_off in range(0, info.height, STRIP_ROWS):
            h = min(STRIP_ROWS, info.height - row_off)
            window = Window(0, row_off, info.width, h)
            land_valid = landmask_src.read(1, window=window) == 1
            imperv = imperv_src.read(1, window=window)
            dprst = dprst_src.read(1, window=window)
            perv = compute_perv_binary(imperv, dprst, land_valid)
            dst.write(perv, 1, window=window)
            n_perv += int((perv == 1).sum())

    total = info.height * info.width
    logger.info(
        "  %d cells marked pervious (%.4f%% of grid)",
        n_perv, 100 * n_perv / total,
    )
    return {"perv": output_path}

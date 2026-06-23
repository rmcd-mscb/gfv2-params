"""Combine wbody regions + connected-wbody mask + imperv into dprst + onstream."""

from __future__ import annotations

import numpy as np
import rasterio

from ..depstor import (
    RasterInfo,
    read_aligned_uint8,
    read_land_mask,
    regions_to_binary,
    regions_touching_mask,
    write_uint8_binary,
)
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    outputs = step_cfg["outputs"]
    dprst_path = ctx.resolve_output(outputs["dprst"])
    onstream_path = ctx.resolve_output(outputs["onstream"])

    landmask_path = ctx.require("landmask")
    wbody_binary_path = ctx.require("wbody_binary")
    wbody_regions_path = ctx.require("wbody_regions")
    connected_path = ctx.require("connected_wbody")
    imperv_path = ctx.require("imperv")

    logger.info("--- dprst ---")
    logger.info("  Dprst out    : %s", dprst_path)
    logger.info("  On-stream out: %s", onstream_path)

    if dprst_path.exists() and onstream_path.exists() and not ctx.force:
        logger.info("  Both outputs exist — skipping (pass --force to rebuild)")
        return {"dprst": dprst_path, "onstream": onstream_path}

    info = RasterInfo.from_path(ctx.template_path)
    wbody_binary = read_aligned_uint8(wbody_binary_path, info)
    connected_binary = read_aligned_uint8(connected_path, info)
    imperv_binary = read_aligned_uint8(imperv_path, info)
    with rasterio.open(wbody_regions_path) as src:
        regions = src.read(1)
    land_valid = read_land_mask(landmask_path)

    onstream_regions = regions_touching_mask(regions, connected_binary)
    imperv_regions = regions_touching_mask(regions, imperv_binary)
    excluded = onstream_regions | imperv_regions
    n_total = int(regions.max())
    logger.info(
        "  %d total wbody regions; %d touch connected wbody, %d touch imperv, %d excluded",
        n_total, len(onstream_regions), len(imperv_regions), len(excluded),
    )

    all_ids = set(int(v) for v in np.unique(regions) if v != 0)
    kept_ids = all_ids - excluded
    dprst_binary = regions_to_binary(regions, kept_ids)
    dprst_binary[~land_valid] = 255  # drop off-land (ocean) cells
    write_uint8_binary(dprst_binary, info, dprst_path)
    n_dprst = int((dprst_binary == 1).sum())
    logger.info(
        "  %d regions kept; %d cells in dprst (%.4f%% of grid)",
        len(kept_ids), n_dprst, 100 * n_dprst / dprst_binary.size,
    )

    onstream = np.where((wbody_binary == 1) & (dprst_binary != 1), np.uint8(1), np.uint8(255))
    onstream[~land_valid] = 255  # drop off-land (ocean) cells
    write_uint8_binary(onstream, info, onstream_path)
    n_on = int((onstream == 1).sum())
    logger.info(
        "  %d cells in on-stream storage (%.4f%% of grid)",
        n_on, 100 * n_on / onstream.size,
    )

    return {"dprst": dprst_path, "onstream": onstream_path}

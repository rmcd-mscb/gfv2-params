"""Classify waterbody regions into dprst + onstream.

A waterbody region is depression storage unless it is on-stream (touches the
NHD-connected mask). Impervious is NOT a region-level exclusion: a single
impervious cell must not remove a whole clump. Impervious cells are carved out
of dprst per-cell so imperv/dprst/perv stay a disjoint partition (no cell is
counted as both impervious and depression storage).

The region-level on-stream exclusion is a HEURISTIC PROXY for connectivity:
`clump_regions` merges any 8-connected waterbody cells into one region, and
`regions_touching_mask` excludes the WHOLE region if any one cell touches the
on-stream mask. That proxy fails when an endorheic waterbody is physically
adjacent to a genuinely on-stream one -- e.g. the Great Salt Lake (4,369 km2,
demoted to dprst by the `endorheic` classifier) is 8-connected to a 49.1 km2
SwampMarsh (COMID 10273192) whose water flows INTO the lake and is correctly
left on-stream. Without correction, that one marsh's on-stream status vetoes
the entire merged region, excluding all 4,854,156 Great Salt Lake cells from
depression storage. The `endorheic_wbody` mask (see `wbody_connectivity.py`)
is DIRECT hydrologic evidence -- "this waterbody's own water terminates inside
itself" -- rather than a proxy, so it exempts a waterbody's own (not-on-stream)
cells from the region-level exclusion. Evidence overrides proxy, but only
where we have evidence: a waterbody with no endorheic evidence keeps today's
clump behaviour exactly, so this cannot re-open the `drains_to_dprst`
over-extension that #145/#158/#161 fixed.

The exemption is also gated on `wbody_binary == 1` to preserve the
`dprst ⊆ wbody_binary` invariant: `endorheic_wbody` is rasterized from a raw,
unfiltered read of the waterbody gpkg (see `wbody_connectivity.py`), so
without this gate it would reinstate cells `waterbody.build()` deliberately
removed -- e.g. Ice Mass polygons (excluded from the waterbody classification
entirely) or sub-`min_area_threshold` slivers.
"""

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
    excluded = onstream_regions
    # Impervious is carved per-cell (below), NOT used to exclude whole regions:
    # a single impervious pixel must not drop an entire waterbody clump from
    # depression storage. regions_touching_mask is kept only for logging.
    imperv_regions = regions_touching_mask(regions, imperv_binary)
    n_total = int(regions.max())
    logger.info(
        "  %d total wbody regions; %d touch connected wbody (excluded), "
        "%d touch imperv (kept; cells carved per-cell)",
        n_total, len(onstream_regions), len(imperv_regions),
    )

    all_ids = set(int(v) for v in np.unique(regions) if v != 0)
    kept_ids = all_ids - excluded
    dprst_binary = regions_to_binary(regions, kept_ids)

    # A waterbody with DIRECT hydrologic evidence that its water terminates inside
    # itself is depression storage even if its 8-connected clump also contains a
    # feature that merely DRAINS INTO it. Without this, a 49 km2 inflow marsh
    # (COMID 10273192) vetoes all 4,369 km2 of the Great Salt Lake, because
    # clump_regions merges them and regions_touching_mask excludes the whole region.
    # Only cells that are themselves on-stream stay carved out.
    #
    # `endorheic_wbody` is OPTIONAL: absent (a fabric that has not run `endorheic`)
    # means no exemption is possible and this is a pure no-op, matching today's
    # behaviour exactly.
    if "endorheic_wbody" in ctx.paths:
        endorheic_binary = read_aligned_uint8(ctx.require("endorheic_wbody"), info)
        # `endorheic_wbody` is rasterized in `wbody_connectivity` from a fresh,
        # unfiltered read of the waterbody gpkg -- no EXCLUDE_WATERBODY_FTYPES
        # (Ice Mass) filter and no min_area_threshold, unlike `wbody_binary`
        # (built in `waterbody.build()`, which applies both). Gate the
        # exemption on `wbody_binary == 1` so it can only ever recover a cell
        # `waterbody` itself already treats as a waterbody -- this keeps
        # `dprst ⊆ wbody_binary` intact. Measured on real CONUS data: 2 of
        # 22,942 flagged endorheic COMIDs are Ice Mass (COMIDs 8265726/8265734,
        # the Mt Shasta glaciers, flagged via Signal B's HUC12 test) and would
        # otherwise be silently reinstated as dprst -- a glacier is not
        # depression storage.
        exempt = endorheic_binary == 1
        exempt &= connected_binary != 1
        exempt &= wbody_binary == 1
        n_exempted = int((exempt & (dprst_binary != 1)).sum())
        dprst_binary[exempt] = 1
        logger.info(
            "  endorheic exemption: %d cells recovered into dprst (region-level "
            "on-stream exclusion overridden by direct evidence the waterbody's "
            "own water terminates inside itself)", n_exempted,
        )
        # Both are ~16.9 GB at CONUS and unused past this point -- free them
        # promptly rather than holding to the end of build(), which pays for
        # the extra `wbody_binary` term in the exemption above.
        del exempt, endorheic_binary
    else:
        logger.info(
            "  endorheic exemption: `endorheic_wbody` not in build context — "
            "no exemption applied, today's clump behaviour unchanged"
        )

    n_carved = int(((dprst_binary == 1) & (imperv_binary == 1)).sum())
    dprst_binary[imperv_binary == 1] = 255  # carve impervious cells (no imperv/dprst double-count)
    dprst_binary[~land_valid] = 255  # drop off-land (ocean) cells
    write_uint8_binary(dprst_binary, info, dprst_path)
    n_dprst = int((dprst_binary == 1).sum())
    logger.info(
        "  %d regions kept; %d impervious cells carved; %d cells in dprst (%.4f%% of grid)",
        len(kept_ids), n_carved, n_dprst, 100 * n_dprst / dprst_binary.size,
    )

    onstream = np.where(
        (wbody_binary == 1) & (dprst_binary != 1) & (imperv_binary != 1),
        np.uint8(1), np.uint8(255),
    )
    onstream[~land_valid] = 255  # drop off-land (ocean) cells
    write_uint8_binary(onstream, info, onstream_path)
    n_on = int((onstream == 1).sum())
    logger.info(
        "  %d cells in on-stream storage (%.4f%% of grid)",
        n_on, 100 * n_on / onstream.size,
    )

    return {"dprst": dprst_path, "onstream": onstream_path}

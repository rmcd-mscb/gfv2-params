"""Rasterise the NHD-connected waterbody polygons to a uint8 binary mask.

Connectivity comes from NHD's WBAREACOMI artificial-path topology (staged by
gfv2_params.download.nhd_flowlines into a connected-COMID parquet), joined to the
waterbody polygons by COMID / member_comid. Replaces the old streambuffer mask as
the on-stream signal consumed by the dprst step.

Also rasterises a SECOND mask, `endorheic_wbody.tif`: every waterbody the
`endorheic` classifier flagged (Signal A and/or B), regardless of whether it is
on-stream. This is direct hydrologic evidence ("this waterbody's water terminates
inside itself"), not a proxy — `dprst.py` uses it to exempt an endorheic
waterbody from the region-level on-stream exclusion when `clump_regions`' 8-connected
labelling has merged it with an on-stream neighbour (e.g. the Great Salt Lake's
49 km2 inflow marsh, COMID 10273192). See `dprst.py` for the exemption itself.
"""

from __future__ import annotations

import geopandas as gpd

from ..depstor import (
    RasterInfo,
    load_connected_comids,
    rasterize_binary,
    read_land_mask,
    select_connected_waterbodies,
    write_uint8_binary,
)
from ..endorheic import load_endorheic_comids
from ..nhd_ftypes import NEVER_ONSTREAM_FTYPES
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError(
            "wbody_connectivity step needs `waterbody_gpkg` and `waterbody_layer`."
        )
    if ctx.connected_comids_table is None:
        raise KeyError(
            "wbody_connectivity step needs `connected_comids_table` in the fabric "
            "profile. Stage it first: "
            "`python -m gfv2_params.download.nhd_flowlines`."
        )
    outputs = step_cfg["outputs"]
    connected_path = ctx.resolve_output(outputs["connected_wbody"])
    endorheic_path = ctx.resolve_output(outputs["endorheic_wbody"])
    landmask_path = ctx.require("landmask")

    logger.info("--- wbody_connectivity ---")
    logger.info("  Waterbody gpkg : %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  Connected table: %s", ctx.connected_comids_table)
    if ctx.flowthrough_comids_table is not None:
        logger.info("  Flow-through table: %s", ctx.flowthrough_comids_table)
    logger.info("  Output (connected): %s", connected_path)
    logger.info("  Output (endorheic): %s", endorheic_path)

    if connected_path.exists() and endorheic_path.exists() and not ctx.force:
        logger.info("  Both outputs exist — skipping (pass --force to rebuild)")
        return {"connected_wbody": connected_path, "endorheic_wbody": endorheic_path}

    if not ctx.connected_comids_table.exists():
        raise FileNotFoundError(
            f"Connected-COMID table not found: {ctx.connected_comids_table}. "
            f"Run `python -m gfv2_params.download.nhd_flowlines` first."
        )

    info = RasterInfo.from_path(ctx.template_path)
    connected = load_connected_comids(ctx.connected_comids_table)
    n_wbareacomi = len(connected)
    n_flowthrough = 0
    if ctx.flowthrough_comids_table is not None:
        if not ctx.flowthrough_comids_table.exists():
            raise FileNotFoundError(
                f"Flow-through COMID table not found: "
                f"{ctx.flowthrough_comids_table}. Run "
                f"`python -m gfv2_params.download.nhd_flowthrough` first, or "
                f"remove `flowthrough_comids_table` from the profile."
            )
        flowthrough = load_connected_comids(ctx.flowthrough_comids_table)
        if not flowthrough:
            raise ValueError(
                "configured flow-through table is empty → it would promote no "
                "waterbodies and silently degrade to WBAREACOMI-only; re-run "
                "nhd_flowthrough or remove the key"
            )
        n_flowthrough = len(flowthrough - connected)
        connected = connected | flowthrough

    # Endorheic demotion (see gfv2_params.endorheic). A STRICT SUBTRACTION: these
    # signals can only remove COMIDs from the on-stream set, never add one — so the
    # on-stream mask can never be inflated by them. This is what finally takes the
    # Great Salt Lake off-stream: both local classifiers promote it, because NHD
    # draws Network artificial paths between its arms.
    #
    # A MISSING `endorheic_comids` (the `endorheic` step never ran) raises below —
    # every fabric that can reach this line has both a COMID-keyed waterbody layer
    # and `fdr_raster` (required on every depstor fabric), so it can always run that
    # step first. An EMPTY endorheic table is different and IS a legitimate no-op: a
    # domain with no closed basin (e.g. tjc, Texas-Gulf) has no endorheic waterbody.
    # The guard against a *silently* empty table on a fabric that should have
    # demotions is the `min_endorheic_comids` floor in the `endorheic` builder, not
    # a raise here.
    #
    # NOTE: `select_connected_waterbodies` promotes a waterbody on COMID **or**
    # `member_comid`, but this subtraction removes COMIDs only (the endorheic table is
    # COMID-keyed — `endorheic_frame` groups by COMID). Verified inert on the real
    # layer today: COMID and member_comid are equal on every numeric row, and 0 rows
    # are on-stream via `member_comid` alone. If those keys ever diverge, a waterbody
    # could be demoted by COMID here and then re-promoted through its `member_comid`
    # below, silently disabling the demotion — subtract on both keys if that happens.
    if "endorheic_comids" not in ctx.paths:
        raise KeyError(
            "wbody_connectivity step needs `endorheic_comids` in the build context, "
            "but the `endorheic` step has not run and produced no output on disk for "
            "this fabric. Without it, terminal/closed-basin lakes — including the "
            "Great Salt Lake — would remain classified ON-STREAM. Run the `endorheic` "
            "step first (e.g. `--from endorheic`), or run the full DAG so it runs "
            "in order. An EMPTY endorheic table (a domain with no closed basin, e.g. "
            "`tjc`) is a legitimate no-op and does NOT hit this branch — this only "
            "fires when the step's output is missing entirely."
        )
    endorheic = load_endorheic_comids(ctx.require("endorheic_comids"))
    n_endorheic = len(connected & endorheic)
    connected = connected - endorheic
    logger.info(
        "  endorheic demotion: %d of %d endorheic COMIDs were on-stream → dprst",
        n_endorheic, len(endorheic),
    )
    if not endorheic:
        logger.info(
            "  (the endorheic table is empty — expected only for a domain with no "
            "closed basin; the demotion is a no-op)"
        )

    logger.info(
        "  on-stream COMIDs: %d WBAREACOMI + %d new flow-through - %d endorheic "
        "= %d total",
        n_wbareacomi, n_flowthrough, n_endorheic, len(connected),
    )
    # NOTE: this re-reads the raw waterbody_gpkg from disk, NOT the merged frame
    # `waterbody.build()` produces (which unions in BurnAddWaterbody rows). So
    # BurnAdd rows are never present here, and the NEVER_ONSTREAM_FTYPES filter
    # below never evaluates against them -- not "checked and passed", genuinely
    # invisible to it. That's fine: BurnAdd rows' negative COMID can never match
    # a connected/flow-through COMID (see waterbody.merge_burn_add), so they can't
    # reach this on-stream set regardless.
    try:
        wb_gdf = gpd.read_file(ctx.waterbody_gpkg, layer=ctx.waterbody_layer, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        wb_gdf = gpd.read_file(ctx.waterbody_gpkg, layer=ctx.waterbody_layer)

    if wb_gdf.crs != info.crs:
        logger.info("  Reprojecting wbodies from %s to %s", wb_gdf.crs, info.crs)
        wb_gdf = wb_gdf.to_crs(info.crs)
    wb_gdf = wb_gdf[wb_gdf.geometry.notna() & ~wb_gdf.geometry.is_empty]

    sel = select_connected_waterbodies(wb_gdf, connected)
    if "FTYPE" in sel.columns:
        n_before = len(sel)
        sel = sel[~sel["FTYPE"].isin(NEVER_ONSTREAM_FTYPES)].copy()
        n_forced = n_before - len(sel)
        if n_forced:
            logger.info(
                "  never-on-stream guardrail: dropped %d Playa (force-dprst) / "
                "Ice Mass (excluded) waterbodies promoted via WBAREACOMI", n_forced,
            )
    else:
        raise KeyError(
            "waterbody layer has no FTYPE column — never-on-stream guardrail "
            "(Playa/Ice Mass) cannot be applied; refusing to write a raster "
            "that would misclassify Playa/Ice Mass waterbodies promoted via "
            "WBAREACOMI as on-stream. A genuinely FTYPE-less waterbody layer "
            "is an upstream data problem (check the source gpkg), not "
            "something this pipeline should paper over."
        )
    logger.info(
        "  %d connected COMIDs; %d of %d waterbody polygons flagged connected",
        len(connected), len(sel), len(wb_gdf),
    )
    if len(sel) == 0:
        # An empty connected mask makes dprst classify every waterbody as
        # depression storage, silently inflating dprst_frac domain-wide. Fail
        # loud instead of writing an all-nodata raster the orchestrator accepts.
        raise ValueError(
            f"wbody_connectivity matched 0 of {len(wb_gdf)} waterbodies against "
            f"{len(connected)} connected COMIDs — this would misclassify every "
            f"waterbody as depression storage. Check that "
            f"{ctx.connected_comids_table} is complete and that the "
            f"COMID/member_comid join keys align with the waterbody layer."
        )

    # Read the land mask once and reuse for both rasters below -- each call
    # allocates a 16.9 GB bool array plus a 16.9 GB `~` temporary at CONUS
    # scale, so reading it twice doubles that cost for no reason.
    land = read_land_mask(landmask_path)

    binary = rasterize_binary(sel, info, all_touched=False)
    binary[~land] = 255  # drop off-land (ocean) cells
    write_uint8_binary(binary, info, connected_path)
    n_in = int((binary == 1).sum())
    logger.info("  %d connected-waterbody cells after land mask", n_in)

    # Endorheic raster: positive hydrologic evidence, independent of on-stream
    # status. Rasterize the FULL endorheic set (not just the ones that were
    # on-stream before the subtraction above) -- "this waterbody's water
    # terminates inside itself" applies regardless of whether it happened to
    # also be on-stream. `dprst.py` uses this to exempt an endorheic waterbody
    # from the region-level on-stream exclusion when clump_regions has merged
    # it with an on-stream neighbour.
    sel_endorheic = select_connected_waterbodies(wb_gdf, endorheic)
    logger.info(
        "  %d endorheic COMIDs; %d of %d waterbody polygons flagged endorheic",
        len(endorheic), len(sel_endorheic), len(wb_gdf),
    )
    endorheic_binary = rasterize_binary(sel_endorheic, info, all_touched=False)
    endorheic_binary[~land] = 255  # drop off-land (ocean) cells
    write_uint8_binary(endorheic_binary, info, endorheic_path)
    n_endorheic_cells = int((endorheic_binary == 1).sum())
    logger.info("  %d endorheic-waterbody cells after land mask", n_endorheic_cells)

    return {"connected_wbody": connected_path, "endorheic_wbody": endorheic_path}

"""Rasterise the NHD-connected waterbody polygons to a uint8 binary mask.

Connectivity comes from NHD's WBAREACOMI artificial-path topology (staged by
gfv2_params.download.nhd_flowlines into a connected-COMID parquet), joined to the
waterbody polygons by COMID / member_comid. Replaces the old streambuffer mask as
the on-stream signal consumed by the dprst step.
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
    output_path = ctx.resolve_output(step_cfg["output"])
    landmask_path = ctx.require("landmask")

    logger.info("--- wbody_connectivity ---")
    logger.info("  Waterbody gpkg : %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  Connected table: %s", ctx.connected_comids_table)
    if ctx.flowthrough_comids_table is not None:
        logger.info("  Flow-through table: %s", ctx.flowthrough_comids_table)
    logger.info("  Output         : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output already exists — skipping (pass --force to rebuild)")
        return {"connected_wbody": output_path}

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
    n_endorheic = 0
    endorheic_applied = False
    if "endorheic_comids" in ctx.paths:
        endorheic = load_endorheic_comids(ctx.require("endorheic_comids"))
        n_endorheic = len(connected & endorheic)
        connected = connected - endorheic
        endorheic_applied = True
        logger.info(
            "  endorheic demotion: %d of %d endorheic COMIDs were on-stream → dprst",
            n_endorheic, len(endorheic),
        )
    else:
        logger.warning(
            "  ENDORHEIC DEMOTION NOT APPLIED: `endorheic_comids` is not present in "
            "the build context (the `endorheic` step has not run and produced no "
            "output on disk for this fabric). Terminal/closed-basin lakes — "
            "including the Great Salt Lake — will remain classified ON-STREAM. "
            "Run the `endorheic` step first (e.g. `--from endorheic`) if this "
            "fabric supports it; if it genuinely can't (e.g. no COMID column), "
            "this is expected and safe to ignore."
        )

    if endorheic_applied:
        logger.info(
            "  on-stream COMIDs: %d WBAREACOMI + %d new flow-through - %d endorheic "
            "= %d total",
            n_wbareacomi, n_flowthrough, n_endorheic, len(connected),
        )
    else:
        logger.info(
            "  on-stream COMIDs: %d WBAREACOMI + %d new flow-through "
            "(endorheic demotion NOT APPLIED — see warning above) = %d total",
            n_wbareacomi, n_flowthrough, len(connected),
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

    binary = rasterize_binary(sel, info, all_touched=False)
    binary[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    write_uint8_binary(binary, info, output_path)
    n_in = int((binary == 1).sum())
    logger.info("  %d connected-waterbody cells after land mask", n_in)

    return {"connected_wbody": output_path}

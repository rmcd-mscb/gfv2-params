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
from ..download.nhd_flowthrough import NEVER_ONSTREAM_FTYPES
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
    logger.info(
        "  on-stream COMIDs: %d WBAREACOMI + %d new flow-through = %d total",
        n_wbareacomi, n_flowthrough, len(connected),
    )
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
                "  force-dprst guardrail: dropped %d Playa/Ice Mass waterbodies "
                "promoted via WBAREACOMI", n_forced,
            )
    else:
        logger.warning(
            "  waterbody layer has no FTYPE column — force-dprst guardrail "
            "(Playa/Ice Mass) cannot be applied"
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

"""Emit the endorheic-waterbody COMID table consumed by `wbody_connectivity`.

Runs BEFORE wbody_connectivity. Signal A needs only `fdr_raster` (already a required
profile key on every fabric), so it works everywhere with no new configuration.
Signal B is optional and activates when `wbd_huc12_table` is present.
"""

from __future__ import annotations

import geopandas as gpd

from ..endorheic import endorheic_frame, write_endorheic_comids
from .context import BuildContext


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError("endorheic step needs `waterbody_gpkg` and `waterbody_layer`.")
    if ctx.fdr_raster is None:
        raise KeyError("endorheic step needs `fdr_raster` in the fabric profile.")
    output_path = ctx.resolve_output(step_cfg["output"])

    logger.info("--- endorheic ---")
    logger.info("  FDR       : %s", ctx.fdr_raster)
    logger.info("  Waterbody : %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  WBD closed: %s", ctx.wbd_huc12_table or "(not configured — Signal B off)")
    logger.info("  Output    : %s", output_path)

    if output_path.exists() and not ctx.force:
        logger.info("  Output exists — skipping (pass --force to rebuild)")
        return {"endorheic_comids": output_path}
    if not ctx.fdr_raster.exists():
        raise FileNotFoundError(f"FDR raster not found: {ctx.fdr_raster}")

    wb = gpd.read_file(ctx.waterbody_gpkg, layer=ctx.waterbody_layer, use_arrow=True)
    if "COMID" not in wb.columns:
        raise KeyError(
            "waterbody layer has no COMID column — the endorheic classifier emits a "
            "COMID table and cannot run. Use a fabric whose waterbody layer carries "
            "COMID (e.g. `gfv2`), not the NHM_01_draft `wbs` layer."
        )
    wb = wb[wb.geometry.notna() & ~wb.geometry.is_empty]

    closed = None
    if ctx.wbd_huc12_table is not None:
        if not ctx.wbd_huc12_table.exists():
            raise FileNotFoundError(
                f"WBD HUC12 table not found: {ctx.wbd_huc12_table}. Run "
                f"`python -m gfv2_params.download.wbd_huc12` first, or remove "
                f"`wbd_huc12_table` from the profile."
            )
        closed = gpd.read_parquet(ctx.wbd_huc12_table)

    df = endorheic_frame(wb, ctx.fdr_raster, closed_gdf=closed, logger=logger)
    if df.empty:
        raise ValueError(
            "endorheic classifier flagged 0 waterbodies — that is a silent no-op "
            "(Great Salt Lake would stay on-stream). Check that fdr_raster has "
            "code-0 cells and that the waterbody layer overlaps it."
        )
    write_endorheic_comids(df, output_path)
    logger.info(
        "  %d endorheic COMIDs (%d by terminus, %d by closed basin)",
        len(df), int(df.by_terminus.sum()), int(df.by_closed_huc12.sum()),
    )
    return {"endorheic_comids": output_path}

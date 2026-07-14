"""Emit the endorheic-waterbody COMID table consumed by `wbody_connectivity`.

Runs BEFORE wbody_connectivity. Signal A needs only `fdr_raster` (already a required
profile key on every fabric), so it works everywhere with no new configuration.
Signal B is optional and activates when `wbd_huc12_table` is present.

Empty is a LEGITIMATE result. A domain with no closed basin has no endorheic
waterbody: `tjc` (Texas-Gulf) has 4 FDR code-0 cells and 0 endorheic waterbodies,
against 15,262 / thousands on `gfv2` and 1,438 / 680 on `oregon`. This step lives in
the fabric-independent `configs/depstor/depstor_rasters.yml`, so raising on a zero-row
result would brick the whole `tjc` depstor DAG with a message about the Great Salt
Lake. Instead, breakage is gated on things that actually indicate breakage:

  * the waterbody layer not overlapping the FDR grid at all (a CRS/extent bug that
    would silently zero Signal A anywhere), and
  * `min_endorheic_comids` — an OPTIONAL per-fabric floor in the profile. `gfv2` sets
    it, so a silently-empty (or collapsed) CONUS result is still impossible to miss;
    a fabric that legitimately has none simply omits the key.
"""

from __future__ import annotations

import geopandas as gpd
import pandas as pd
import rasterio
from shapely.geometry import box

from ..endorheic import endorheic_frame, write_endorheic_comids
from .context import BuildContext


def _check_floor(n: int, ctx: BuildContext, output_path) -> None:
    """Raise if the fabric declares a `min_endorheic_comids` floor and `n` is under it.

    This is the real protection the old "raise on 0 rows" guard was reaching for, but
    scoped to the fabrics that can actually assert an expectation. On `gfv2` a
    collapsed or silently-empty classifier result can never slip through unnoticed:
    the demotion would be a no-op and the Great Salt Lake would stay on-stream.
    """
    floor = ctx.min_endorheic_comids
    if floor is not None and n < floor:
        raise ValueError(
            f"endorheic classifier produced {n} COMIDs for fabric '{ctx.fabric}', "
            f"below its declared `min_endorheic_comids` floor of {floor} "
            f"({output_path}). That is a collapsed or no-op demotion — the Great Salt "
            f"Lake would stay on-stream. Check that fdr_raster has code-0 cells, that "
            f"the waterbody layer overlaps it, and that wbd_huc12_table is staged. "
            f"Lower/remove the floor in the fabric profile ONLY if this domain "
            f"genuinely has no closed basin."
        )


def _assert_overlaps_fdr(wb: gpd.GeoDataFrame, fdr_path, logger) -> None:
    """Fail loud if the waterbody layer and the FDR grid don't overlap at all.

    A disjoint extent (wrong fabric wiring, a CRS collapse upstream) would make BOTH
    signals silently return nothing, which is indistinguishable from a domain that
    legitimately has no endorheic waterbody. This is the check that lets the empty
    result be tolerated everywhere else.
    """
    with rasterio.open(fdr_path) as src:
        fdr_box = gpd.GeoSeries([box(*src.bounds)], crs=src.crs)
    if wb.crs is not None and fdr_box.crs != wb.crs:
        fdr_box = fdr_box.to_crs(wb.crs)
    wb_box = box(*wb.total_bounds)
    if not wb_box.intersects(fdr_box.iloc[0]):
        raise ValueError(
            f"the waterbody layer's extent {wb.total_bounds.tolist()} does not "
            f"overlap the FDR grid {fdr_box.total_bounds.tolist()} (in the waterbody "
            f"CRS) — the endorheic classifier would flag nothing, silently. This is a "
            f"wiring/CRS bug, not a domain with no closed basin."
        )
    logger.info("  waterbody layer overlaps the FDR grid")


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
        # Still honour the fabric's floor: a stale/empty table left on disk would
        # otherwise sail through the skip path and silently disable the demotion.
        # The table may carry unflagged evaluated candidates (see endorheic_frame),
        # so the floor must count only the FLAGGED (actually-demoted) rows, not
        # every row on disk.
        existing = pd.read_parquet(
            output_path, columns=["comid", "by_terminus", "by_closed_huc12"]
        )
        n_existing_flagged = int(
            (existing["by_terminus"] | existing["by_closed_huc12"]).sum()
        )
        _check_floor(n_existing_flagged, ctx, output_path)
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
    if wb.empty:
        raise ValueError(
            f"every geometry in {ctx.waterbody_gpkg} (layer={ctx.waterbody_layer}) is "
            f"null/empty — the classifier would flag nothing, silently."
        )
    _assert_overlaps_fdr(wb, ctx.fdr_raster, logger)

    closed = None
    if ctx.wbd_huc12_table is not None:
        if not ctx.wbd_huc12_table.exists():
            raise FileNotFoundError(
                f"WBD HUC12 table not found: {ctx.wbd_huc12_table}. Run "
                f"`python -m gfv2_params.download.wbd_huc12` first, or remove "
                f"`wbd_huc12_table` from the profile."
            )
        closed = gpd.read_parquet(ctx.wbd_huc12_table)
        # Signal B is "majority-inside a CLOSED (type-C) HUC12". `download/wbd_huc12`
        # already stages only type-C rows, but this step must not depend on that: if
        # `wbd_huc12_table` is ever pointed at a genuine FULL WBD layer, every
        # waterbody would be majority-inside the "closed" union, every COMID would be
        # flagged endorheic, and the on-stream set would empty. The filter is cheap;
        # the failure mode is catastrophic and silent.
        if "HU_12_TYPE" not in closed.columns:
            raise KeyError(
                f"{ctx.wbd_huc12_table} has no HU_12_TYPE column — the endorheic "
                f"builder cannot verify that these are CLOSED (type-C) HUC12s, and "
                f"treating contributing HUC12s as closed would flag every waterbody "
                f"endorheic and empty the on-stream set. Re-stage with "
                f"`python -m gfv2_params.download.wbd_huc12`."
            )
        n_all = len(closed)
        closed = closed[closed["HU_12_TYPE"] == "C"]
        logger.info(
            "  WBD: %d of %d HUC12s are type-C (closed basin)", len(closed), n_all
        )

    df = endorheic_frame(wb, ctx.fdr_raster, closed_gdf=closed, logger=logger)
    # `df` now carries every Signal-A-EVALUATED candidate (flagged or not), so the
    # threshold sweep in scripts/diagnose/endorheic_fixtures.py can measure the real
    # frac_own distribution -- NOT just the demotions. `len(df)` is therefore the
    # wrong count for both the floor and the "is this domain endorheic at all?"
    # check below; both must count only the FLAGGED (actually-demoted) rows.
    n_flagged = int((df["by_terminus"] | df["by_closed_huc12"]).sum())
    _check_floor(n_flagged, ctx, output_path)
    if n_flagged == 0:
        # A domain with no closed basin (e.g. `tjc`, Texas-Gulf). Legitimate: write the
        # table (possibly with unflagged evaluated candidates, but zero demotions) so
        # `wbody_connectivity` applies an empty — and therefore no-op — subtraction,
        # and carry on. The `min_endorheic_comids` floor above is what keeps this from
        # being a silent regression on a fabric that expects demotions.
        logger.warning(
            "  endorheic classifier flagged 0 waterbodies — no waterbody's D8 terminus "
            "lies inside itself and none is majority-inside a closed HUC12. Expected "
            "for a domain with no closed basin; the demotion will be a no-op."
        )
    write_endorheic_comids(df, output_path)
    logger.info(
        "  %d endorheic COMIDs (%d by terminus, %d by closed basin) out of %d "
        "evaluated candidates persisted",
        n_flagged, int(df.by_terminus.sum()), int(df.by_closed_huc12.sum()), len(df),
    )
    return {"endorheic_comids": output_path}

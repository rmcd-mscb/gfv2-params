"""Rasterise waterbody polygons + scipy connected-component labels."""

from __future__ import annotations

import math
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..depstor import (
    RasterInfo,
    clump_regions,
    rasterize_binary,
    read_land_mask,
    write_int32_regions,
    write_uint8_binary,
)
from ..nhd_ftypes import EXCLUDE_WATERBODY_FTYPES
from .context import BuildContext


def merge_burn_add(
    wb_gdf: gpd.GeoDataFrame,
    burn_gdf: gpd.GeoDataFrame | None,
    cell_size: float = 30.0,
) -> gpd.GeoDataFrame:
    """Union NHDPlus BurnAddWaterbody polygons into the waterbody layer.

    These are closed lakes / playas NHDPlus added for the DEM burn that are absent
    from NHDWaterbody — genuinely new depression AREA (0 of 23 overlap an existing
    waterbody in VPU 16). Once they are waterbody polygons they flow through
    waterbody -> dprst -> routing untouched and become dprst pour-points, which is
    why `routing` needs no change.

    Their COMID (NHDPlus `PolyID`) is NEGATIVE, so it can never match a WBAREACOMI or
    flow-through COMID (all positive) — that is what makes them structurally incapable
    of on-stream promotion. Asserted here rather than left to luck.

    On `NEVER_ONSTREAM_FTYPES` (Playa/Ice Mass): `wbody_connectivity` and
    `nhd_flowthrough` both apply that guardrail against a SEPARATE, fresh re-read of
    the raw `waterbody_gpkg` — never against the merged frame this function returns.
    A BurnAdd row's negative COMID is simply never present in that re-read, so
    `NEVER_ONSTREAM_FTYPES` is never evaluated against it — it neither passes nor
    fails that check, it is invisible to it. Do NOT describe BurnAdd rows as
    "subject to" or "checked against" `NEVER_ONSTREAM_FTYPES`. Safety instead comes
    structurally from the two asserts in this function: the negative-COMID guard
    (can't match a connected/flow-through COMID to be promoted on-stream in the
    first place) and the overlap guard below (can't be clump-merged into an
    on-stream region by `clump_regions`). `EXCLUDE_WATERBODY_FTYPES` (Ice Mass) is
    different and DOES apply to BurnAdd rows: `waterbody.build()` runs it on the
    merged frame this function returns, which is why this merge must happen before
    that filter, not after.

    `cell_size` (the template raster's pixel size, in the same linear units as
    `wb_gdf`'s CRS) drives the overlap guard below: it must match what
    `clump_regions` will actually rasterize against, so pass the real template
    cell size (`RasterInfo.from_path(ctx.template_path)`), not the default.
    """
    if burn_gdf is None or len(burn_gdf) == 0:
        return wb_gdf
    if (burn_gdf["COMID"] >= 0).any():
        raise ValueError(
            "BurnAddWaterbody COMID must be negative (NHDPlus PolyID). A non-negative "
            "value could match a positive WBAREACOMI/flow-through COMID and be promoted "
            "on-stream — but NHDPlus flagged every BurnAddWaterbody as a sink."
        )
    burn = burn_gdf.to_crs(wb_gdf.crs) if burn_gdf.crs != wb_gdf.crs else burn_gdf

    # A BurnAdd polygon overlapping an existing waterbody would be MERGED with it by
    # clump_regions (8-connected labelling). If that waterbody is on-stream,
    # regions_touching_mask then excludes the whole clump — silently DELETING the
    # BurnAdd playa's depression area, the opposite of why we staged it, and invisible
    # in the logs. VPU 16 measured 0 of 23 overlapping; CONUS-wide is unverified, so
    # this fails loud rather than corrupting the product quietly.
    #
    # This must test what clump_regions actually does to the RASTERIZED cells, not
    # vector intersection: 8-connectivity merges cells whose centres are up to
    # `cell_size * sqrt(2)` apart (a diagonal neighbour), so two polygons that do NOT
    # intersect in vector space can still land in adjacent, 8-connected cells and be
    # clump-merged anyway. Buffering the BurnAdd geometry by one cell diagonal before
    # the join is a conservative over-approximation of that adjacency test — it can
    # flag a near-miss that wouldn't actually rasterize into the same clump, but it
    # can never miss a real one. Fail loud on the near-miss; never miss the real thing.
    buffer_dist = cell_size * math.sqrt(2)
    buffered = burn[["COMID", "geometry"]].copy()
    buffered["geometry"] = buffered.geometry.buffer(buffer_dist)
    hits = gpd.sjoin(
        buffered, wb_gdf[["COMID", "geometry"]],
        how="inner", predicate="intersects",
    )
    if not hits.empty:
        bad = sorted(set(hits["COMID_left"]))[:10]
        raise ValueError(
            f"{hits['COMID_left'].nunique()} BurnAddWaterbody polygon(s) overlap or "
            f"lie within one rasterized cell diagonal ({buffer_dist:.1f} m) of an "
            f"existing waterbody (e.g. {bad}). clump_regions would merge them into one "
            f"8-connected region, so an on-stream neighbour would silently drag the "
            f"BurnAdd depression out of dprst. Investigate the overlap — do not "
            f"suppress this."
        )

    burn = burn[wb_gdf.columns].copy()
    # `member_comid` is a plain string in the real conus_waterbodies.gpkg, but
    # burn_add_to_waterbody_frame (Task 1) emits it as int64 (same value as COMID).
    # Left un-normalised, pd.concat below produces an `object` column with mixed
    # str/int rows. Nothing downstream reads it today -- select_connected_waterbodies
    # re-reads the raw gpkg, not this merged frame -- so it's inert, but it's a
    # fragile state a future `sorted()` or `.str` accessor over it would TypeError
    # on. Normalise to whatever dtype wb_gdf already uses before concatenating.
    if "member_comid" in wb_gdf.columns:
        target_dtype = wb_gdf["member_comid"].dtype
        # `pd.api.types.is_object_dtype` compares dtype identity correctly, unlike
        # `target_dtype is object` (always False for a pandas dtype instance — that
        # comparison never fires, so this branch was previously dead code). A plain
        # `.astype(object)` on an int64 column wraps the raw ints rather than
        # stringifying them, reproducing the exact mixed str/int column this
        # normalisation exists to prevent.
        if pd.api.types.is_object_dtype(target_dtype):
            burn["member_comid"] = burn["member_comid"].astype(str)
        else:
            burn["member_comid"] = burn["member_comid"].astype(target_dtype)

    return gpd.GeoDataFrame(
        pd.concat([wb_gdf, burn], ignore_index=True), crs=wb_gdf.crs
    )


def _load_waterbodies(path: Path, layer: str | None, logger):
    try:
        return gpd.read_file(path, layer=layer, use_arrow=True)
    except ImportError:
        logger.warning("PyArrow unavailable for vector load; falling back to fiona.")
        return gpd.read_file(path, layer=layer)


def build(step_cfg: dict, ctx: BuildContext, logger) -> dict:
    if ctx.waterbody_gpkg is None or ctx.waterbody_layer is None:
        raise KeyError(
            "waterbody step needs `waterbody_gpkg` and `waterbody_layer` in fabric profile."
        )
    outputs = step_cfg["outputs"]
    binary_path = ctx.resolve_output(outputs["binary"])
    regions_path = ctx.resolve_output(outputs["regions"])
    landmask_path = ctx.require("landmask")
    min_area = float(step_cfg.get("min_area_threshold", 900.0))

    if not ctx.waterbody_gpkg.exists():
        raise FileNotFoundError(f"Waterbody gpkg not found: {ctx.waterbody_gpkg}")

    logger.info("--- waterbody ---")
    logger.info("  Waterbody gpkg: %s (layer=%s)", ctx.waterbody_gpkg, ctx.waterbody_layer)
    logger.info("  Binary out    : %s", binary_path)
    logger.info("  Regions out   : %s", regions_path)
    logger.info("  Min area      : %.1f m^2", min_area)

    if binary_path.exists() and regions_path.exists() and not ctx.force:
        logger.info("  Both outputs exist — skipping (pass --force to rebuild)")
        return {"wbody_binary": binary_path, "wbody_regions": regions_path}

    info = RasterInfo.from_path(ctx.template_path)
    wb_gdf = _load_waterbodies(ctx.waterbody_gpkg, ctx.waterbody_layer, logger)
    if wb_gdf.crs != info.crs:
        logger.info("  Reprojecting wbodies from %s to %s", wb_gdf.crs, info.crs)
        wb_gdf = wb_gdf.to_crs(info.crs)
    wb_gdf = wb_gdf[wb_gdf.geometry.notna() & ~wb_gdf.geometry.is_empty]

    if ctx.burn_add_waterbody_table is not None:
        if not ctx.burn_add_waterbody_table.exists():
            raise FileNotFoundError(
                f"BurnAddWaterbody table not found: {ctx.burn_add_waterbody_table}. "
                f"Run `python -m gfv2_params.download.nhd_burn_components` first, or "
                f"remove `burn_add_waterbody_table` from the profile."
            )
        burn = gpd.read_parquet(ctx.burn_add_waterbody_table)
        n_before = len(wb_gdf)
        wb_gdf = merge_burn_add(wb_gdf, burn, cell_size=abs(info.transform.a))
        logger.info(
            "  merged %d BurnAddWaterbody polygons (%.1f km2) into %d waterbodies",
            len(wb_gdf) - n_before,
            float(burn.to_crs(info.crs).geometry.area.sum() / 1e6),
            n_before,
        )

    if "FTYPE" in wb_gdf.columns:
        n_pre = len(wb_gdf)
        wb_gdf = wb_gdf[~wb_gdf["FTYPE"].isin(EXCLUDE_WATERBODY_FTYPES)].copy()
        n_excluded = n_pre - len(wb_gdf)
        if n_excluded:
            logger.info(
                "  excluded %d Ice Mass waterbodies (not depression storage; "
                "treated as land)", n_excluded,
            )
    else:
        raise KeyError(
            "waterbody layer has no FTYPE column — cannot exclude Ice Mass "
            "(EXCLUDE_WATERBODY_FTYPES); refusing to write a raster that would "
            "misclassify glacier/permanent-ice cells as depression storage. A "
            "genuinely FTYPE-less waterbody layer is an upstream data problem "
            "(check the source gpkg), not something this pipeline should paper "
            "over."
        )

    n_before = len(wb_gdf)
    wb_gdf = wb_gdf[wb_gdf.geometry.area >= min_area].copy()
    logger.info("  Loaded %d wbodies, kept %d after >= %.1f m^2 filter", n_before, len(wb_gdf), min_area)

    binary = rasterize_binary(wb_gdf, info, all_touched=False)
    binary[~read_land_mask(landmask_path)] = 255  # drop off-land (ocean) cells
    n_in = int((binary == 1).sum())
    logger.info("  %d wbody cells after land mask", n_in)
    write_uint8_binary(binary, info, binary_path)

    regions = clump_regions(binary)
    n_regions = int(regions.max())
    logger.info("  Labeled %d connected components (8-connectivity)", n_regions)
    write_int32_regions(regions, info, regions_path)

    return {"wbody_binary": binary_path, "wbody_regions": regions_path}

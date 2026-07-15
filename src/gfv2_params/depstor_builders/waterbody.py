"""Rasterise waterbody polygons + scipy connected-component labels."""

from __future__ import annotations

import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from ..depstor import (
    RasterInfo,
    clump_regions,
    load_connected_comids,
    rasterize_binary,
    read_land_mask,
    select_connected_waterbodies,
    write_int32_regions,
    write_uint8_binary,
)
from ..nhd_ftypes import EXCLUDE_WATERBODY_FTYPES, NEVER_ONSTREAM_FTYPES
from .context import BuildContext


def _load_onstream_comids(ctx: BuildContext, logger) -> set[int] | None:
    """Union of WBAREACOMI + flow-through COMIDs, for the BurnAdd overlap guard.

    Mirrors the union `wbody_connectivity` computes — NOT its endorheic
    subtraction: `waterbody` runs before `endorheic` in `STEP_ORDER` and cannot see
    its output. Using the pre-endorheic on-stream set is a conservative superset —
    it can only make the overlap guard in `merge_burn_add` fire MORE, never less,
    which is the safe direction (verified: still 0 CONUS-wide hits).

    Returns `None` if the on-stream set can't be determined — `connected_comids_table`
    isn't configured, or either configured table isn't yet staged on disk — so the
    caller must fall back to the broad "raise on any overlap" guard rather than
    silently skip it.
    """
    if ctx.connected_comids_table is None or not ctx.connected_comids_table.exists():
        return None
    connected = load_connected_comids(ctx.connected_comids_table)
    if ctx.flowthrough_comids_table is not None:
        if not ctx.flowthrough_comids_table.exists():
            return None
        connected = connected | load_connected_comids(ctx.flowthrough_comids_table)
    logger.info(
        "  BurnAdd overlap guard: %d on-stream COMIDs loaded (WBAREACOMI + "
        "flow-through, pre-endorheic)", len(connected),
    )
    return connected


def _burn_clumps_reaching_onstream(
    burn: gpd.GeoDataFrame,
    wb_gdf: gpd.GeoDataFrame,
    onstream_comids: set[int],
    buffer_dist: float,
    logger=None,
) -> dict[int, int]:
    """BurnAdd COMIDs whose clump TRANSITIVELY contains an on-stream waterbody.

    `clump_regions` labels 8-connected components, and connectivity is TRANSITIVE:
    BurnAdd -> dprst waterbody `W` -> on-stream waterbody `X` puts all three in ONE
    region, and `regions_touching_mask` then excludes the whole region — silently
    deleting the BurnAdd playa's depression area. A guard that only tests BurnAdd
    against its DIRECT on-stream neighbours cannot see that chain, so this walks the
    adjacency graph outward from each BurnAdd polygon until it stops growing.

    Only polygons that actually RASTERIZE can carry a clump, so Ice Mass
    (`EXCLUDE_WATERBODY_FTYPES`, dropped from the layer entirely by `build()`) cannot
    propagate one. Sub-`min_area_threshold` polygons are NOT excluded here — including
    them can only make the walk reach further, which is the safe (over-approximating)
    direction, consistent with the cell-diagonal buffer itself.

    Returns `{burn_comid: an on-stream waterbody COMID its clump reaches}`. Empty means
    no BurnAdd polygon is at risk. Measured on the real CONUS layer: 113 waterbodies sit
    within one cell diagonal of a BurnAdd polygon, the walk closes after ONE hop (none
    of those 113 has a further neighbour of its own), and NONE of them is on-stream — so
    this is inert today. What is wrong in general is the premise that "merging into an
    already-dprst neighbour is harmless"; today's measurement only happens to be safe
    because those clumps stop at one hop, which no data refresh guarantees.
    """
    prop = wb_gdf
    if "FTYPE" in prop.columns:
        prop = prop[~prop["FTYPE"].isin(EXCLUDE_WATERBODY_FTYPES)]
    prop = prop.reset_index(drop=True)
    if prop.empty or burn.empty:
        return {}

    sel = select_connected_waterbodies(prop, onstream_comids)
    if "FTYPE" in sel.columns:
        sel = sel[~sel["FTYPE"].isin(NEVER_ONSTREAM_FTYPES)]
    is_onstream = np.zeros(len(prop), dtype=bool)
    is_onstream[prop.index.get_indexer(sel.index)] = True

    sidx = prop.sindex
    visited = np.zeros(len(prop), dtype=bool)
    # Which BurnAdd polygon pulled each waterbody into a clump. A separate `visited`
    # array is required: BurnAdd COMIDs are NEGATIVE (NHDPlus PolyID), so no negative
    # sentinel can mark "unvisited" here.
    seed = np.zeros(len(prop), dtype=np.int64)
    frontier = burn.geometry.buffer(buffer_dist).to_numpy()
    frontier_seed = burn["COMID"].to_numpy()

    hops = 0
    while len(frontier):
        hops += 1
        fi, wj = sidx.query(
            gpd.GeoSeries(frontier, crs=prop.crs), predicate="intersects"
        )
        fresh = ~visited[wj]
        fi, wj = fi[fresh], wj[fresh]
        if len(wj) == 0:
            break
        # One frontier polygon can reach the same waterbody twice; first writer wins.
        _, first = np.unique(wj, return_index=True)
        fi, wj = fi[first], wj[first]
        visited[wj] = True
        seed[wj] = frontier_seed[fi]
        if logger:
            logger.info(
                "  BurnAdd clump walk, hop %d: +%d waterbodies", hops, len(wj)
            )
        frontier = prop.geometry.iloc[wj].buffer(buffer_dist).to_numpy()
        frontier_seed = seed[wj]

    hits = np.flatnonzero(visited & is_onstream)
    return {
        int(seed[i]): int(prop["COMID"].iloc[i]) for i in hits
    }


def merge_burn_add(
    wb_gdf: gpd.GeoDataFrame,
    burn_gdf: gpd.GeoDataFrame | None,
    cell_size: float = 30.0,
    onstream_comids: set[int] | None = None,
    logger=None,
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

    `onstream_comids` (see `_load_onstream_comids`) narrows the overlap guard below to
    clumps that actually reach an on-stream waterbody. Measured on the real CONUS layer,
    the 1,658 BurnAdd polygons pull 113 existing waterbodies into their clumps and none
    of those clumps reaches an on-stream feature — the guard is inert on today's data.

    The harm is a clump-level property, NOT a neighbour-level one: `clump_regions` is
    transitive, so BurnAdd -> already-dprst waterbody -> on-stream waterbody merges all
    three into one region and `regions_touching_mask` deletes it. Merging into an
    already-dprst neighbour is therefore NOT automatically harmless — being "dprst by
    COMID" does not mean a waterbody's REGION survives the on-stream exclusion, which is
    precisely what the Great Salt Lake / COMID 10273192 marsh case demonstrates. So the
    guard walks the adjacency graph outward (`_burn_clumps_reaching_onstream`) rather
    than testing direct neighbours only.

    Pass `None` (the default) when the on-stream set is unknown (tables not configured /
    not staged) — the guard then falls back to raising on ANY overlap, the old broad
    behaviour, since a false negative here (silently letting a real on-stream merge slip
    through) is worse than a false positive.
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

    # This must test what clump_regions actually does to the RASTERIZED cells, not
    # vector intersection: 8-connectivity merges cells whose centres are up to
    # `cell_size * sqrt(2)` apart (a diagonal neighbour), so two polygons that do NOT
    # intersect in vector space can still land in adjacent, 8-connected cells and be
    # clump-merged anyway. Buffering by one cell diagonal is a conservative
    # over-approximation of that adjacency test — it can flag a near-miss that wouldn't
    # actually rasterize into the same clump, but it can never miss a real one. Fail
    # loud on the near-miss; never miss the real thing.
    buffer_dist = cell_size * math.sqrt(2)

    if onstream_comids is not None:
        # The harm is a CLUMP-level property: clump_regions is transitive, so a BurnAdd
        # polygon adjacent to a dprst waterbody that is itself adjacent to an on-stream
        # one lands in a single region that regions_touching_mask then deletes whole.
        # Walk the adjacency graph rather than testing direct neighbours only.
        at_risk = _burn_clumps_reaching_onstream(
            burn, wb_gdf, onstream_comids, buffer_dist, logger=logger
        )
        if at_risk:
            sample = sorted(at_risk.items())[:10]
            raise ValueError(
                f"{len(at_risk)} BurnAddWaterbody polygon(s) sit in an 8-connected "
                f"clump (within one rasterized cell diagonal, {buffer_dist:.1f} m) that "
                f"reaches an ON-STREAM waterbody — possibly transitively, through an "
                f"intermediate dprst waterbody. e.g. "
                f"{[f'BurnAdd {b} -> on-stream {w}' for b, w in sample]}. "
                f"clump_regions merges the whole chain into one region, so "
                f"regions_touching_mask would silently drag the BurnAdd depression out "
                f"of dprst. Investigate — do not suppress this."
            )
        return _concat_burn(wb_gdf, burn)

    # On-stream status unknowable (tables not configured / not staged): fall back to the
    # broad guard — raise on ANY overlap. A false negative (silently letting a real
    # on-stream merge through) is worse than a false positive.
    buffered = burn[["COMID", "geometry"]].copy()
    buffered["geometry"] = buffered.geometry.buffer(buffer_dist)
    hits = gpd.sjoin(
        buffered, wb_gdf[["COMID", "geometry"]], how="inner", predicate="intersects",
    )
    if not hits.empty:
        bad = sorted(set(hits["COMID_left"]))[:10]
        raise ValueError(
            f"{hits['COMID_left'].nunique()} BurnAddWaterbody polygon(s) overlap or "
            f"lie within one rasterized cell diagonal ({buffer_dist:.1f} m) of an "
            f"existing waterbody (e.g. {bad}). clump_regions would merge them into one "
            f"8-connected region, so an on-stream neighbour would silently drag the "
            f"BurnAdd depression out of dprst. The on-stream COMID tables are not "
            f"staged, so this guard cannot tell an on-stream neighbour from a harmless "
            f"dprst one — stage them, or investigate the overlap. Do not suppress this."
        )
    return _concat_burn(wb_gdf, burn)


def _concat_burn(wb_gdf: gpd.GeoDataFrame, burn: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Append the (guard-cleared) BurnAdd rows to the waterbody frame."""
    burn = burn[wb_gdf.columns].copy()
    # `member_comid` is a plain string in the real waterbody layer, but
    # burn_add_to_waterbody_frame (Task 1) emits it as int64 (same value as COMID).
    # Left un-normalised, pd.concat below produces an `object` column with mixed
    # str/int rows. Nothing downstream reads it today -- select_connected_waterbodies
    # re-reads the raw gpkg, not this merged frame -- so it's inert, but it's a
    # fragile state a future `sorted()` or `.str` accessor over it would TypeError
    # on. Normalise to whatever dtype wb_gdf already uses before concatenating.
    if "member_comid" in wb_gdf.columns:
        target_dtype = wb_gdf["member_comid"].dtype
        # Must be `pd.api.types.is_object_dtype`, NOT `target_dtype is object`: the
        # latter is always False for a pandas dtype instance, so the branch would never
        # fire. And the cast must stringify explicitly — a plain `.astype(object)` on an
        # int64 column wraps the raw ints rather than stringifying them, reproducing the
        # exact mixed str/int column this normalisation exists to prevent.
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
        if burn.empty:
            # A zero-row table is NOT a legitimate result: nhd_burn_components.main()
            # itself raises on it ("0 BurnAddWaterbody polygons staged across all VPUs
            # -- that would add no depression area at all"), so a configured, present,
            # EMPTY table means the staging was truncated or corrupted after the fact.
            # merge_burn_add would quietly return wb_gdf unchanged and ~722 km2 of
            # playa / closed-lake depression area would vanish from dprst behind an
            # INFO line reading "merged 0 BurnAddWaterbody polygons".
            raise ValueError(
                f"{ctx.burn_add_waterbody_table} is configured but has ZERO rows. "
                f"That is not a legitimate result — the staging step refuses to write "
                f"an empty table — so it has been truncated or corrupted. Re-stage "
                f"with `python -m gfv2_params.download.nhd_burn_components`, or remove "
                f"`burn_add_waterbody_table` from the profile to run without BurnAdd "
                f"depression area deliberately."
            )
        onstream_comids = _load_onstream_comids(ctx, logger)
        if onstream_comids is None:
            logger.warning(
                "  BurnAdd overlap guard: on-stream COMID table(s) unavailable "
                "(`connected_comids_table` not configured, or it/`flowthrough_"
                "comids_table` not yet staged on disk) — cannot restrict the guard "
                "to on-stream neighbours, falling back to the broad guard (raises "
                "on ANY overlap with an existing waterbody, not just an on-stream one)."
            )
        n_before = len(wb_gdf)
        wb_gdf = merge_burn_add(
            wb_gdf, burn, cell_size=abs(info.transform.a),
            onstream_comids=onstream_comids, logger=logger,
        )
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

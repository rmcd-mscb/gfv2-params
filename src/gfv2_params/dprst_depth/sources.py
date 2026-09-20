"""Per-polygon DEM source assignment from the real 3DEP tile inventory (issue #223).

A polygon's candidates are TILE SETS: all real tiles of ONE project, in ONE
UTM zone, that intersect the polygon's rim-buffered window. Ranked by
(covers the whole window, QL, newest collection, project name). The 10 m
seamless tile is always appended as the last resort. The primary set
(`source_tiles`) is what the array reads; the rest are for recovery when the
primary's interior turns out to hold no valid cells.

This exists because the WESM workunit footprint (a convex HULL) claims
ground a project never flew: measured on the gfv2r2 CONUS run, 51.6% of
polygons took the slow per-polygon existence-probe fallback, 67.8% of those
only because two or more project HULLS overlapped their tile cell, and of
300 probed, 61% had exactly ONE real covering tile, 15% had NONE, and only
24% genuinely had 2+. `rank_candidates` and `assign_sources` build the
candidate list from tiles that actually exist (`inventory.load_inventory`'s
real, possibly-cropped, per-tile bounds), so the ranking reflects reality
instead of hull overlap.

`tag_and_assign` is the ONE entry point shared by the in-process builder and
the SLURM planner, so the two paths cannot diverge (same doctrine as
`topo.load_fabric_dprst_polygons`).
"""
from __future__ import annotations

from dataclasses import dataclass

import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from shapely.ops import unary_union

from .tiling import _tile13_key, guard_oversized_windows

TEN_M = "10m"
# A project directory never sinks below "unknown" quality just because it
# matched no WESM row (`inventory.load_project_attrs` -- a directory absent
# from that frame, not an error). 9 sorts after every real QL0-QL3 value, so
# an unmatched project is ranked last on quality, never raises, and never
# silently wins a tie against a project with real WESM provenance.
_UNKNOWN_QL = 9
_NO_DATE = pd.Timestamp("1900-01-01")


@dataclass(frozen=True)
class TileSet:
    """One project's real tiles, in one UTM zone, for one polygon's window.

    `project == "10m"` means the seamless 1/3 arc-second last resort, whose
    single `keys` entry is a `/vsicurl/` path from `_tile13_key`, not a 3DEP
    1 m project tile.
    """

    project: str
    keys: tuple[str, ...]
    covers: bool


def encode(ts: TileSet) -> str:
    """Serialise a `TileSet` as `"project|covers|key1|key2..."`.

    No 3DEP project directory name contains `|` (verified against all 967
    listed directories on 2026-09-19), so the delimiter is safe.
    """
    return "|".join([ts.project, "1" if ts.covers else "0", *ts.keys])


def decode(s: str) -> TileSet:
    project, covers, *keys = s.split("|")
    return TileSet(project, tuple(keys), covers == "1")


def tag_best_topo(dprst: gpd.GeoDataFrame, inventory: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Tag each polygon `"1m"` iff its centroid falls inside a REAL 1 m tile's
    own bounds, else `"10m"`.

    Deliberately a centroid-in-tile test against `inventory`'s per-tile COG
    bounds, not a hull membership test -- a hull can claim ground a project
    never flew (see module docstring), so this is the fix for that: a
    polygon whose centroid lands in the gap between two real, non-abutting
    tiles is correctly `"10m"`, even if some project's convex hull would have
    covered it.
    """
    out = dprst.copy()
    pts = out.set_geometry(out.geometry.centroid)
    hit = gpd.sjoin(pts, inventory[["geometry"]], how="left", predicate="within")
    has = hit.groupby(level=0)["index_right"].first().notna()
    out["best_topo"] = has.map({True: "1m", False: TEN_M})
    return out


def rank_candidates(window, hits: gpd.GeoDataFrame, attrs: pd.DataFrame) -> list[TileSet]:
    """Rank 1 m tile-set candidates for `window` from the inventory rows in `hits`.

    One `TileSet` per PROJECT: its tiles are unioned from whichever UTM ZONE
    has the larger overlap with `window` (a project can span zones -- e.g.
    WI_12County_B22 publishes 300 zone-15 and 167 zone-16 tiles -- and
    `gdal.BuildVRT` cannot mosaic sources in different CRSs, so only one
    zone's tiles can go in a set). Ranked ascending by
    `(not covers, ql_rank, -collect_end, project)`:

    1. Full coverage of the rim-buffered window beats partial coverage --
       the science call is that a set covering the whole window can compute
       a rim depth without falling back mid-polygon.
    2. Best quality level (lowest `ql_rank`) -- denser points define the
       shoreline rim better, and the rim drives the depth estimate.
    3. Newest `collect_end`.
    4. Project name, purely for determinism when 2 and 3 tie (as they do in
       the QL/date test fixture) -- a re-run must reproduce the same
       assignment.

    A project absent from `attrs` (no WESM match at all) is ranked as
    `_UNKNOWN_QL` with no date, never excluded and never an error.
    """
    ranked = []
    for project, grp in hits.groupby("project", sort=True):
        by_zone = grp.groupby("zone")
        zone = max(
            by_zone.groups,
            key=lambda z: (by_zone.get_group(z).geometry.intersection(window).area.sum(), -z),
        )
        tiles = by_zone.get_group(zone).sort_values("key")
        covers = bool(unary_union(list(tiles.geometry)).covers(window))
        a = attrs.loc[project] if project in attrs.index else None
        ql = int(a["ql_rank"]) if a is not None else _UNKNOWN_QL
        date = a["collect_end"] if a is not None and pd.notna(a["collect_end"]) else _NO_DATE
        sort_key = (not covers, ql, -date.value, project)
        ranked.append((sort_key, TileSet(project, tuple(tiles["key"]), covers)))
    return [ts for _, ts in sorted(ranked, key=lambda kv: kv[0])]


def assign_sources(dprst: gpd.GeoDataFrame, inventory: gpd.GeoDataFrame, attrs: pd.DataFrame,
                    rim_m: float = 200.0) -> gpd.GeoDataFrame:
    """Assign a ranked candidate list + primary source per polygon.

    `dprst` must already carry `best_topo` (`tag_best_topo`, after
    `guard_oversized_windows` -- see `tag_and_assign`). Only `"1m"` polygons
    get real 1 m candidates from `rank_candidates`; every polygon, `"1m"` or
    `"10m"`, gets the 10 m seamless tile appended as the LAST entry of
    `candidates` -- the guaranteed last resort, never dropped, so
    `candidates` is never empty. `source_tiles` is the encoded first
    (highest-ranked) candidate.
    """
    out = dprst.copy()
    bounds = out.geometry.bounds
    windows = gpd.GeoDataFrame(
        {"idx": out.index},
        geometry=[
            box(r.minx - rim_m, r.miny - rim_m, r.maxx + rim_m, r.maxy + rim_m)
            for r in bounds.itertuples()
        ],
        crs=out.crs,
    )
    is_1m = (out["best_topo"] == "1m").values
    hits = gpd.sjoin(windows[is_1m], inventory, how="inner", predicate="intersects")
    hit_groups = {idx: grp for idx, grp in hits.groupby("idx")}
    inv_cols = [c for c in inventory.columns if c != "geometry"]

    cands, primary = [], []
    for idx, geom, win in zip(out.index, out.geometry, windows.geometry):
        ten = encode(TileSet(TEN_M, (_tile13_key(geom, out.crs),), True))
        ranked: list[str] = []
        if idx in hit_groups:
            g = hit_groups[idx]
            tiles = gpd.GeoDataFrame(
                g[inv_cols], geometry=inventory.geometry.loc[g["index_right"]].values, crs=out.crs
            )
            ranked = [encode(t) for t in rank_candidates(win, tiles, attrs)]
        lst = ranked + [ten]
        cands.append(lst)
        primary.append(lst[0])
    out["candidates"] = cands
    out["source_tiles"] = primary
    return out


def tag_and_assign(dprst: gpd.GeoDataFrame, inventory_path, attrs_path, logger) -> gpd.GeoDataFrame:
    """Tag + assign, in the fixed order both the builder and the planner must use.

    Order is load-bearing: `guard_oversized_windows` must run BETWEEN
    `tag_best_topo` and `assign_sources` so a polygon it retags to `"10m"`
    never gets 1 m candidates built for it (assign_sources reads the
    POST-guard `best_topo`). This is the ONE function both the in-process
    builder and the SLURM `--plan` path call, mirroring
    `topo.load_fabric_dprst_polygons`'s shared-entry-point doctrine so the
    two paths cannot silently diverge on which polygons get downgraded.
    """
    from .inventory import load_inventory, load_project_attrs

    inventory = load_inventory(inventory_path)
    attrs = load_project_attrs(attrs_path)
    out = tag_best_topo(dprst, inventory)
    logger.info(
        "  best_topo: %d/%d polygons inside a real 1m tile", int((out["best_topo"] == "1m").sum()), len(out)
    )
    out = guard_oversized_windows(out, logger=logger)
    out = assign_sources(out, inventory, attrs)
    n_multi = sum(len(decode(s).keys) > 1 for s in out["source_tiles"])
    n_alt = sum(len(c) > 2 for c in out["candidates"])
    logger.info(
        "  sources: %d primary sets span >1 tile; %d polygons have a real alternative project",
        n_multi, n_alt,
    )
    return out

# src/gfv2_params/download/nhd_flowthrough.py  (module top + classifier; main() added in Task 2)
"""Distil flow-through (on-stream) waterbody COMIDs from NHD topology.

WBAREACOMI (see nhd_flowlines) only flags waterbodies NHD drew an artificial
path through. Many through-flow swamps/marshes carry no WBAREACOMI and are
wrongly left in depression storage, so their whole upstream watershed counts as
draining to dprst. This module adds a second, geometry-based on-stream signal:
a waterbody that a stream demonstrably flows THROUGH (channel inflow AND
outflow) is on-stream/lake, not a depression. Endorheic terminal sinks (Playa,
Ice Mass) are force-kept as dprst.

The COMID set written here is unioned with connected_waterbody_comids.parquet by
the depstor wbody_connectivity builder.
"""

from __future__ import annotations

import geopandas as gpd
from shapely.geometry import Point

# A conveyance NHDFlowline carries channelised flow (vs. Pipeline, Coastline...).
CONVEYANCE_FTYPES = {"StreamRiver", "ArtificialPath", "Connector", "CanalDitch"}
# NHDArea polygons that ARE the 2-D channel (wide/braided rivers).
CONVEYANCE_AREA_FTYPES = {"StreamRiver"}
# Endorheic guardrail: these never get promoted out of dprst, regardless of
# topology (and FLOWDIR is unreliable around them).
FORCE_DPRST_FTYPES = {"Playa", "Ice Mass"}

_DIGITIZED = "With Digitized"  # FLOWDIR value where geometry direction is trusted


def _endpoints(geom):
    """(upstream_point, downstream_point) for a (Multi)LineString: first & last coord."""
    if geom.geom_type == "MultiLineString":
        parts = list(geom.geoms)
        first = parts[0].coords[0]
        last = parts[-1].coords[-1]
    else:
        first = geom.coords[0]
        last = geom.coords[-1]
    return Point(first), Point(last)


def flowthrough_comids(
    waterbodies: gpd.GeoDataFrame,
    flowlines: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame | None = None,
) -> set[int]:
    """COMIDs of waterbodies classified on-stream by flow-through topology.

    `waterbodies`: polygons with COMID, FTYPE. `flowlines`: lines with FTYPE,
    FLOWDIR. `areas`: optional NHDArea polygons with FTYPE. All share one CRS.

    A waterbody is on-stream if ANY of:
      T1  a single conveyance flowline crosses its boundary >= 2 times,
      T2  it has >= 1 inflow (a 'With Digitized' conveyance line whose
          downstream end is inside) AND >= 1 outflow (upstream end inside),
      T3  it overlaps a conveyance NHDArea polygon.
    Playa / Ice Mass waterbodies are dropped first and never returned.
    """
    wb = waterbodies[~waterbodies["FTYPE"].isin(FORCE_DPRST_FTYPES)].copy()
    if wb.empty:
        return set()
    wb = wb.reset_index(drop=True)
    wb["_wbidx"] = wb.index

    conv = flowlines[flowlines["FTYPE"].isin(CONVEYANCE_FTYPES)].copy()
    onstream: set[int] = set()

    if not conv.empty:
        conv = conv.reset_index(drop=True)
        # Candidate (waterbody, flowline) pairs that intersect at all.
        pairs = gpd.sjoin(
            conv[["FTYPE", "FLOWDIR", "geometry"]],
            wb[["_wbidx", "geometry"]],
            how="inner", predicate="intersects",
        )

        # --- T1: a single line crosses the boundary >= 2 times ---
        t1_idx: set[int] = set()
        for line_pos, wbidx in zip(pairs.index, pairs["_wbidx"]):
            line = conv.geometry.iloc[line_pos]
            poly = wb.geometry.iloc[wbidx]
            crossing = line.intersection(poly.boundary)
            n = 0 if crossing.is_empty else (
                len(crossing.geoms) if crossing.geom_type.startswith("Multi") else 1
            )
            if n >= 2:
                t1_idx.add(int(wbidx))

        # --- T2: inflow endpoint AND outflow endpoint, trusting digitization ---
        has_inflow: set[int] = set()
        has_outflow: set[int] = set()
        dig = pairs[pairs["FLOWDIR"] == _DIGITIZED]
        for line_pos, wbidx in zip(dig.index, dig["_wbidx"]):
            up, down = _endpoints(conv.geometry.iloc[line_pos])
            poly = wb.geometry.iloc[wbidx]
            if poly.covers(down):   # downstream end inside -> water flows IN
                has_inflow.add(int(wbidx))
            if poly.covers(up):     # upstream end inside -> water flows OUT
                has_outflow.add(int(wbidx))
        t2_idx = has_inflow & has_outflow

        for wbidx in t1_idx | t2_idx:
            onstream.add(int(wb.loc[wbidx, "COMID"]))

    # --- T3: overlap a conveyance NHDArea polygon ---
    if areas is not None and not areas.empty:
        ca = areas[areas["FTYPE"].isin(CONVEYANCE_AREA_FTYPES)]
        if not ca.empty:
            hit = gpd.sjoin(
                wb[["COMID", "geometry"]], ca[["geometry"]],
                how="inner", predicate="intersects",
            )
            onstream |= {int(c) for c in hit["COMID"].unique()}

    return onstream

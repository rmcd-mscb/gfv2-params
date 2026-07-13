"""Endorheic (closed-basin) depression-storage classifier.

The existing on-stream tests (WBAREACOMI, flow-through) are LOCAL: "does a Network
flowline enter and leave this waterbody?" NHD draws Network artificial paths between
the arms of a terminal lake, so the lake reads as through-flowing and is promoted
on-stream. No local test can see that a whole basin is endorheic -- which is why the
Great Salt Lake is currently classified on-stream.

Two signals fix that, both of which can only SUBTRACT from the on-stream set:

Signal A (primary) -- terminus-inside-itself, on the FDR grid.
    A waterbody is depression storage iff its water's terminus lies INSIDE ITSELF.
    GSL's water ends in GSL. A pond upstream in the same closed basin ends in GSL,
    not in the pond, so it stays on-stream. Lewis and Clark Lake's water ends in the
    Gulf of Mexico. (This is why the rule is terminus-INSIDE-ITSELF and not merely
    terminates-at-a-sink: the latter demotes every on-stream reservoir in a closed
    basin.)

Signal B (complement) -- majority-inside a WBD type-C (closed) HUC12. Earns its place
    because Walker Lake contains no FDR terminal cell, so Signal A misses it.

Signal A reads the same grid -- and runs the same kernel -- that `routing` reads, so
the classifier and the router agree BY CONSTRUCTION: d8_routing treats code 0 as a
terminus, so a waterbody the router dead-ends in IS a depression.
"""

from __future__ import annotations

import geopandas as gpd

# Share of a waterbody's area that must lie inside the closed-basin union (Signal B),
# and share of its cells that must reach its own terminus (Signal A). NOT a tuned
# knob: frac_own is bimodal (204 of 239 CONUS candidates at >= 0.95, only 3 in
# 0.45-0.55) and the demotion set moves ~3% across thresholds 0.3-0.7.
MIN_FRAC = 0.5


def closed_basin_comids(
    wb_gdf: gpd.GeoDataFrame,
    closed_gdf: gpd.GeoDataFrame,
    min_frac: float = MIN_FRAC,
) -> set[int]:
    """COMIDs of waterbodies majority-inside the DISSOLVED union of closed HUC12s.

    Dissolve first, then measure: a lake straddling two *adjacent* closed HUC12s is
    fully inside the closed system but majority-inside neither polygon on its own.

    Majority-area -- NOT `intersects`, NOT `within`:
      * `within` fails on Great Salt Lake, which spills 1.1% into a neighbouring
        HUC12 (frac_in = 0.989).
      * `intersects` returns True for a ZERO-interior-overlap boundary touch, which
        wrongly grabs lakes grazing a closed boundary at frac_in = 0.000 (Eagle Lake,
        Middle Alkali Lake). Do not "simplify" this predicate back to `intersects`.
    """
    if closed_gdf.empty:
        return set()
    closed = closed_gdf.to_crs(wb_gdf.crs) if closed_gdf.crs != wb_gdf.crs else closed_gdf
    union = closed.geometry.union_all()
    area = wb_gdf.geometry.area
    frac = wb_gdf.geometry.intersection(union).area / area.where(area > 0)
    return {int(c) for c in wb_gdf.loc[frac > min_frac, "COMID"]}

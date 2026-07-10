"""Diagnostic probe for Issue #173 Phase 0 spike: dprst_depth_avg from
best-available topography. Analysis-only; not a builder. See
docs/superpowers/specs/2026-07-10-dprst-depth-phase0-spike-design.md.
"""
from __future__ import annotations

import geopandas as gpd

from gfv2_params.nhd_ftypes import EXCLUDE_WATERBODY_FTYPES, FORCE_DPRST_FTYPES
from gfv2_params.depstor import select_connected_waterbodies


def dprst_polygons(wb_gdf: gpd.GeoDataFrame, connected: set[int]) -> gpd.GeoDataFrame:
    """Reconstruct the shipped `dprst` polygon set at the polygon level.

    Mirrors wbody_connectivity -> dprst: drop genuinely on-stream waterbodies,
    force Playa to dprst, exclude Ice Mass entirely.
    """
    if "FTYPE" not in wb_gdf.columns:
        raise KeyError("waterbody layer has no FTYPE column; cannot classify dprst")
    wb = wb_gdf[~wb_gdf["FTYPE"].isin(EXCLUDE_WATERBODY_FTYPES)].copy()
    onstream = select_connected_waterbodies(wb, connected)
    onstream = onstream[~onstream["FTYPE"].isin(FORCE_DPRST_FTYPES)]
    onstream_idx = set(onstream.index)
    return wb[~wb.index.isin(onstream_idx)].copy()

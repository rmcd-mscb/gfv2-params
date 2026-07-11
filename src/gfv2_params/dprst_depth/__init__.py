"""dprst_depth_avg builder package (issue #173).

Re-exports the topography helpers validated in the Phase 0 spike
(`gfv2_params.dprst_depth.topo`) so both the diagnostic script and the
Phase 1 builder import a single copy.
"""
from __future__ import annotations

from .topo import (
    _normalize_nodata,
    _tile13_name,
    depth_to_spill,
    dprst_polygons,
    is_hydroflattened,
    lake_max_depth,
    max_to_mean,
    read_window,
    resolution_class,
    volume_mean_depth,
)

__all__ = [
    "_normalize_nodata",
    "_tile13_name",
    "depth_to_spill",
    "dprst_polygons",
    "is_hydroflattened",
    "lake_max_depth",
    "max_to_mean",
    "read_window",
    "resolution_class",
    "volume_mean_depth",
]

"""Gridded-time-series → HRU aggregation harness (source-agnostic).

The time-series counterpart to the static-raster zonal_runners: wraps gdptools
UserCatData/WeightGen/AggGen so any gridded source (SNODAS today, climate later)
can be area-weighted to an HRU fabric via a declarative SourceAdapter.
"""

from __future__ import annotations

from .adapter import SourceAdapter

__all__ = ["SourceAdapter"]

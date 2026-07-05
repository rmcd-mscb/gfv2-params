"""SNODAS daily SWE adapter: area-weighted mean SWE + snow-covered-area fraction.

SWE arrives pre-projected to EPSG:5070 (1000 m) with -9999 fill over ocean /
non-CONUS / Great Lakes. The hook re-asserts that fill as NaN and derives a
binary snow-cover field `scov = (swe > 0)` that *carries the NaN mask* (fill
pixels stay NaN, not 0), so under masked_mean the HRU `scov` is the
area-weighted fraction of finite pixels with snow — the Driscoll et al. SCA.
"""

from __future__ import annotations

import numpy as np
import xarray as xr

from .adapter import SourceAdapter

_SNODAS_FILL_THRESHOLD = -9990  # -9999 fill, with slop margin


def _snodas_hook(ds: xr.Dataset) -> xr.Dataset:
    swe = ds["swe"].where(ds["swe"] > _SNODAS_FILL_THRESHOLD)
    scov = xr.where(swe.notnull(), (swe > 0).astype("float32"), np.float32("nan"))
    return ds.assign(swe=swe, scov=scov)


SNODAS_ADAPTER = SourceAdapter(
    source_key="snodas",
    variables=("swe", "scov"),
    files_glob="snodas_daily_*.nc",
    source_crs="EPSG:5070",
    x_coord="x",
    y_coord="y",
    time_coord="time",
    stat_method="masked_mean",
    pre_aggregate_hook=_snodas_hook,
    grid_variable="swe",
)

"""Per-HRU source-coverage diagnostic (issue #166).

gdptools' masked_mean returns 0.0 — not NaN — for an HRU whose source cells are
all fill, so a zero-coverage HRU is indistinguishable from a genuinely zero-valued
one. These tests pin the weight-weighted valid fraction that makes the difference
visible.
"""

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import xarray as xr
from shapely.geometry import box

from gfv2_params.aggregate import SourceAdapter
from gfv2_params.aggregate.coverage import coverage_from_weights, coverage_over_years

_FILL = -9999.0


def _hook(ds: xr.Dataset) -> xr.Dataset:
    return ds.assign(swe=ds["swe"].where(ds["swe"] > -9990))


def _adapter(**over):
    kw = dict(
        source_key="demo", variables=("swe",), files_glob="demo_daily_*.nc",
        source_crs="EPSG:5070", x_coord="x", y_coord="y", time_coord="time",
        stat_method="masked_mean", pre_aggregate_hook=_hook,
    )
    kw.update(over)
    return SourceAdapter(**kw)


def _grid(left_fill: bool, right_fill: bool) -> xr.Dataset:
    """4x4 1-km grid, EPSG:5070; each half independently all-fill or all-valid."""
    x = np.array([500.0, 1500.0, 2500.0, 3500.0])
    y = np.array([3500.0, 2500.0, 1500.0, 500.0])   # descending (north-up)
    a = np.ones((4, 4), dtype="float32")
    a[:, :2] = _FILL if left_fill else 1.0
    a[:, 2:] = _FILL if right_fill else 1.0
    ds = xr.Dataset(
        {"swe": (("time", "y", "x"), a[None, ...])},
        coords={"time": pd.to_datetime(["2010-01-01"]), "y": y, "x": x},
    )
    ds["x"].attrs["units"] = "m"
    ds["y"].attrs["units"] = "m"
    return ds


def _polys() -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"hru_id": [1, 2]},
        geometry=[box(0, 0, 2000, 4000), box(2000, 0, 4000, 4000)],
        crs="EPSG:5070",
    )


def _weights() -> pd.DataFrame:
    """Equal weight on each of the 8 cells in each half-polygon."""
    rows = []
    for hru, js in ((1, (0, 1)), (2, (2, 3))):
        for i in range(4):
            for j in js:
                rows.append({"hru_id": hru, "i": i, "j": j, "wght": 0.125})
    return pd.DataFrame(rows)


def test_all_fill_polygon_reports_zero_coverage():
    # The load-bearing case: HRU 1 sits entirely on fill, so its masked_mean
    # is 0.0 and looks snow-free. Coverage must say 0.0, not 1.0.
    out = coverage_from_weights(
        _adapter(), _grid(left_fill=True, right_fill=False), _polys(),
        "hru_id", _weights(),
    )
    assert out[1] == pytest.approx(0.0)
    assert out[2] == pytest.approx(1.0)


def test_fully_covered_polygons_report_one():
    out = coverage_from_weights(
        _adapter(), _grid(left_fill=False, right_fill=False), _polys(),
        "hru_id", _weights(),
    )
    assert out[1] == pytest.approx(1.0)
    assert out[2] == pytest.approx(1.0)


def test_partial_coverage_is_the_weighted_valid_fraction():
    # HRU 1 spans both halves: 8 valid cells of 16, equally weighted -> 0.5.
    gdf = gpd.GeoDataFrame({"hru_id": [1]}, geometry=[box(0, 0, 4000, 4000)],
                           crs="EPSG:5070")
    w = pd.DataFrame([
        {"hru_id": 1, "i": i, "j": j, "wght": 1 / 16}
        for i in range(4) for j in range(4)
    ])
    out = coverage_from_weights(
        _adapter(), _grid(left_fill=True, right_fill=False), gdf, "hru_id", w)
    assert out[1] == pytest.approx(0.5)


def test_coverage_is_weighted_not_a_plain_cell_count():
    # Weight the two valid cells far more heavily than the two fill cells; a
    # naive count would say 0.5, the weighted fraction says 0.9.
    # This polygon's bounds-subset keeps y = 2500/1500/500 (3 rows, the 2000 m
    # margin pulls in two extra), so the bottom grid row is index 2, not 3.
    gdf = gpd.GeoDataFrame({"hru_id": [1]}, geometry=[box(0, 0, 4000, 1000)],
                           crs="EPSG:5070")
    w = pd.DataFrame([
        {"hru_id": 1, "i": 2, "j": 0, "wght": 0.05},   # left half -> fill
        {"hru_id": 1, "i": 2, "j": 1, "wght": 0.05},   # left half -> fill
        {"hru_id": 1, "i": 2, "j": 2, "wght": 0.45},   # right half -> valid
        {"hru_id": 1, "i": 2, "j": 3, "wght": 0.45},   # right half -> valid
    ])
    out = coverage_from_weights(
        _adapter(), _grid(left_fill=True, right_fill=False), gdf, "hru_id", w)
    assert out[1] == pytest.approx(0.9)


def test_raises_when_weight_index_does_not_match_the_subset():
    # Weight (i, j) index the batch's OWN bounds-subset. Handing in a weight
    # table from a different batch silently produces plausible-looking garbage
    # unless this is caught — so it must raise, not guess.
    bad = pd.DataFrame([{"hru_id": 1, "i": 0, "j": 99, "wght": 1.0}])
    with pytest.raises(ValueError, match="index"):
        coverage_from_weights(
            _adapter(), _grid(left_fill=True, right_fill=False), _polys(),
            "hru_id", bad)


def test_coverage_over_years_averages_a_moving_footprint(tmp_path):
    # The real SNODAS case: HRU 1 is all-fill in year one and fully valid in
    # year two. A single-year probe would call it permanently uncovered; the
    # mean across years reports 0.5, marking it partially covered.
    y1 = tmp_path / "demo_daily_2004.nc"
    y2 = tmp_path / "demo_daily_2015.nc"
    _grid(left_fill=True, right_fill=False).to_netcdf(y1)
    _grid(left_fill=False, right_fill=False).to_netcdf(y2)

    out = coverage_over_years(
        _adapter(), _polys(), "hru_id", _weights(), [y1, y2]
    ).set_index("hru_id")
    assert out.loc[1, "coverage"] == pytest.approx(0.5)
    assert out.loc[2, "coverage"] == pytest.approx(1.0)
    assert out.loc[1, "n_years_sampled"] == 2


def test_coverage_over_years_requires_at_least_one_file():
    with pytest.raises(ValueError, match="no source files"):
        coverage_over_years(_adapter(), _polys(), "hru_id", _weights(), [])


def test_adapter_without_a_hook_treats_nan_as_the_fill_marker():
    # Coverage is defined by the adapter's own fill convention (its hook), not
    # by a hard-coded sentinel; an adapter with no hook falls back to NaN.
    ds = _grid(left_fill=False, right_fill=False)
    ds["swe"].values[0, :, :2] = np.nan
    out = coverage_from_weights(
        _adapter(pre_aggregate_hook=None), ds, _polys(), "hru_id", _weights())
    assert out[1] == pytest.approx(0.0)
    assert out[2] == pytest.approx(1.0)

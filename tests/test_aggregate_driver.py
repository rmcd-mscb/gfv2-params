from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import box

from gfv2_params.aggregate import SourceAdapter
from gfv2_params.aggregate.driver import aggregate_source


def _synthetic_grid(tmp_path: Path) -> Path:
    # 4x4 grid, 1000 m cells, EPSG:5070, origin (0,0). Cell centers at 500,1500,...
    x = np.array([500.0, 1500.0, 2500.0, 3500.0])
    y = np.array([3500.0, 2500.0, 1500.0, 500.0])  # descending (north-up)
    # day 0: all 1.0 ; day 1: left half 2.0, right half 0.0
    d0 = np.ones((4, 4), dtype="float32")
    d1 = np.array([[2, 2, 0, 0]] * 4, dtype="float32")
    swe = np.stack([d0, d1])  # (time, y, x)
    ds = xr.Dataset(
        {"swe": (("time", "y", "x"), swe)},
        coords={"time": pd.to_datetime(["2010-01-01", "2010-01-02"]), "y": y, "x": x},
    )
    # gdptools' intersection check requires a units attr on the projected x/y
    # coords (it uses "degree(s)" in units to detect geographic grids).
    ds["x"].attrs["units"] = "m"
    ds["y"].attrs["units"] = "m"
    ds["swe"].rio  # noqa: keep rioxarray import path warm if present
    p = tmp_path / "demo_daily_2010.nc"
    ds.to_netcdf(p)
    return p


def _two_polys() -> gpd.GeoDataFrame:
    # left poly covers x in [0,2000], right poly x in [2000,4000], full y.
    left = box(0, 0, 2000, 4000)
    right = box(2000, 0, 4000, 4000)
    return gpd.GeoDataFrame({"hru_id": [1, 2]}, geometry=[left, right], crs="EPSG:5070")


def test_aggregate_source_area_weighted_mean(tmp_path):
    _synthetic_grid(tmp_path)
    gdf = _two_polys()
    adapter = SourceAdapter(
        source_key="demo", variables=("swe",), files_glob="demo_daily_*.nc",
        source_crs="EPSG:5070", x_coord="x", y_coord="y", time_coord="time",
        stat_method="mean",
    )
    out = aggregate_source(
        adapter, gdf, "hru_id",
        input_dir=tmp_path, output_dir=tmp_path / "out",
        weight_file=tmp_path / "w.csv", output_prefix="demo",
    )
    assert len(out) == 1
    res = xr.open_dataset(out[0])
    # day 0: both polys mean 1.0 ; day 1: left mean 2.0, right mean 0.0
    swe = res["swe"].sel(hru_id=[1, 2]).values  # (time, hru)
    np.testing.assert_allclose(swe[0], [1.0, 1.0], atol=1e-6)
    np.testing.assert_allclose(swe[1], [2.0, 0.0], atol=1e-6)

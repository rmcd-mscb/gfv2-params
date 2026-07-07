from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import box

from gfv2_params.aggregate.driver import aggregate_source
from gfv2_params.aggregate.snodas import SNODAS_ADAPTER, _snodas_hook


def _ds(swe_vals):
    return xr.Dataset({"swe": (("time", "y", "x"), np.array(swe_vals, dtype="float32"))})


def test_hook_derives_scov_and_masks_fill():
    # one time slice, 1x3: [fill=-9999, dry=0, snow=5]
    ds = _ds([[[-9999.0, 0.0, 5.0]]])
    out = _snodas_hook(ds)
    swe = out["swe"].values.ravel()
    scov = out["scov"].values.ravel()
    assert np.isnan(swe[0])                    # fill masked to NaN
    assert np.isnan(scov[0])                    # scov NaN where fill (not counted as 0)
    assert scov[1] == 0.0                       # dry -> 0
    assert scov[2] == 1.0                       # snow -> 1


def test_adapter_settings():
    assert SNODAS_ADAPTER.variables == ("swe", "scov")
    assert SNODAS_ADAPTER.stat_method == "masked_mean"
    assert SNODAS_ADAPTER.source_crs == "EPSG:5070"
    assert SNODAS_ADAPTER.grid_variable == "swe"
    assert SNODAS_ADAPTER.files_glob == "snodas_daily_*.nc"


def test_snodas_adapter_declares_swe_std():
    assert SNODAS_ADAPTER.std_variables == ("swe",)


# --- End-to-end masked_mean / fill-exclusion regression -------------------
# The unit test above pins the _snodas_hook contract (fill -> NaN, scov carries
# the NaN mask). The test below drives the *whole* Stage-1 path
# (aggregate_source -> UserCatData/AggGen with stat_method="masked_mean") to
# prove the load-bearing SCA claim end-to-end: SNODAS fill cells are dropped
# from the area-weighted reduction, so per-HRU swe/scov are computed over the
# *finite* cells only — scov is the fraction of finite cells with snow, not
# diluted by treating fill as 0. A gdptools upgrade or an aggregate_variables
# refactor that silently reverted masked handling would fail here; it is not
# covered by the plain-"mean" driver test (which has no fill), nor by the
# _snodas_hook unit test (which never runs the aggregation). Modeled on
# tests/test_aggregate_driver.py::_synthetic_grid/_two_polys.
#
# gdptools drops the source grid's outer ring of cells (a cell-boundary
# inference artifact — verified: a 6x6 grid keeps only i,j in {1..4}). The
# driver test never noticed because its values are uniform per polygon, so
# dropping edge cells can't change a mean. Here the values vary within each
# polygon, so the meaningful fill/snow/dry pattern is placed in the grid
# INTERIOR and the outer ring is filled with -9999. That ring choice makes the
# assertion robust to the boundary heuristic either way: if the ring is dropped
# it never counts, and if a future gdptools keeps it, it is fill and masked out.

_FILL = -9999.0


def _snodas_grid(tmp_path: Path) -> Path:
    """6x6, 1000 m, EPSG:5070 grid; fill/snow/dry pattern in the interior 4x4.

    One time slice. Cell centres at 500..5500 (x) and, north-up, 5500..500 (y).
    The outer ring (row/col 0 and 5) is all -9999 fill (dropped by gdptools, or
    masked if kept). The interior columns split at x=3000: cols x=1500,2500 fall
    in the left polygon, cols x=3500,4500 in the right. The left polygon's
    interior holds two -9999 fill cells plus snow/dry cells; the right polygon's
    interior is all-finite (a no-fill control). Named to match
    ``SNODAS_ADAPTER.files_glob`` and to carry a 4-digit year for the driver's
    per-file year parse.
    """
    x = np.array([500.0, 1500.0, 2500.0, 3500.0, 4500.0, 5500.0])
    y = np.array([5500.0, 4500.0, 3500.0, 2500.0, 1500.0, 500.0])  # descending
    F = _FILL
    # rows = y (top->bottom), cols = x (left->right). Interior = rows 1..4,
    # cols 1..4. Left poly interior = cols 1,2 (x=1500,2500); right = cols 3,4.
    #   Left finite cells  = [0, 0, 10, 20, 30, 0] (col-1 fill in rows 1,2)
    #   Right finite cells = six 4.0 + two 0.0, no fill
    grid = np.array(
        [
            [F, F,  F,  F, F, F],
            [F, F, 10,  4, 4, F],
            [F, F, 20,  4, 4, F],
            [F, 0, 30,  4, 4, F],
            [F, 0,  0,  0, 0, F],
            [F, F,  F,  F, F, F],
        ],
        dtype="float32",
    )
    ds = xr.Dataset(
        {"swe": (("time", "y", "x"), grid[np.newaxis, :, :])},  # (time=1, y, x)
        coords={"time": pd.to_datetime(["2010-01-01"]), "y": y, "x": x},
    )
    # gdptools detects geographic grids via a "degree(s)" units attr, so the
    # projected x/y coords must carry a non-degree units attr (see driver test).
    ds["x"].attrs["units"] = "m"
    ds["y"].attrs["units"] = "m"
    p = tmp_path / "snodas_daily_2010.nc"
    ds.to_netcdf(p)
    return p


def _two_polys() -> gpd.GeoDataFrame:
    # left poly covers x in [0,3000] (interior cells x=1500,2500); right x in
    # [3000,6000] (interior cells x=3500,4500); both span the full y extent. The
    # interior cells fall wholly inside one poly, so each carries equal area
    # weight and masked_mean reduces to a nanmean over that poly's finite cells.
    left = box(0, 0, 3000, 6000)
    right = box(3000, 0, 6000, 6000)
    return gpd.GeoDataFrame({"hru_id": [1, 2]}, geometry=[left, right], crs="EPSG:5070")


def test_aggregate_source_masked_mean_excludes_fill(tmp_path):
    _snodas_grid(tmp_path)
    gdf = _two_polys()
    out = aggregate_source(
        SNODAS_ADAPTER, gdf, "hru_id",
        input_dir=tmp_path, output_dir=tmp_path / "out",
        weight_file=tmp_path / "w.csv", output_prefix="snodas",
    )
    assert len(out) == 1
    res = xr.open_dataset(out[0])
    # scov is derived by the hook, not present in the source file: both the
    # source var and the derived var must survive the single multi-var pass.
    assert "swe" in res and "scov" in res

    swe = res["swe"].sel(hru_id=[1, 2]).values[0]   # single time slice -> (hru,)
    scov = res["scov"].sel(hru_id=[1, 2]).values[0]

    # Equal-area interior cells, so masked_mean == nanmean over the finite
    # in-poly cells.
    # Left HRU finite cells = [0, 0, 10, 20, 30, 0] (2 fill cells dropped):
    #   swe  = 60 / 6 = 10.0
    #   scov = 3 snow / 6 finite = 0.5
    # Right HRU all 8 cells finite (six 4.0, two 0.0), no fill:
    #   swe  = 24 / 8 = 3.0
    #   scov = 6 snow / 8 finite = 0.75
    np.testing.assert_allclose(swe, [10.0, 3.0], atol=1e-6)
    np.testing.assert_allclose(scov, [0.5, 0.75], atol=1e-6)

    # Discriminating guard: had fill been counted as 0 (the regression this test
    # exists to catch), the left HRU would read 60/8 = 7.5 and 3/8 = 0.375. The
    # asserted values differ, so a silent revert to fill-as-0 fails this test.
    assert not np.isclose(swe[0], 60.0 / 8)     # 7.5
    assert not np.isclose(scov[0], 3.0 / 8)     # 0.375

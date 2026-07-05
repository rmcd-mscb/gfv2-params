"""Unit tests for the Stage 2 derive_snarea_curve readers.

Targets the two pure-ish readers directly on tiny synthetic files: no gdptools/
geopandas involved, safe to run anywhere (see CLAUDE.md HPC head-node gate).
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import xarray as xr

from scripts.derive_snarea_curve import cells_from_weights, read_daily_by_hru  # noqa: E402


def test_cells_from_weights(tmp_path):
    wf = tmp_path / "w.csv"
    pd.DataFrame({"hru_id": [1, 1, 1, 2], "wght": [.1, .2, .3, .9]}).to_csv(wf, index=False)
    assert cells_from_weights(wf, "hru_id") == {1: 3, 2: 1}


def test_read_daily_by_hru(tmp_path):
    idx = pd.date_range("2010-02-01", periods=3, freq="D")
    ds = xr.Dataset(
        {"swe": (("time", "hru_id"), np.array([[10, 5], [8, 4], [0, 0]], "float64")),
         "scov": (("time", "hru_id"), np.array([[1, 1], [.8, .5], [0, 0]], "float64"))},
        coords={"time": idx, "hru_id": [1, 2]},
    )
    ds.to_netcdf(tmp_path / "snodas_agg_2010.nc")
    out = read_daily_by_hru(tmp_path, "hru_id")
    assert set(out) == {1, 2}
    assert list(out[1]["swe"].values) == [10, 8, 0]
    assert "sca" in out[1].columns          # scov renamed to sca

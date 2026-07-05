import numpy as np
import xarray as xr

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

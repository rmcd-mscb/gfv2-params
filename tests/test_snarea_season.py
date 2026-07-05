import numpy as np
import pandas as pd

from gfv2_params.snarea.season import (
    SWE_LEVELS,
    annual_sdc,
    melt_season,
    remove_reversals,
)


def _series(vals):
    idx = pd.date_range("2010-02-01", periods=len(vals), freq="D")
    return pd.Series(vals, index=idx, dtype="float64")


def test_swe_levels():
    assert list(SWE_LEVELS) == [1.0, 0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]


def test_melt_season_trims_to_peak_and_first_zero():
    swe = _series([1, 3, 10, 6, 2, 0, 0, 4])   # peak at idx2, zero at idx5
    sca = _series([0.2, .5, 1, .8, .4, 0, 0, .6])
    ms = melt_season(swe, sca)
    assert ms is not None
    swe_m, _ = ms
    assert swe_m.iloc[0] == 10 and swe_m.iloc[-1] == 0
    assert len(swe_m) == 4                      # idx2..idx5


def test_melt_season_none_when_no_snow():
    assert melt_season(_series([0, 0, 0]), _series([0, 0, 0])) is None


def test_remove_reversals_enforces_monotonic_sca():
    # SCA dips then rises (snowfall reversal) then falls again
    swe = _series([10, 8, 9, 5, 0])
    sca = _series([1.0, 0.6, 0.9, 0.4, 0.0])
    _, sca_r = remove_reversals(swe, sca)
    # kept SCA must be non-increasing
    assert list(sca_r.values) == sorted(sca_r.values, reverse=True)


def test_annual_sdc_shape_and_endpoints():
    swe = _series([10, 8, 6, 4, 2, 0])
    sca = _series([1.0, 0.9, 0.7, 0.5, 0.3, 0.0])
    curve = annual_sdc(swe, sca)
    assert curve is not None and curve.shape == (11,)
    assert curve[0] == 1.0                        # at swe_n=1 -> sca_n=1
    np.testing.assert_allclose(curve[-1], 0.0, atol=1e-9)  # at swe_n=0 -> ~0
    assert np.all(np.diff(curve) <= 1e-9)         # non-increasing across levels


def test_annual_sdc_none_when_sca_zero_at_peak():
    # mean SWE peaks but SCA at peak is 0 -> unusable season, not a flat-zero curve
    swe = _series([10, 6, 2, 0])
    sca = _series([0.0, 0.0, 0.0, 0.0])
    assert annual_sdc(swe, sca) is None


def test_annual_sdc_after_reversal_is_monotonic():
    # a post-peak snowfall reversal must not produce an increasing curve
    swe = _series([10, 8, 9, 5, 0])
    sca = _series([1.0, 0.6, 0.9, 0.4, 0.0])
    curve = annual_sdc(swe, sca)
    assert curve is not None and curve.shape == (11,)
    assert np.all(np.diff(curve) <= 1e-9)

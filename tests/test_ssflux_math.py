"""Unit tests for the pure ssflux maths (no geo fixtures, no data root)."""

import numpy as np
import pandas as pd
import pytest

from gfv2_params.zonal_runners.ssflux_math import (
    FFLUX_NO_OVERLAP,
    PARAM_NAMES,
    aggregate_k_perm_log,
    derive_log_params,
    interpolate_to_range,
    validate_weight_coverage,
)

ID = "nat_hru_id"


def _weights(rows):
    return pd.DataFrame(rows, columns=[ID, "k_perm", "normalized_area_weight"])


def test_intensive_aggregation_is_area_weighted_mean():
    """(sum v*a)/(sum a) -- gdptools INTENSIVE form, not the extensive sum."""
    w = _weights([(1, -10.0, 0.25), (1, -14.0, 0.75)])
    out = aggregate_k_perm_log(w, ID)
    assert out.loc[out[ID] == 1, "k_perm_log_wtd"].iloc[0] == pytest.approx(-13.0)


def test_weights_are_renormalised_when_coverage_is_partial():
    """A gap (weights summing to 0.5) must not halve the mean."""
    w = _weights([(1, -10.0, 0.25), (1, -14.0, 0.25)])
    out = aggregate_k_perm_log(w, ID)
    assert out.loc[out[ID] == 1, "k_perm_log_wtd"].iloc[0] == pytest.approx(-12.0)
    assert out.loc[out[ID] == 1, "fflux"].iloc[0] == pytest.approx(0.5)


def test_nodata_k_perm_is_excluded_not_floored():
    """k_perm == 0 is a no-data flag; it must not enter the mean."""
    w = _weights([(1, -10.0, 0.5), (1, 0.0, 0.5)])
    out = aggregate_k_perm_log(w, ID)
    assert out.loc[out[ID] == 1, "k_perm_log_wtd"].iloc[0] == pytest.approx(-10.0)
    assert out.loc[out[ID] == 1, "fflux"].iloc[0] == pytest.approx(0.5)


def test_hru_with_no_valid_lithology_is_nan_with_sentinel_fflux():
    w = _weights([(1, 0.0, 1.0)])
    out = aggregate_k_perm_log(w, ID)
    assert np.isnan(out.loc[out[ID] == 1, "k_perm_log_wtd"].iloc[0])
    assert out.loc[out[ID] == 1, "fflux"].iloc[0] == FFLUX_NO_OVERLAP


def test_aggregation_rejects_missing_weight_column():
    w = _weights([(1, -10.0, 0.5)]).rename(columns={"normalized_area_weight": "wght"})
    with pytest.raises(ValueError, match="normalized_area_weight"):
        aggregate_k_perm_log(w, ID)


def test_derive_log_params_matches_closed_forms():
    k = np.array([-12.0])
    slope = np.array([0.25])
    area = np.array([100.0])
    out = derive_log_params(k, slope, area)
    assert out["soil2gw_max"][0] == pytest.approx(-36.0)              # 3*k
    assert out["ssr2gw_rate"][0] == pytest.approx(-12.0 + np.log10(0.75))
    expected_slow = -12.0 + np.log10(0.25) - np.log10(100.0)
    assert out["slowcoef_lin"][0] == pytest.approx(expected_slow)
    assert out["fastcoef_lin"][0] == pytest.approx(expected_slow + np.log10(2))
    assert out["gwflow_coef"][0] == pytest.approx(expected_slow)
    assert out["dprst_seep_rate_open"][0] == pytest.approx(out["ssr2gw_rate"][0])
    assert out["dprst_flow_coef"][0] == pytest.approx(out["fastcoef_lin"][0])
    assert set(out) == set(PARAM_NAMES)


def test_slope_guards_produce_finite_output():
    """slope == 0 and slope >= 1 must not yield -inf or NaN."""
    k = np.array([-12.0, -12.0, -12.0])
    slope = np.array([0.0, 1.0, 2.34])
    area = np.array([100.0, 100.0, 100.0])
    out = derive_log_params(k, slope, area)
    for name in PARAM_NAMES:
        assert np.all(np.isfinite(out[name])), name


def test_nan_k_perm_propagates_as_nan_not_an_error():
    out = derive_log_params(np.array([np.nan]), np.array([0.5]), np.array([100.0]))
    assert np.isnan(out["soil2gw_max"][0])


def test_derive_rejects_non_positive_area():
    with pytest.raises(ValueError, match="hru_area"):
        derive_log_params(np.array([-12.0]), np.array([0.5]), np.array([0.0]))


def test_interpolate_maps_endpoints_and_is_affine_invariant():
    v = np.array([-3.0, -2.0, -1.0])
    out = interpolate_to_range(v, 0.1, 0.3)
    assert out[0] == pytest.approx(0.1)
    assert out[-1] == pytest.approx(0.3)
    # the cube is a scale on the exponent -> absorbed by min-max normalisation
    assert interpolate_to_range(3 * v, 0.1, 0.3) == pytest.approx(out, abs=1e-12)


def test_interpolate_ignores_nan_but_preserves_position():
    v = np.array([-3.0, np.nan, -1.0])
    out = interpolate_to_range(v, 0.0, 1.0)
    assert out[0] == pytest.approx(0.0)
    assert np.isnan(out[1])
    assert out[2] == pytest.approx(1.0)


def test_interpolate_raises_on_degenerate_range():
    with pytest.raises(ValueError, match="degenerate"):
        interpolate_to_range(np.array([2.0, 2.0, 2.0]), 0.0, 1.0)


def test_extensive_form_is_not_silently_accepted():
    """A frame carrying ONLY the extensive columns must raise, not guess.

    Guards the #175 root cause: dividing by the source-polygon area is gdptools'
    EXTENSIVE form and must never be reachable again by accident.
    """
    w = pd.DataFrame(
        {ID: [1, 1], "k_perm": [-10.0, -14.0],
         "area_weight": [100.0, 300.0], "flux_id_area": [400.0, 1200.0]}
    )
    with pytest.raises(ValueError, match="extensive"):
        aggregate_k_perm_log(w, ID)


class _CapturingLogger:
    def __init__(self): self.warnings = []
    def info(self, *a, **k): pass
    def debug(self, *a, **k): pass
    def warning(self, msg, *a): self.warnings.append(msg % a if a else msg)


def test_weight_coverage_passes_when_weights_sum_to_one():
    w = _weights([(1, -10.0, 0.5), (1, -12.0, 0.5), (2, -11.0, 1.0)])
    log = _CapturingLogger()
    validate_weight_coverage(w, ID, log)
    assert log.warnings == []


def test_weight_coverage_warns_on_gaps_and_overlaps():
    w = _weights([(1, -10.0, 0.4), (2, -11.0, 2.0), (3, -12.0, 1.0)])
    log = _CapturingLogger()
    validate_weight_coverage(w, ID, log, max_bad_fraction=0.9)
    assert any("outside" in m for m in log.warnings)


def test_weight_coverage_raises_when_too_many_are_bad():
    """A wholesale deviation from 1.0 means the wrong weight column."""
    w = _weights([(1, -10.0, 0.02), (2, -11.0, 0.03), (3, -12.0, 0.01)])
    with pytest.raises(ValueError, match="normalized_area_weight"):
        validate_weight_coverage(w, ID, _CapturingLogger())

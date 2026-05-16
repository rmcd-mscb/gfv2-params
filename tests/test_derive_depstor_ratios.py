"""Tests for compute_ratio in gfv2_params.depstor_ratios."""

import numpy as np
import pandas as pd

from gfv2_params.depstor_ratios import compute_ratio


def _df(hru_ids, counts, id_col="nat_hru_id", count_col="count"):
    return pd.DataFrame({id_col: hru_ids, count_col: counts})


def test_basic_ratio_no_clamp():
    num = _df([1, 2, 3], [10.0, 5.0, 0.0])
    den = _df([1, 2, 3], [20.0, 10.0, 5.0])
    out, stats = compute_ratio(num, den, "nat_hru_id", "count", "p", clamp_to_one=False)
    np.testing.assert_array_equal(out["nat_hru_id"].to_numpy(), [1, 2, 3])
    np.testing.assert_allclose(out["p"].to_numpy(), [0.5, 0.5, 0.0])
    assert stats["n_total"] == 3
    assert stats["n_zero_denom"] == 0
    assert stats["n_clamped"] == 0


def test_divide_by_zero_becomes_zero():
    num = _df([1, 2], [5.0, 0.0])
    den = _df([1, 2], [0.0, 0.0])  # both denominators are zero
    out, stats = compute_ratio(num, den, "nat_hru_id", "count", "p", clamp_to_one=False)
    np.testing.assert_allclose(out["p"].to_numpy(), [0.0, 0.0])
    assert stats["n_zero_denom"] == 2


def test_nan_inputs_become_zero():
    num = _df([1, 2], [np.nan, 5.0])
    den = _df([1, 2], [10.0, np.nan])
    out, stats = compute_ratio(num, den, "nat_hru_id", "count", "p", clamp_to_one=False)
    np.testing.assert_allclose(out["p"].to_numpy(), [0.0, 0.0])
    assert stats["n_zero_denom"] == 1


def test_clamp_to_one_applied():
    """getCarea outputs are clamped at 1.0 in the ArcPy reference."""
    num = _df([1, 2, 3], [10.0, 8.0, 12.0])
    den = _df([1, 2, 3], [5.0, 8.0, 6.0])  # ratios: 2.0, 1.0, 2.0
    out, stats = compute_ratio(num, den, "nat_hru_id", "count", "carea", clamp_to_one=True)
    np.testing.assert_allclose(out["carea"].to_numpy(), [1.0, 1.0, 1.0])
    assert stats["n_clamped"] == 2


def test_clamp_to_one_not_applied_for_sro_to_dprst():
    """sro_to_dprst_* ratios should NOT be clamped."""
    num = _df([1, 2], [10.0, 3.0])
    den = _df([1, 2], [5.0, 6.0])
    out, stats = compute_ratio(num, den, "nat_hru_id", "count", "p", clamp_to_one=False)
    np.testing.assert_allclose(out["p"].to_numpy(), [2.0, 0.5])
    assert stats["n_clamped"] == 0


def test_outer_join_fills_missing_hrus_with_zero():
    """If one CSV is missing an HRU the other has, treat the missing side as 0."""
    num = _df([1, 2, 3], [10.0, 20.0, 30.0])
    den = _df([2, 3, 4], [10.0, 10.0, 10.0])
    out, stats = compute_ratio(num, den, "nat_hru_id", "count", "p", clamp_to_one=False)
    np.testing.assert_array_equal(out["nat_hru_id"].to_numpy(), [1, 2, 3, 4])
    np.testing.assert_allclose(out["p"].to_numpy(), [0.0, 2.0, 3.0, 0.0])
    assert stats["n_zero_denom"] == 1


def test_output_columns_are_id_and_param_only():
    num = _df([1], [5.0])
    den = _df([1], [10.0])
    out, _ = compute_ratio(num, den, "nat_hru_id", "count", "my_param", clamp_to_one=False)
    assert list(out.columns) == ["nat_hru_id", "my_param"]


def test_output_sorted_by_id():
    num = _df([3, 1, 2], [9.0, 3.0, 6.0])
    den = _df([3, 1, 2], [3.0, 3.0, 3.0])
    out, _ = compute_ratio(num, den, "nat_hru_id", "count", "p", clamp_to_one=False)
    np.testing.assert_array_equal(out["nat_hru_id"].to_numpy(), [1, 2, 3])

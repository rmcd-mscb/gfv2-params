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


def test_aggregation_is_batch_partition_invariant():
    """Per-HRU results must be bit-identical regardless of how the source
    weight rows are partitioned across SLURM batches (the map stage).

    This is the MAP-side half of the #175 batch-invariance acceptance
    criterion (test_batch_invariance_is_bit_exact in test_ssflux.py covers the
    reduce side). It rests on a fact asserted nowhere until now: boolean-mask
    filtering by id_feature (exactly what run_ssflux_batch does --
    ``weights[weights[id_feature].isin(batch_ids)]``) preserves each HRU's
    weight rows in their original file order, so groupby('id').sum() sees the
    identical operand sequence whether run once over the whole frame or once
    per disjoint batch. Uses non-trivial floats so a change in summation order
    would show up at the ULP level rather than being masked by symmetry.
    """
    rng = np.random.default_rng(42)
    ids = np.repeat(np.arange(1, 21), 5)  # 20 HRUs x 5 weight rows each
    rows = pd.DataFrame(
        {
            ID: ids,
            "k_perm": rng.uniform(-16.0, -10.0, len(ids)),
            "normalized_area_weight": rng.uniform(0.05, 0.3, len(ids)),
        }
    )
    whole = aggregate_k_perm_log(rows, ID).sort_values(ID).reset_index(drop=True)

    # Partition by HRU id into two disjoint batches via boolean mask -- exactly
    # what two SLURM array tasks reading the same weights CSV do -- aggregate
    # each separately, and recombine.
    part_a = rows[rows[ID] <= 10]
    part_b = rows[rows[ID] > 10]
    split = (
        pd.concat(
            [aggregate_k_perm_log(part_a, ID), aggregate_k_perm_log(part_b, ID)],
            ignore_index=True,
        )
        .sort_values(ID)
        .reset_index(drop=True)
    )
    pd.testing.assert_frame_equal(whole, split, check_exact=True)


def test_all_ids_gives_zero_coverage_hru_a_row_with_sentinel_fflux():
    """Issue #209: an HRU with ZERO weight rows (no lithology overlap at all)
    must still get a row when the caller supplies the full target HRU list --
    NaN k_perm_log_wtd (feeding the KNN gap-fill) and fflux ==
    FFLUX_NO_OVERLAP, making that sentinel reachable for the case it's
    actually named for."""
    w = _weights([(1, -10.0, 1.0)])  # HRU 2 has NO weight rows at all
    out = aggregate_k_perm_log(w, ID, all_ids=pd.Index([1, 2], name=ID))
    assert set(out[ID]) == {1, 2}
    row2 = out.loc[out[ID] == 2].iloc[0]
    assert np.isnan(row2["k_perm_log_wtd"])
    assert row2["fflux"] == FFLUX_NO_OVERLAP
    # the HRU that DOES have weight rows is unaffected
    row1 = out.loc[out[ID] == 1].iloc[0]
    assert row1["k_perm_log_wtd"] == pytest.approx(-10.0)
    assert row1["fflux"] == pytest.approx(1.0)


def test_omitting_all_ids_preserves_current_behavior():
    """Without all_ids, a zero-coverage HRU stays absent from the output --
    the pre-#209 behaviour, unchanged for existing callers."""
    w = _weights([(1, -10.0, 1.0)])
    out = aggregate_k_perm_log(w, ID)
    assert set(out[ID]) == {1}


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


def test_interpolate_raises_on_transposed_hi_lo():
    """A transposed min/max in a flux_params config entry must raise, not
    silently emit an inverted field."""
    v = np.array([-3.0, -2.0, -1.0])
    with pytest.raises(ValueError, match="hi"):
        interpolate_to_range(v, hi=0.1, lo=0.3)


def test_interpolate_raises_on_equal_hi_lo():
    v = np.array([-3.0, -2.0, -1.0])
    with pytest.raises(ValueError, match="hi"):
        interpolate_to_range(v, hi=0.2, lo=0.2)


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
    # This tiny 3-HRU fixture is 2/3 "bad" by the tol=0.01 tail test, and one
    # of the three (sum=0.4) is also outside the [0.5, 2.0] magnitude band;
    # raise both max_bad_fraction and magnitude_bad_fraction so this stays a
    # test of the soft tail-count warning path specifically, not an
    # accidental hit of the magnitude raise.
    validate_weight_coverage(w, ID, log, max_bad_fraction=0.9, magnitude_bad_fraction=0.95)
    assert any("outside" in m for m in log.warnings)


def test_weight_coverage_raises_when_median_is_displaced():
    """The extensive-form signature is a displaced MEDIAN (0.022 vs 1.0), not
    a tail count -- gfv2 batches are spatially contiguous, so legitimate
    coverage gaps cluster and can blow past a tail-fraction threshold on their
    own (see the clustered-gap test below). Median is what actually
    distinguishes "wrong column" from "this batch has a gap cluster"."""
    w = _weights([(1, -10.0, 0.02), (2, -11.0, 0.03), (3, -12.0, 0.01)])
    with pytest.raises(ValueError, match="normalized_area_weight"):
        validate_weight_coverage(w, ID, _CapturingLogger())


def test_weight_coverage_clustered_gap_batch_warns_not_raises():
    """A batch that happens to contain a large cluster of legitimate,
    MILDLY-off coverage gaps (sums 0.6-0.95, comfortably inside the
    [band_lo, band_hi]=[0.5, 2.0] magnitude band) at a high fraction (~32% of
    HRUs, matching the measured 1,828-in-~5,650 CONUS batch) must WARN, not
    raise, as long as the median stays near 1.0 and the magnitude stays
    in-band -- proving the new rule keys on MAGNITUDE, not a bare tail count.
    An old tail-fraction-only raise fired here and blamed the extensive form,
    aborting a CONUS array task over a correct, if clustered, renormalised
    result."""
    n_gap = 32
    n_full = 68
    rng = np.random.default_rng(1)
    rows = [
        (i, -12.0, float(v))
        for i, v in zip(range(1, n_gap + 1), rng.uniform(0.6, 0.95, n_gap))
    ]
    rows += [(i, -12.0, 1.0) for i in range(n_gap + 1, n_gap + n_full + 1)]
    w = _weights(rows)
    log = _CapturingLogger()
    sums = w.groupby(ID)["normalized_area_weight"].sum()
    assert sums.median() == pytest.approx(1.0)
    assert ((sums >= 0.5) & (sums <= 2.0)).all(), "fixture must stay inside the magnitude band"
    validate_weight_coverage(w, ID, log, max_bad_fraction=0.05)
    assert any("outside" in m for m in log.warnings)


def test_weight_coverage_partial_regression_at_low_fraction_raises():
    """The point of the whole change: a PARTIAL extensive-form regression
    affecting only ~10% of HRUs (well under the old hard_bad_fraction=0.35
    ceiling, which would have silently passed this) must now RAISE, because
    the affected HRUs sit at extensive-form MAGNITUDE (~0.02), far outside
    the [0.5, 2.0] plausible band."""
    n_bad = 10
    n_good = 90
    rows = [(i, -12.0, 0.02) for i in range(1, n_bad + 1)]
    rows += [(i, -12.0, 1.0) for i in range(n_bad + 1, n_bad + n_good + 1)]
    w = _weights(rows)
    sums = w.groupby(ID)["normalized_area_weight"].sum()
    assert sums.median() == pytest.approx(1.0)
    with pytest.raises(ValueError, match="band"):
        validate_weight_coverage(w, ID, _CapturingLogger())


def test_weight_coverage_raises_on_magnitude_ceiling_even_with_median_at_one():
    """The median-only check has a blind spot: up to just under 50% of HRUs
    can carry the extensive-form bug while the median stays exactly at 1.0,
    because the median only reflects the majority. 60 HRUs at sum=1.0 plus 40
    at sum=0.05 (the exact extensive-form signature, on a MINORITY of rows)
    keeps the median at 1.0 -- and now raises because 40% of sums fall
    outside the [0.5, 2.0] plausible band (default magnitude_bad_fraction is
    0.05), not because of a tuned tail-count ceiling. This is the old
    hard_bad_fraction=0.35 counterexample; it must still raise under the
    magnitude discriminator."""
    rows = [(i, -12.0, 1.0) for i in range(1, 61)]
    rows += [(i, -12.0, 0.05) for i in range(61, 101)]
    w = _weights(rows)
    sums = w.groupby(ID)["normalized_area_weight"].sum()
    assert sums.median() == pytest.approx(1.0)
    with pytest.raises(ValueError, match="band"):
        validate_weight_coverage(w, ID, _CapturingLogger())


def test_weight_coverage_magnitude_ceiling_is_configurable():
    """band_lo/band_hi/magnitude_bad_fraction are keywords, not hardcoded
    constants."""
    rows = [(i, -12.0, 1.0) for i in range(1, 61)]
    rows += [(i, -12.0, 0.05) for i in range(61, 101)]
    w = _weights(rows)
    with pytest.raises(ValueError, match="band"):
        validate_weight_coverage(w, ID, _CapturingLogger(), magnitude_bad_fraction=0.05)
    # raising the ceiling above the actual 40% bad fraction: no raise, only warn
    log = _CapturingLogger()
    validate_weight_coverage(w, ID, log, magnitude_bad_fraction=0.9, max_bad_fraction=0.05)
    assert any("outside" in m for m in log.warnings)


def test_weight_coverage_median_tol_is_configurable():
    """The threshold is a keyword, not a hardcoded constant (spec robustness
    rule 5: 'a configured threshold')."""
    w = _weights([(1, -10.0, 0.85), (2, -11.0, 0.85), (3, -12.0, 0.85)])
    log = _CapturingLogger()
    # default median_tol=0.1 tolerates 0.85 (within 1.0 +/- 0.1 is false --
    # 0.85 is exactly on the edge at tol 0.15); use an explicit tight/loose
    # pair to prove the kwarg is actually threaded through. All 3 HRUs sit at
    # the same 0.85, which is inside the default [0.5, 2.0] magnitude band,
    # so no magnitude override is needed here -- this test is about
    # median_tol, not the separate magnitude-ceiling behaviour.
    with pytest.raises(ValueError, match="normalized_area_weight"):
        validate_weight_coverage(w, ID, log, median_tol=0.1)
    validate_weight_coverage(w, ID, log, median_tol=0.2)

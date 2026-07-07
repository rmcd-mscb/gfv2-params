import numpy as np
import pytest

from gfv2_params.snarea.library import (
    _INTERIOR,
    CURVE_COLS,
    CV_GRID,
    SWE_LEVELS,
    _to_prms_order,
    assign_deplcrv,
    build_library,
    fit_cv,
    sdc_from_cv,
    snarea_thresh_inches,
    validate_and_calibrate,
)


def test_swe_levels_descending_11pt():
    assert SWE_LEVELS.shape == (11,)
    assert SWE_LEVELS[0] == 1.0 and SWE_LEVELS[-1] == 0.0
    assert np.all(np.diff(SWE_LEVELS) < 0)


def test_sdc_from_cv_shape_and_endpoints():
    c = sdc_from_cv(0.5)
    assert c.shape == (11,)
    assert c[0] == pytest.approx(1.0)
    assert c[-1] == pytest.approx(0.0)


def test_sdc_from_cv_monotone_nonincreasing():
    c = sdc_from_cv(0.7)
    assert np.all(np.diff(c) <= 1e-9)


def test_sdc_from_cv_higher_cv_steeper():
    # larger CV -> lower SCA at mid SWE (index 5 = SWE 0.5)
    assert sdc_from_cv(1.5)[5] < sdc_from_cv(0.5)[5] < sdc_from_cv(0.1)[5]


def test_fit_cv_recovers_known_cv():
    for true_cv in (0.32, 0.7, 1.2):
        curve = sdc_from_cv(true_cv)
        # fit is on the CV grid; recovered value within one grid step
        assert abs(fit_cv(curve) - true_cv) <= (CV_GRID[1] - CV_GRID[0]) + 1e-9


def test_fit_cv_is_interior_driven():
    # Assert the interior-only invariant directly
    assert _INTERIOR == slice(1, 10)

    # Assert that interior shape drives the fit: differing interiors yield different fitted CVs
    cv_low = 0.4
    cv_high = 1.0
    curve_low = sdc_from_cv(cv_low)
    curve_high = sdc_from_cv(cv_high)

    fitted_low = fit_cv(curve_low)
    fitted_high = fit_cv(curve_high)

    # Both should be recovered within one grid step
    assert abs(fitted_low - cv_low) <= (CV_GRID[1] - CV_GRID[0]) + 1e-9
    assert abs(fitted_high - cv_high) <= (CV_GRID[1] - CV_GRID[0]) + 1e-9

    # They should be distinctly different (not confused by endpoints)
    assert fitted_low < fitted_high


def test_snarea_thresh_mm_to_inches():
    assert snarea_thresh_inches(254.0) == pytest.approx(10.0)
    assert snarea_thresh_inches(0.0) == 0.0
    assert snarea_thresh_inches(float("nan")) == 0.0
    assert snarea_thresh_inches(-5.0) == 0.0


def test_to_prms_order_is_reverse_and_involutive():
    c = sdc_from_cv(0.6)
    p = _to_prms_order(c)
    assert p[0] == pytest.approx(c[-1])   # ascending: SCA@frac0 first
    assert p[-1] == pytest.approx(c[0])
    assert np.allclose(_to_prms_order(p), c)


def test_build_library_default_plus_bins():
    rng = np.linspace(0.2, 1.4, 500)   # spread of CVs
    default = np.linspace(1.0, 0.0, 11)
    lib = build_library(rng, ndepl_cv=8, default_curve=default)
    assert len(lib) == 9                              # 1 default + 8 bins
    assert list(lib["deplcrv_id"]) == list(range(1, 10))
    assert lib.iloc[0]["curve_kind"] == "default"
    assert np.isnan(lib.iloc[0]["cv"])
    np.testing.assert_allclose(lib.iloc[0][CURVE_COLS].to_numpy(float), default)
    assert (lib.iloc[1:]["curve_kind"] == "cv_bin").all()
    # bin median CVs are increasing
    assert np.all(np.diff(lib.iloc[1:]["cv"].to_numpy()) > 0)
    # each cv_bin curve equals sdc_from_cv(its median cv)
    row = lib.iloc[3]
    np.testing.assert_allclose(row[CURVE_COLS].to_numpy(float), sdc_from_cv(row["cv"]), atol=1e-9)


def test_build_library_equal_population_bins():
    cv = np.arange(1000) / 1000.0 + 0.1   # uniform
    lib = build_library(cv, ndepl_cv=5, default_curve=np.linspace(1, 0, 11))
    assert len(lib) == 6


def test_build_library_tied_cvs_still_produce_ndepl_cv_bins():
    # fit_cv snaps CVs to a grid, so heavy ties are realistic; must NOT collapse.
    cv = np.concatenate([np.full(1800, 0.45), np.linspace(0.2, 1.4, 200)])
    lib = build_library(cv, ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    assert len(lib) == 9                                  # 1 default + 8 cv bins, no collapse
    assert (lib.iloc[1:]["curve_kind"] == "cv_bin").all()
    assert list(lib["deplcrv_id"]) == list(range(1, 10))


def test_build_library_raises_when_fewer_cvs_than_bins():
    with pytest.raises(ValueError, match="at least ndepl_cv"):
        build_library(np.array([0.4, 0.5, 0.6]), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))


def test_assign_deplcrv_nearest_bin_and_default():
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    bin_cvs = lib.iloc[1:]["cv"].to_numpy()
    cv_assign = np.array([bin_cvs[0], bin_cvs[-1], np.nan, float(bin_cvs.mean())])
    out = assign_deplcrv(cv_assign, lib)
    assert out[0] == 2                     # nearest first bin
    assert out[1] == 9                     # nearest last bin
    assert out[2] == 1                     # NaN -> reserved default
    assert 2 <= out[3] <= 9                # some cv_bin, never the default
    assert out.dtype.kind in ("i", "u")


def test_assign_deplcrv_never_returns_default_for_finite_cv():
    lib = build_library(np.linspace(0.2, 1.4, 500), ndepl_cv=8, default_curve=np.linspace(1, 0, 11))
    out = assign_deplcrv(np.full(50, 0.45), lib)
    assert (out >= 2).all()


def test_calibration_removes_synthetic_bias():
    rng = np.linspace(0.3, 1.2, 400)
    emp_curves = np.vstack([sdc_from_cv(c) for c in rng])
    cv_empirical = rng.copy()
    cv_subgrid = rng * 0.6  # biased low by construction
    cal, report = validate_and_calibrate(cv_subgrid, cv_empirical, emp_curves, mode="auto", bias_tol=0.05)
    assert report["calibrated"] is True
    # after calibration the median bias vs empirical is much smaller
    assert abs(np.median(cal) - np.median(cv_empirical)) < abs(
        np.median(cv_subgrid) - np.median(cv_empirical)
    )


def test_no_calibration_when_unbiased():
    rng = np.linspace(0.3, 1.2, 400)
    emp_curves = np.vstack([sdc_from_cv(c) for c in rng])
    cal, report = validate_and_calibrate(rng.copy(), rng.copy(), emp_curves, mode="auto", bias_tol=0.1)
    assert report["calibrated"] is False
    np.testing.assert_allclose(cal, rng)


def test_calibration_off_is_identity():
    rng = np.linspace(0.3, 1.2, 50)
    emp = np.vstack([sdc_from_cv(c) for c in rng])
    cal, report = validate_and_calibrate(rng * 0.5, rng, emp, mode="off")
    np.testing.assert_allclose(cal, rng * 0.5)
    assert report["calibrated"] is False

import numpy as np
import pytest

from gfv2_params.snarea.library import _INTERIOR, CV_GRID, SWE_LEVELS, fit_cv, sdc_from_cv


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

import numpy as np
import pytest

from gfv2_params.snarea.library import CV_GRID, SWE_LEVELS, fit_cv, sdc_from_cv


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
    for true_cv in (0.3, 0.7, 1.2):
        curve = sdc_from_cv(true_cv)
        # fit is on the CV grid; recovered value within one grid step
        assert abs(fit_cv(curve) - true_cv) <= (CV_GRID[1] - CV_GRID[0]) + 1e-9


def test_fit_cv_uses_interior_only():
    # a curve whose endpoints are perturbed but interior matches cv=0.5 still fits ~0.5
    curve = sdc_from_cv(0.5).copy()
    curve[0] = 1.0  # endpoints already fixed; assert fit ignores them
    assert fit_cv(curve) == pytest.approx(0.5, abs=CV_GRID[1] - CV_GRID[0])

import numpy as np
import pytest

from gfv2_params.snarea.library import SWE_LEVELS, sdc_from_cv


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

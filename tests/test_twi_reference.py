"""Unit tests for the pure percentile / CDF-inversion helpers used to derive
TWI threshold cutoffs (issue #55 Stage 1)."""

import numpy as np
import pytest

from gfv2_params.shared_rasters.twi_reference import (
    percentile_of_values,
    rank_of_value,
)


def test_percentile_of_values_basic():
    vals = np.arange(1, 101, dtype="float64")  # 1..100
    p = percentile_of_values(vals, [75.0, 95.0])
    assert p[0] == pytest.approx(75.25, abs=0.5)
    assert p[1] == pytest.approx(95.05, abs=0.5)


def test_percentile_drops_nan_and_nodata():
    vals = np.array([1.0, 2.0, 3.0, 4.0, np.nan, -9999.0], dtype="float64")
    p = percentile_of_values(vals, [50.0], nodata=-9999.0)
    # median of {1,2,3,4} == 2.5
    assert p[0] == pytest.approx(2.5)


def test_rank_of_value_is_inverse_of_percentile():
    vals = np.arange(1, 101, dtype="float64")
    # value 75 sits at ~the 75th percentile
    assert rank_of_value(vals, 75.0) == pytest.approx(75.0, abs=1.0)


def test_rank_of_value_handles_nodata():
    vals = np.array([1.0, 2.0, 3.0, 4.0, -9999.0], dtype="float64")
    # 3.0 is >= 3 of 4 valid values -> 75%
    assert rank_of_value(vals, 3.0, nodata=-9999.0) == pytest.approx(75.0)


def test_percentile_empty_raises():
    with pytest.raises(ValueError, match="no valid"):
        percentile_of_values(np.array([-9999.0, np.nan]), [50.0], nodata=-9999.0)

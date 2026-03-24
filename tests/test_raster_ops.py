import math

from gfv2_params.raster_ops import deg_to_fraction


def test_deg_to_fraction_zero():
    assert deg_to_fraction(0.0) == 0.0


def test_deg_to_fraction_45():
    result = deg_to_fraction(45.0)
    assert math.isclose(result, 1.0, rel_tol=1e-9)


def test_deg_to_fraction_30():
    result = deg_to_fraction(30.0)
    expected = math.tan(math.radians(30.0))
    assert math.isclose(result, expected, rel_tol=1e-9)

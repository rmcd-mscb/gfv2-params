import numpy as np

from gfv2_params.snarea.representative import median_sdc, similarity, select_representative


def test_median_elementwise():
    annual = np.array([[0.0] * 11, [0.5] * 11, [1.0] * 11])
    np.testing.assert_allclose(median_sdc(annual), [0.5] * 11)


def test_similarity_zero_for_identical():
    med = np.linspace(1, 0, 11)
    annual = np.stack([med, med])
    assert similarity(annual, med) == 0.0


def test_similarity_positive_and_scaled_by_points():
    med = np.zeros(11)
    annual = np.array([np.ones(11)])       # each of 11 points off by 1
    # sum(|1-0|)=11 over 11 points/curve -> 11/11 = 1.0
    assert similarity(annual, med) == 1.0


def test_select_representative_picks_closest_year():
    med = np.linspace(1, 0, 11)
    far = np.zeros(11)
    near = med + 0.01
    annual = np.stack([far, near])
    rep = select_representative(annual, med)
    np.testing.assert_allclose(rep, near)

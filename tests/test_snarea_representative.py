import numpy as np

from gfv2_params.snarea.representative import median_sdc, select_representative, similarity


def test_median_elementwise():
    annual = np.array([[0.0] * 11, [0.5] * 11, [1.0] * 11])
    np.testing.assert_allclose(median_sdc(annual), [0.5] * 11)


def test_similarity_zero_for_identical():
    med = np.linspace(1, 0, 11)
    annual = np.stack([med, med])
    assert similarity(annual, med) == 0.0


def test_similarity_is_mean_per_point_deviation():
    med = np.zeros(11)
    annual = np.array([np.full(11, 0.1)])   # every point off by 0.1
    assert abs(similarity(annual, med) - 0.1) < 1e-12


def test_similarity_is_scale_free_in_n_seasons():
    # A uniform 0.1 per-point deviation must give 0.1 regardless of how many
    # seasons — the fix for the metric scaling with n_seasons (Oregon 2026-07-06).
    med = np.zeros(11)
    for n in (1, 3, 20):
        annual = np.full((n, 11), 0.1)
        assert abs(similarity(annual, med) - 0.1) < 1e-12


def test_select_representative_picks_closest_year():
    med = np.linspace(1, 0, 11)
    far = np.zeros(11)
    near = med + 0.01
    annual = np.stack([far, near])
    rep = select_representative(annual, med)
    np.testing.assert_allclose(rep, near)

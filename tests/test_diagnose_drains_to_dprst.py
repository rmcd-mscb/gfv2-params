import numpy as np

from scripts.diagnose_drains_to_dprst import vpu_coverage


def test_vpu_coverage_fraction_of_land():
    # VPU 1: 4 land cells, 2 drain -> 0.5. VPU 2: 2 land cells, 0 drain -> 0.0.
    vpu_id = np.array([[1, 1, 2], [1, 1, 2]], dtype=np.uint8)
    drains = np.array([[1, 1, 0], [0, 0, 0]], dtype=np.uint8)
    land = np.ones((2, 3), dtype=bool)
    cov = vpu_coverage(drains, vpu_id, land)
    assert cov[1] == 0.5
    assert cov[2] == 0.0


def test_vpu_coverage_ignores_non_land():
    vpu_id = np.array([[1, 1]], dtype=np.uint8)
    drains = np.array([[1, 1]], dtype=np.uint8)
    land = np.array([[True, False]])  # second cell is ocean
    cov = vpu_coverage(drains, vpu_id, land)
    assert cov[1] == 1.0  # only the 1 land cell counts, and it drains

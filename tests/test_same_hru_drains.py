import numpy as np
from gfv2_params.depstor import same_hru_intersect


def test_same_hru_intersect_keeps_only_matching_hru():
    labeled = np.array([[42, 42, 9, 0]], dtype=np.int32)   # reached-HRU per cell
    hru_id = np.array([[42, 8, 9, 5]], dtype=np.int32)     # cell's own HRU
    land = np.array([[1, 1, 1, 1]], dtype=np.uint8)         # perv everywhere
    out = same_hru_intersect(labeled, hru_id, land)
    # col0: 42==42 & perv -> 1 ; col1: 42!=8 -> 255 ; col2: 9==9 -> 1 ; col3: 0!=5 -> 255
    assert out.tolist() == [[1, 255, 1, 255]]

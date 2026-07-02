import numpy as np

from gfv2_params.d8_routing import drains_to_dprst_kernel
from gfv2_params.depstor import (
    assign_vpu_drains,
    mask_fdr_to_vpu,
    vpu_bbox,
    vpu_codes_present,
    vpu_pour_points,
)
from gfv2_params.depstor_builders import STEP_ORDER


def test_vpu_id_runs_before_routing():
    # routing tiles by vpu_id, so the partition must be built first.
    assert STEP_ORDER.index("vpu_id") < STEP_ORDER.index("routing")


def test_vpu_codes_present_excludes_nodata():
    v = np.array([[0, 1, 1], [2, 2, 0]], dtype=np.uint8)
    assert vpu_codes_present(v) == [1, 2]


def test_vpu_bbox_bounds_the_code_and_is_slice_ready():
    v = np.array(
        [[0, 0, 0, 0],
         [0, 1, 1, 0],
         [0, 1, 0, 0],
         [0, 0, 0, 0]], dtype=np.uint8)
    assert vpu_bbox(v, 1) == (1, 3, 1, 3)
    assert vpu_bbox(v, 9) is None


def test_mask_fdr_to_vpu_sets_outside_to_nodata():
    fdr = np.array([[1, 2], [4, 8]], dtype=np.uint8)
    vpu = np.array([[1, 2], [1, 1]], dtype=np.uint8)
    out = mask_fdr_to_vpu(fdr, vpu, code=1, nodata=255)
    assert out.tolist() == [[1, 255], [4, 8]]


def test_vpu_pour_points_only_this_vpu_depressions():
    dprst = np.array([[1, 1], [1, 255]], dtype=np.uint8)
    vpu = np.array([[1, 2], [1, 1]], dtype=np.uint8)
    out = vpu_pour_points(dprst, vpu, code=1)
    assert out.tolist() == [[1, 0], [1, 0]]


def test_assign_vpu_drains_isolates_by_vpu_even_with_overlapping_bbox():
    vpu = np.array([[1, 1, 1], [0, 2, 0], [1, 1, 1]], dtype=np.uint8)
    drains = np.full((3, 3), np.uint8(255), dtype=np.uint8)

    b1 = vpu_bbox(vpu, 1)
    ws1 = np.ones((3, 3), dtype=np.int32)
    assign_vpu_drains(drains, vpu, 1, b1, ws1, ws_nodata=0)
    assert drains[1, 1] == 255
    assert (drains[0, :] == 1).all() and (drains[2, :] == 1).all()
    assert drains[1, 0] == 255 and drains[1, 2] == 255

    b2 = vpu_bbox(vpu, 2)
    ws2 = np.ones((1, 1), dtype=np.int32)
    assign_vpu_drains(drains, vpu, 2, b2, ws2, ws_nodata=0)
    assert drains[1, 1] == 1


def test_assign_vpu_drains_unlabelled_cells_stay_nodata():
    vpu = np.array([[1, 1]], dtype=np.uint8)
    drains = np.full((1, 2), np.uint8(255), dtype=np.uint8)
    ws = np.array([[5, 0]], dtype=np.int32)
    assign_vpu_drains(drains, vpu, 1, (0, 1, 0, 2), ws, ws_nodata=0)
    assert drains.tolist() == [[1, 255]]


def test_assign_vpu_drains_all_nodata_marks_nothing():
    # A VPU with no pour-points -> WBT fills the output with its nodata sentinel
    # -> nothing should be marked as draining (the zero-pour-points edge case).
    vpu = np.array([[1, 1], [1, 1]], dtype=np.uint8)
    drains = np.full((2, 2), np.uint8(255), dtype=np.uint8)
    ws = np.full((2, 2), -32768, dtype=np.int32)
    assign_vpu_drains(drains, vpu, 1, (0, 2, 0, 2), ws, ws_nodata=-32768)
    assert (drains == 255).all()


def test_onstream_barrier_blocks_drainage_at_helper_level():
    # Single VPU (code 1). Row flows East into a dprst pour at the right end,
    # with an on-stream waterbody cell in the middle acting as a barrier.
    #   land -> land -> [onstream] -> [dprst]
    vpu = np.ones((1, 4), dtype=np.uint8)
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    dprst = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    onstream = np.array([[0, 0, 1, 0]], dtype=np.uint8)

    pour = vpu_pour_points(dprst, vpu, code=1)
    barrier = vpu_pour_points(onstream, vpu, code=1)  # reused: mask ∩ VPU
    out, _ = drains_to_dprst_kernel(fdr, pour, barrier)

    assert out.tolist() == [[0, 0, 0, 1]]  # upslope land blocked by on-stream cell

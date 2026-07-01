import numpy as np

from gfv2_params.d8_routing import drains_to_dprst_kernel

# ESRI D8 codes used in the fixtures:
#   1=E  2=SE  4=S  8=SW  16=W  32=NW  64=N  128=NE   255=nodata/sink
#
# drains_to_dprst_kernel returns (out, n_cycles); tests unpack both.


def test_pour_point_itself_drains():
    # A lone pour point with no inflow still counts as draining.
    fdr = np.array([[255]], dtype=np.uint8)
    pour = np.array([[1]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))
    assert out.tolist() == [[1]]
    assert n_cycles == 0


def test_straight_chain_into_pour_point():
    # Row of cells all flowing East (code 1) into a pour point at the right end.
    # cells:  ->  ->  ->  [pour]
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    pour = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))
    # every upstream cell reaches the pour point
    assert out.tolist() == [[1, 1, 1, 1]]
    assert n_cycles == 0


def test_chain_draining_away_is_not_marked():
    # Cells flow West (code 16) away from the only pour point on the right.
    # The pour point drains (itself); nothing upstream of it exists.
    fdr = np.array([[16, 16, 16, 255]], dtype=np.uint8)
    pour = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))
    # cells 0..2 flow further West off-grid -> do not reach the pour point
    assert out.tolist() == [[0, 0, 0, 1]]
    assert n_cycles == 0


def test_two_cell_cycle_with_no_pour_terminates_and_marks_zero():
    # Regression for the WBT hang: two cells point at each other.
    # left flows East (1) into right; right flows West (16) into left.
    fdr = np.array([[1, 16]], dtype=np.uint8)
    pour = np.array([[0, 0]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))  # must return, not hang
    assert out.tolist() == [[0, 0]]
    assert n_cycles >= 1  # the cycle is detected and counted


def test_four_cell_cycle_with_no_pour_terminates_and_marks_zero():
    # 2x2 rotational cycle: (0,0)->E->(0,1)->S->(1,1)->W->(1,0)->N->(0,0)
    fdr = np.array([[1, 4],
                    [64, 16]], dtype=np.uint8)
    pour = np.zeros((2, 2), dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))  # must return, not hang
    assert out.tolist() == [[0, 0], [0, 0]]
    assert n_cycles >= 1


def test_cell_upstream_of_cycle_not_marked():
    # A feeder cell flows into a closed cycle that never reaches a pour point.
    # layout (1 row, 3 cols): feeder(E) -> A(E) -> B(W back to A)
    #   col0 -> col1 -> col2, col2 -> col1  => cycle between col1 and col2
    fdr = np.array([[1, 1, 16]], dtype=np.uint8)
    pour = np.array([[0, 0, 0]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))  # must return, not hang
    assert out.tolist() == [[0, 0, 0]]
    assert n_cycles >= 1


def test_cycle_containing_pour_point_marks_drains():
    # A pour point sits inside a 2-cell cycle. Seeding pre-marks pour points
    # _DRAINS, so the traversal exits via the _DRAINS branch when it reaches the
    # pour cell -- NOT via the cycle guard. This pins the seeding-order invariant
    # (a regression that seeds at write-time instead would mis-mark these _NOT).
    # left(E->right) and right(W->left) form a cycle; left is the pour point.
    fdr = np.array([[1, 16]], dtype=np.uint8)
    pour = np.array([[1, 0]], dtype=np.uint8)  # col 0 is the pour point
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))
    # col 0 is the pour point => drains; col 1 flows into it => drains too
    assert out.tolist() == [[1, 1]]
    assert n_cycles == 0  # the pour point breaks the cycle before it is entered


def test_nodata_sink_does_not_drain():
    # Single non-pour sink cell.
    fdr = np.array([[255]], dtype=np.uint8)
    pour = np.array([[0]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))
    assert out.tolist() == [[0]]
    assert n_cycles == 0


def test_branching_tributaries_all_reach_pour():
    # Two tributaries merge then flow into a pour point.
    #   (0,0) SE(2) ->(1,1)
    #   (0,2) SW(8) ->(1,1)
    #   (1,1) S(4)  ->(2,1) = pour
    fdr = np.array([[2, 255, 8],
                    [255, 4, 255],
                    [255, 255, 255]], dtype=np.uint8)
    pour = np.zeros((3, 3), dtype=np.uint8)
    pour[2, 1] = 1
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))
    assert out[0, 0] == 1   # NW tributary
    assert out[0, 2] == 1   # NE tributary
    assert out[1, 1] == 1   # confluence
    assert out[2, 1] == 1   # pour point
    assert out[0, 1] == 0   # untouched nodata cell
    assert n_cycles == 0


def test_all_eight_directions_decode_into_a_central_pour():
    # Each surrounding cell points at the central pour point, exercising every
    # ESRI decode branch on an acyclic draining path. Neighbour -> code that
    # lands on (1,1):
    #   (0,0)->SE(2)  (0,1)->S(4)   (0,2)->SW(8)
    #   (1,0)->E(1)   (1,1)=pour    (1,2)->W(16)
    #   (2,0)->NE(128)(2,1)->N(64)  (2,2)->NW(32)
    fdr = np.array([[2, 4, 8],
                    [1, 255, 16],
                    [128, 64, 32]], dtype=np.uint8)
    pour = np.zeros((3, 3), dtype=np.uint8)
    pour[1, 1] = 1
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))
    assert out.tolist() == [[1, 1, 1], [1, 1, 1], [1, 1, 1]]
    assert n_cycles == 0


def test_off_window_flow_does_not_drain():
    # A cell flowing North off the top edge terminates as does-not-drain.
    fdr = np.array([[64]], dtype=np.uint8)
    pour = np.array([[0]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour))
    assert out.tolist() == [[0]]
    assert n_cycles == 0


def test_custom_nodata_value_terminates():
    # fdr_nodata is configurable; 0 here marks the sink.
    fdr = np.array([[1, 0]], dtype=np.uint8)
    pour = np.array([[0, 0]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, np.zeros_like(pour), fdr_nodata=0)
    assert out.tolist() == [[0, 0]]
    assert n_cycles == 0


def test_barrier_blocks_upslope_from_pour():
    # Row flowing East into a pour point at the right end, with a barrier in
    # the middle:  cell0 ->  cell1 -> [barrier] -> [pour]
    # cell0/cell1 hit the barrier before the pour => not draining.
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    pour = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    barrier = np.array([[0, 0, 1, 0]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, barrier)
    # cell0, cell1 blocked; barrier itself non-draining; pour drains itself.
    assert out.tolist() == [[0, 0, 0, 1]]
    assert n_cycles == 0


def test_barrier_downstream_of_pour_does_not_unmark():
    # Path reaches the pour BEFORE the barrier: first-waterbody-wins => drains.
    # cell0 -> [pour] -> [barrier]
    fdr = np.array([[1, 1, 255]], dtype=np.uint8)
    pour = np.array([[0, 1, 0]], dtype=np.uint8)
    barrier = np.array([[0, 0, 1]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, barrier)
    assert out.tolist() == [[1, 1, 0]]
    assert n_cycles == 0


def test_no_barrier_is_equivalent_to_old_behavior():
    # An all-zero barrier reproduces the pre-barrier straight-chain result.
    fdr = np.array([[1, 1, 1, 255]], dtype=np.uint8)
    pour = np.array([[0, 0, 0, 1]], dtype=np.uint8)
    barrier = np.zeros_like(pour)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, barrier)
    assert out.tolist() == [[1, 1, 1, 1]]
    assert n_cycles == 0


def test_pour_wins_when_cell_is_both_pour_and_barrier():
    # Defensive: overlap is impossible by construction, but if a cell is both,
    # dprst (_DRAINS) must win over the barrier seed.
    fdr = np.array([[1, 255]], dtype=np.uint8)
    pour = np.array([[0, 1]], dtype=np.uint8)
    barrier = np.array([[0, 1]], dtype=np.uint8)
    out, n_cycles = drains_to_dprst_kernel(fdr, pour, barrier)
    assert out.tolist() == [[1, 1]]
    assert n_cycles == 0

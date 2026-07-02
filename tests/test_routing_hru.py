import numpy as np
from gfv2_params.depstor import mask_fdr_to_vpu, vpu_pour_points
from gfv2_params.d8_routing import drains_to_dprst_labeled_kernel


def test_labeled_trace_attributes_to_reached_hru_with_barrier():
    # 1x5 single VPU (code 1). land -> land -> [dprst in HRU 42] , and a second
    # chain where an on-stream barrier blocks the reach.
    #   col: 0    1        2(dprst,HRU42)   3(onstream)    4(dprst,HRU9)
    # flow all East; col4 pours to HRU9 but col3 is a barrier.
    vpu = np.ones((1, 5), dtype=np.uint8)
    fdr = np.array([[1, 1, 255, 1, 255]], dtype=np.uint8)
    dprst = np.array([[0, 0, 1, 0, 1]], dtype=np.uint8)
    onstream = np.array([[0, 0, 0, 1, 0]], dtype=np.uint8)
    hru = np.array([[7, 7, 42, 8, 9]], dtype=np.int32)

    fdr_m = mask_fdr_to_vpu(fdr, vpu, code=1)
    label = np.where((dprst == 1) & (vpu == 1), hru, 0).astype(np.int32)
    barrier = vpu_pour_points(onstream, vpu, code=1)
    out, _ = drains_to_dprst_labeled_kernel(fdr_m, label, barrier)
    # col0,col1 reach dprst HRU42; col2 is the dprst (label 42); col3 barrier=0;
    # col4 is its own dprst (label 9).
    assert out.tolist() == [[42, 42, 42, 0, 9]]

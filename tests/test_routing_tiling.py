from gfv2_params.depstor_builders import STEP_ORDER


def test_vpu_id_runs_before_routing():
    # routing tiles by vpu_id, so the partition must be built first.
    assert STEP_ORDER.index("vpu_id") < STEP_ORDER.index("routing")

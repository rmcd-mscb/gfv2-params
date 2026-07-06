import numpy as np
import pytest

from gfv2_params.snarea.selection import SelectionParams, classify, passes_selection


def _ok(**over):
    base = dict(has_snow=True, n_cells=100, water_frac=0.1,
                seasonal_sca_max=0.9, constant_frac=0.3, similarity_value=0.05,
                params=SelectionParams())
    base.update(over)
    return passes_selection(**base)


def test_passes_when_all_good():
    assert _ok() == (True, "derived")


def test_reasons():
    assert _ok(has_snow=False)[1] == "default_no_snow"
    assert _ok(n_cells=10)[1] == "default_too_few_cells"
    assert _ok(water_frac=0.8)[1] == "default_water_dominated"
    assert _ok(seasonal_sca_max=0.2)[1] == "default_low_sca"
    assert _ok(constant_frac=0.95)[1] == "default_constant_sca"
    assert _ok(similarity_value=0.5)[1] == "default_dissimilar"


def test_selection_params_rejects_out_of_range():
    with pytest.raises(ValueError):
        SelectionParams(max_water_frac=50)
    with pytest.raises(ValueError):
        SelectionParams(min_cells=-1)
    SelectionParams()  # valid defaults must not raise


def test_classify():
    lo = np.array([1, .9, .8, .7, .6, .40, .3, .2, .1, .05, 0.0])
    hi = np.array([1, .95, .9, .85, .8, .70, .6, .5, .3, .1, 0.0])
    mid = np.array([1, .9, .8, .7, .6, .50, .4, .3, .2, .1, 0.0])
    assert classify(lo) == "low"
    assert classify(hi) == "high"
    assert classify(mid) == "mid"


def test_selection_params_recalibrated_defaults():
    # Pin the Oregon-recalibrated defaults so an accidental revert to the
    # paper's 25 / 0.15 is caught (see 2026-07-06 investigation).
    p = SelectionParams()
    assert p.min_cells == 15
    assert p.max_similarity == 0.10

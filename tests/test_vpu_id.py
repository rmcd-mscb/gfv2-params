"""Unit tests for vpu_id code mapping + resolution precedence (issue #55)."""

import pytest

from gfv2_params.depstor_builders.vpu_id import resolve_vpu_source, vpu_to_code


def test_vpu_to_code_zero_padded():
    assert vpu_to_code("01") == 1
    assert vpu_to_code("17") == 17
    assert vpu_to_code("18") == 18


def test_vpu_to_code_rejects_garbage():
    with pytest.raises(ValueError):
        vpu_to_code("not-a-vpu")


def test_resolve_prefers_profile_scalar():
    # profile vpu scalar wins even if the fabric has an attribute
    kind, value = resolve_vpu_source(profile_vpu="17", fabric_has_vpu_attr=True)
    assert kind == "scalar" and value == "17"


def test_resolve_falls_back_to_attribute():
    kind, value = resolve_vpu_source(profile_vpu=None, fabric_has_vpu_attr=True)
    assert kind == "attribute" and value == "vpu"


def test_resolve_errors_when_neither():
    with pytest.raises(ValueError, match="requires a profile `vpu`"):
        resolve_vpu_source(profile_vpu=None, fabric_has_vpu_attr=False)


def test_vpu_to_code_subregions_map_to_parent():
    assert vpu_to_code("03N") == 3
    assert vpu_to_code("03S") == 3
    assert vpu_to_code("03W") == 3
    assert vpu_to_code("10L") == 10
    assert vpu_to_code("10U") == 10

import pytest

from gfv2_params.aggregate import SourceAdapter


def test_defaults_and_grid_variable():
    a = SourceAdapter(source_key="demo", variables=["swe"], files_glob="*.nc")
    assert a.variables == ("swe",)          # list coerced to tuple
    assert a.grid_variable == "swe"         # defaults to first variable
    assert a.stat_method == "mean"
    assert a.source_crs == "EPSG:4326"


def test_rejects_bad_stat_method():
    with pytest.raises(ValueError, match="stat_method"):
        SourceAdapter(source_key="d", variables=("swe",), files_glob="*.nc",
                      stat_method="not_a_method")


def test_rejects_empty_variables():
    with pytest.raises(ValueError, match="variables"):
        SourceAdapter(source_key="d", variables=(), files_glob="*.nc")


def test_std_variables_defaults_empty_and_validates():
    a = SourceAdapter(source_key="s", variables=("swe", "scov"), files_glob="*.nc")
    assert a.std_variables == ()
    a2 = SourceAdapter(source_key="s", variables=("swe",), files_glob="*.nc", std_variables=("swe",))
    assert a2.std_variables == ("swe",)
    with pytest.raises(ValueError, match="std_variables"):
        SourceAdapter(source_key="s", variables=("swe",), files_glob="*.nc", std_variables=("missing",))

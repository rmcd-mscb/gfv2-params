"""`plan_exports` -- which PRMS parameters the export writes, from the real configs.

Pure YAML + the committed data dictionary, so these run in CI. They pin the decisions
that are easy to erode: the alternative LULC sources stay out, provenance never leaks,
and a PRMS name can only ever have one producer.
"""

from __future__ import annotations

import pytest

from gfv2_params.params_index import DeclaredParam, load_declared_params
from gfv2_params.prms_export import ExportItem, load_dictionary, plan_exports

DICTIONARY = load_dictionary()


def _by_name(items):
    return {i.prms_name: i for i in items}


def _declared(name, merged_file, columns, **prms):
    return DeclaredParam(
        name, merged_file, [], {},
        {"columns": {c: {"prms": p, "processes": []} for c, p in columns.items()}, **prms},
    )


def test_real_plan_is_non_empty_with_unique_names_all_in_the_dictionary():
    items = plan_exports()
    names = [i.prms_name for i in items]
    assert len(names) >= 25, f"suspiciously small plan: {names}"
    assert len(names) == len(set(names))
    assert set(names) <= set(DICTIONARY)


def test_renamed_columns_are_exported_under_the_prms_name():
    plan = _by_name(plan_exports())
    assert plan["hru_elev"].source_cols == ("mean",)
    assert plan["soil_type"].source_cols == ("soils",)
    # one quantity under two names across fabric vintages: an alias group, not a collision
    assert set(plan["rad_trncf"].source_cols) == {"retention", "rad_trncf"}


def test_only_nhm_v11_feeds_the_lulc_parameters():
    plan = _by_name(plan_exports())
    for name in ("cov_type", "covden_sum", "covden_win", "srain_intcp", "wrain_intcp", "snow_intcp"):
        assert plan[name].entry == "lulc_nhm_v11"


def test_export_false_is_set_on_exactly_the_three_alternative_lulc_entries():
    opted_out = {d.name for d in load_declared_params() if d.prms.get("export") is False}
    assert opted_out == {"lulc_nalcms", "lulc_nlcd", "lulc_foresce"}


def test_provenance_and_derivation_inputs_are_never_exported():
    names = set(_by_name(plan_exports()))
    assert {"hru_elev", "hru_aspect", "dprst_depth_avg"} <= names  # positive control: plan is real
    # raw stats, derivation inputs and ssflux's non-PRMS columns (hru_area there is m2,
    # PRMS hru_area is acres)
    for leaked in ("mean_sin", "mean_cos", "hru_area", "vpu", "fflux", "k_perm_log_wtd",
                   "dprst_depth_provenance", "cv_assign", "similarity", "count", "std"):
        assert leaked not in names


def test_snarea_curve_is_the_dimensioned_parameter():
    item = _by_name(plan_exports())["snarea_curve"]
    assert item.dimension == "ndeplval"
    assert item.source_cols == tuple(f"snarea_curve_{i}" for i in range(11))
    assert item.entry == "snarea_curve"


def test_integer_dictionary_types_are_carried_through():
    plan = _by_name(plan_exports())
    for name in ("cov_type", "soil_type", "hru_deplcrv"):
        assert plan[name].dtype == "I"
    assert plan["hru_slope"].dtype == "F"


def test_a_name_produced_by_two_entries_raises():
    declared = [
        _declared("a", "a.csv", {"x": "cov_type"}),
        _declared("b", "b.csv", {"y": "cov_type"}),
    ]
    with pytest.raises(ValueError, match="more than one exportable entry"):
        plan_exports(declared, DICTIONARY)


def test_export_false_removes_the_collision():
    declared = [
        _declared("a", "a.csv", {"x": "cov_type"}),
        _declared("b", "b.csv", {"y": "cov_type"}, export=False),
    ]
    assert [i.entry for i in plan_exports(declared, DICTIONARY)] == ["a"]


def test_an_alias_group_inside_one_entry_is_not_a_collision():
    declared = [_declared("a", "a.csv", {"retention": "rad_trncf", "rad_trncf": "rad_trncf"})]
    (item,) = plan_exports(declared, DICTIONARY)
    assert isinstance(item, ExportItem) and len(item.source_cols) == 2


def test_a_name_missing_from_the_dictionary_raises():
    with pytest.raises(ValueError, match="not in the data dictionary"):
        plan_exports([_declared("a", "a.csv", {"x": "not_a_prms_param"})], DICTIONARY)


def test_a_dimension_the_export_cannot_write_raises_at_plan_time():
    with pytest.raises(NotImplementedError, match="nhru"):
        plan_exports([_declared("a", "a.csv", {"x": "adjmix_rain"})], DICTIONARY)  # nhru x nmonths


def test_an_ndeplval_parameter_other_than_snarea_curve_raises_at_plan_time():
    fake = {"fake_curve": {"Type": "F", "Dimensions": "ndeplval", "Units": "", "Description": "",
                           "minimum": "0", "maximum": "1", "Source": "x"}}
    with pytest.raises(NotImplementedError, match="snarea_curve"):
        plan_exports([_declared("a", "a.csv", {"x": "fake_curve"})], fake)


def test_defects_and_provenance_are_not_read():
    declared = [DeclaredParam(
        "a", "a.csv", [], {},
        {"columns": {"x": {"prms": "cov_type", "processes": []}},
         "defects": {"y": {"prms": "soil_type"}},
         "provenance": {"z": "note"}},
    )]
    assert [i.prms_name for i in plan_exports(declared, DICTIONARY)] == ["cov_type"]

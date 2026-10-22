"""The coverage classifier and renderers (gfv2_params.prms_coverage).

The classifier is the only logic standing between a new dictionary shape and a silent
mis-bucket in the README, so the real dictionary must classify completely.
"""

from __future__ import annotations

import re

from gfv2_params.prms_coverage import (
    FABRIC_ATTRS, GAGE, HRU_CONSTANTS, MONTHLY, NOT_YET, RUN_CONTROLS, SCALARS, SEGMENT, UNCLASSIFIED,
    classify, covered_names, group_uncovered, render_covered, render_not_covered, render_summary,
)
from gfv2_params.prms_export import load_dictionary, plan_exports

DICTIONARY = load_dictionary()
ITEMS = plan_exports()


def _group(name):
    return classify(DICTIONARY[name])


def test_known_parameters_land_in_the_expected_group():
    assert _group("adjmix_rain") == MONTHLY
    assert _group("K_coef") == SEGMENT
    assert _group("poi_gage_id") == GAGE
    assert _group("print_freq") == RUN_CONTROLS
    assert _group("elev_units") == RUN_CONTROLS
    assert _group("outlet_sta") == RUN_CONTROLS
    assert _group("den_init") == SCALARS
    assert _group("gwsink_coef") == HRU_CONSTANTS
    assert _group("hru_area") == FABRIC_ATTRS
    assert _group("hru_lat") == FABRIC_ATTRS
    assert _group("jh_coef_hru") == NOT_YET


def test_the_real_dictionary_classifies_completely():
    groups = group_uncovered(ITEMS, DICTIONARY)
    assert UNCLASSIFIED not in groups, [r["Parameter"] for r in groups.get(UNCLASSIFIED, [])]


def test_covered_and_not_covered_partition_the_dictionary():
    covered = covered_names(ITEMS)
    uncovered = {r["Parameter"] for rows in group_uncovered(ITEMS, DICTIONARY).values() for r in rows}
    assert covered | uncovered == set(DICTIONARY)
    assert not covered & uncovered


def test_covered_wins_over_classification():
    # dprst_depth_avg / op_flow_thres have dictionary Source `Regan` (a constants source),
    # yet we produce them: they must be listed as covered, never as a Regan constant.
    uncovered = {r["Parameter"] for rows in group_uncovered(ITEMS, DICTIONARY).values() for r in rows}
    assert not {"dprst_depth_avg", "op_flow_thres", "dprst_flow_coef"} & uncovered


def test_summary_counts_come_from_the_plan_and_dictionary():
    text = render_summary(ITEMS, DICTIONARY)
    assert f"{len(ITEMS)} of the {len(DICTIONARY)}" in text
    assert f"{len(DICTIONARY) - len(ITEMS)} are not" in text


def test_covered_table_has_one_row_per_item_and_marks_snarea_curve_as_not_per_hru():
    lines = render_covered(ITEMS, DICTIONARY).splitlines()
    assert len(lines) == 2 + len(ITEMS)
    (curve,) = [ln for ln in lines if ln.startswith("| `snarea_curve`")]
    assert "not per-HRU" in curve


def test_cells_cannot_break_the_table():
    rows = {"x": {"Parameter": "x", "Type": "F", "Units": "a|b", "Dimensions": "nhru",
                  "Description": "line one\nline | two", "Source": "Regan"}}
    text = render_not_covered([], rows)
    assert "a\\|b" in text and "line one line \\| two" in text
    rows_ = [ln for ln in text.splitlines() if ln.startswith("| `x`")]
    assert rows_ and all(len(re.findall(r"(?<!\\)\|", ln)) == 5 for ln in rows_)  # unescaped pipes only

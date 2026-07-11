"""Unit tests for the pure WESM 1m-qualification helpers (issue #173 Task
7b). No network I/O — `ensure_wesm_local`/`load_wesm_1m_footprints`'s
download/read paths are exercised live, not here (see the staging module's
own live smoke test)."""
from __future__ import annotations

from gfv2_params.download.wesm import _OUTPUT_NAME, _TARGET_CRS
from gfv2_params.dprst_depth.wesm_io import (
    QUALIFYING_1M_CATEGORIES,
    _qualifying_where_clause,
    is_1m_qualifying,
)


def test_is_1m_qualifying_true_for_meets_categories():
    assert is_1m_qualifying("Meets")
    assert is_1m_qualifying("Meets with variance")


def test_is_1m_qualifying_false_for_non_qualifying_categories():
    assert not is_1m_qualifying("Does not meet")
    assert not is_1m_qualifying("Pending publication")
    assert not is_1m_qualifying("")
    assert not is_1m_qualifying("meets")  # case-sensitive, not a fuzzy match


def test_qualifying_where_clause_default_matches_categories():
    clause = _qualifying_where_clause()
    assert clause == "onemeter_category IN ('Meets', 'Meets with variance')"
    for cat in QUALIFYING_1M_CATEGORIES:
        assert f"'{cat}'" in clause


def test_qualifying_where_clause_custom_categories():
    clause = _qualifying_where_clause(("Does not meet",))
    assert clause == "onemeter_category IN ('Does not meet')"


def test_staging_output_name_matches_config_key_filename():
    # gfv2_params.download.wesm.stage_wesm must write exactly the filename
    # base_config.yml's `wesm_index` profile key points at
    # ({data_root}/input/wesm/wesm_1m_footprints.gpkg) -- see
    # depstor_builders/dprst_depth.py's ctx.wesm_index consumer.
    assert _OUTPUT_NAME == "wesm_1m_footprints.gpkg"
    assert _TARGET_CRS == "EPSG:5070"

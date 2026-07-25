"""Tests for scripts/migrate_filled_params.py

One-time migration off the retired `filled_` prefix onto the canonical
convention (`merged/<name>.csv` always filled, pre-fill copy at
`merged/_unfilled/<name>.csv`). See scripts/merge_and_fill_params.py's
UNFILLED_DIRNAME / write_filled_in_place for the convention this migrates
existing products onto.
"""

import importlib.util
from pathlib import Path

import pytest

_spec = importlib.util.spec_from_file_location(
    "migrate_filled_params",
    Path(__file__).resolve().parent.parent / "scripts" / "migrate_filled_params.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

mig = _mod


def test_plan_migration_pairs_filled_with_canonical(tmp_path):
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    (tmp_path / "nhm_ssflux_params.csv").write_text("hru_id,v\n1,\n")
    (tmp_path / "nhm_slope_params.csv").write_text("hru_id,v\n1,2\n")   # no filled_ pair

    plan = mig.plan_migration(tmp_path)

    assert len(plan) == 1
    filled, canonical, raw = plan[0]
    assert filled.name == "filled_nhm_ssflux_params.csv"
    assert canonical.name == "nhm_ssflux_params.csv"
    assert raw == tmp_path / "_unfilled" / "nhm_ssflux_params.csv"


def test_migration_is_idempotent(tmp_path):
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    (tmp_path / "nhm_ssflux_params.csv").write_text("hru_id,v\n1,\n")

    mig.apply_migration(mig.plan_migration(tmp_path))
    raw_after_first = (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").read_text()
    mig.apply_migration(mig.plan_migration(tmp_path))   # second run: nothing left to do

    assert (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").read_text() == raw_after_first
    assert not (tmp_path / "filled_nhm_ssflux_params.csv").exists()
    assert (tmp_path / "nhm_ssflux_params.csv").read_text() == "hru_id,v\n1,1\n"


def test_migration_resumes_when_raw_preserved_but_canonical_missing(tmp_path):
    """A previous run of this script died between step 1 (preserve) and step 2
    (move filled_ onto canonical): raw_target already exists, but canonical_target
    does not. This IS a resumable partial migration -- step 2 must proceed and the
    preserved raw copy must be untouched."""
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    (tmp_path / "_unfilled").mkdir()
    (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").write_text("ORIGINAL\n")
    assert not (tmp_path / "nhm_ssflux_params.csv").exists()

    mig.apply_migration(mig.plan_migration(tmp_path))

    assert (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").read_text() == "ORIGINAL\n"
    assert (tmp_path / "nhm_ssflux_params.csv").read_text() == "hru_id,v\n1,1\n"
    assert not (tmp_path / "filled_nhm_ssflux_params.csv").exists()


def test_migration_raises_when_canonical_already_filled_and_raw_preserved(tmp_path):
    """The Finding-1 regression: raw_target AND canonical_target BOTH already
    exist -- the canonical file is already on the new convention (e.g. a fresh
    merge_and_fill_params.py run) and filled_<name>.csv is a stale leftover.
    Moving it over the canonical would silently revert today's correct fill.
    Must raise, and must leave every file exactly as it was."""
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    todays_fill = "hru_id,v\n1,1.0\n2,42.0\n"
    (tmp_path / "nhm_ssflux_params.csv").write_text(todays_fill)
    (tmp_path / "_unfilled").mkdir()
    (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").write_text("ORIGINAL\n")

    with pytest.raises(mig.AlreadyMigratedError, match="Refusing to migrate"):
        mig.apply_migration(mig.plan_migration(tmp_path))

    # Nothing moved: the canonical file must still hold TODAY's correct fill,
    # not be silently reverted; filled_ and _unfilled/ are untouched too.
    assert (tmp_path / "nhm_ssflux_params.csv").read_text() == todays_fill
    assert (tmp_path / "filled_nhm_ssflux_params.csv").read_text() == "hru_id,v\n1,1\n"
    assert (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").read_text() == "ORIGINAL\n"


def test_print_plan_matches_apply_for_refuse_case(tmp_path, capsys):
    """A dry run must show REFUSE for exactly the triples apply_migration would
    raise on -- not a plan implying the move would succeed."""
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    (tmp_path / "nhm_ssflux_params.csv").write_text("hru_id,v\n1,1.0\n2,42.0\n")
    (tmp_path / "_unfilled").mkdir()
    (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").write_text("ORIGINAL\n")

    plan = mig.plan_migration(tmp_path)
    mig.print_plan(plan)
    out = capsys.readouterr().out

    assert "REFUSE" in out
    # print_plan must not touch the filesystem regardless of what it prints.
    assert (tmp_path / "filled_nhm_ssflux_params.csv").exists()
    assert (tmp_path / "nhm_ssflux_params.csv").read_text() == "hru_id,v\n1,1.0\n2,42.0\n"

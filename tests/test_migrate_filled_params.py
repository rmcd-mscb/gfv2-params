"""Tests for scripts/migrate_filled_params.py

One-time migration off the retired `filled_` prefix onto the canonical
convention (`merged/<name>.csv` always filled, pre-fill copy at
`merged/_unfilled/<name>.csv`). See scripts/merge_and_fill_params.py's
UNFILLED_DIRNAME / write_filled_in_place for the convention this migrates
existing products onto.
"""

import importlib.util
from pathlib import Path

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


def test_migration_refuses_when_raw_already_preserved(tmp_path):
    """Never overwrite an existing _unfilled/ copy — that is the irreversible mistake."""
    (tmp_path / "filled_nhm_ssflux_params.csv").write_text("hru_id,v\n1,1\n")
    (tmp_path / "nhm_ssflux_params.csv").write_text("hru_id,v\n1,\n")
    (tmp_path / "_unfilled").mkdir()
    (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").write_text("ORIGINAL\n")

    mig.apply_migration(mig.plan_migration(tmp_path))

    assert (tmp_path / "_unfilled" / "nhm_ssflux_params.csv").read_text() == "ORIGINAL\n"

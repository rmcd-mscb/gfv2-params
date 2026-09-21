import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# Same loading idiom as tests/test_dprst_depth_probe.py: scripts/ is not a package.
_spec = importlib.util.spec_from_file_location(
    "compare_dprst_depth_runs",
    Path(__file__).resolve().parent.parent / "scripts" / "diagnose" / "compare_dprst_depth_runs.py",
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)
compare = _mod.compare
main = _mod.main


def test_compare_classifies_rows():
    old = pd.DataFrame({"COMID": [1, 2, 3, 4], "dprst_depth_m": [1.0, 2.0, np.nan, 4.0],
                        "method": ["measured", "measured", "calibrated_hollister", "measured"]})
    new = pd.DataFrame({"COMID": [1, 2, 3, 4], "dprst_depth_m": [1.0, 2.5, 3.0, 4.0],
                        "method": ["measured", "measured", "measured", "measured"],
                        "source": ["A", "B", "A", "A"]})
    r = compare(old, new, expect_identical={1, 2})
    assert r["identical"] == 2          # COMIDs 1 and 4
    assert r["changed"] == 1            # COMID 2: 2.0 -> 2.5
    assert r["dropped"] == 0
    assert r["newly_measured"] == 1     # COMID 3: filled -> measured
    assert r["violations"] == [2]       # COMID 2 was expected identical but changed
    assert r["violations_changed"] == [2]
    assert r["violations_dropped"] == []


def test_compare_reports_dropped_comids_separately_from_changed():
    # A baseline COMID absent from the new run used to fall into `changed` with a
    # NaN delta -- np.nanmedian silently dropped it from abs_change_p50_m, so a
    # re-run that LOSES polygons reported a falsely small median change, and
    # `changed` was inflated by rows that were never a same-COMID comparison at all.
    old = pd.DataFrame({"COMID": [1, 2, 3], "dprst_depth_m": [1.0, 2.0, 3.0]})
    new = pd.DataFrame({"COMID": [1, 3], "dprst_depth_m": [1.0, 3.5]})  # COMID 2 gone
    r = compare(old, new, expect_identical={2})
    assert r["dropped"] == 1            # COMID 2
    assert r["changed"] == 1            # COMID 3: 3.0 -> 3.5, NOT diluted by COMID 2
    assert r["abs_change_p50_m"] == pytest.approx(0.5)  # only COMID 3's real delta
    assert r["violations_dropped"] == [2]   # expected identical but gone entirely
    assert r["violations_changed"] == []
    assert r["violations"] == [2]


def test_main_exits_0_on_a_clean_comparison(tmp_path, monkeypatch):
    """(#223 review round 3, test gap 6b) `main()`'s exit code is the ACTUAL
    re-run gate a CI/operator script checks -- untested until now."""
    old = tmp_path / "old.parquet"
    new = tmp_path / "new.parquet"
    pd.DataFrame({"COMID": [1, 2], "dprst_depth_m": [1.0, 2.0]}).to_parquet(old)
    pd.DataFrame({"COMID": [1, 2], "dprst_depth_m": [1.0, 2.0]}).to_parquet(new)
    monkeypatch.setattr("sys.argv", ["compare_dprst_depth_runs.py", str(old), str(new)])
    assert main() == 0


def test_main_exits_1_on_an_expect_identical_violation(tmp_path, monkeypatch, capsys):
    old = tmp_path / "old.parquet"
    new = tmp_path / "new.parquet"
    ids = tmp_path / "ids.txt"
    pd.DataFrame({"COMID": [1, 2], "dprst_depth_m": [1.0, 2.0]}).to_parquet(old)
    pd.DataFrame({"COMID": [1, 2], "dprst_depth_m": [1.0, 9.0]}).to_parquet(new)  # COMID 2 changed
    ids.write_text("2\n")
    monkeypatch.setattr(
        "sys.argv", ["compare_dprst_depth_runs.py", str(old), str(new), "--expect-identical", str(ids)],
    )
    assert main() == 1
    assert "first violations (changed)" in capsys.readouterr().out

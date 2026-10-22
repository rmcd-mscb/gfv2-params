"""scripts/export_prms_params.py -- the thin CLI around export_fabric."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "export_prms_params.py"


def _run(*args):
    return subprocess.run([sys.executable, str(SCRIPT), *args], cwd=REPO, capture_output=True, text=True,
                          env={**os.environ, "PYTHONPATH": str(REPO / "src")})


def test_dry_run_prints_the_plan_and_touches_nothing():
    r = _run("--dry-run")
    out = r.stdout + r.stderr
    assert r.returncode == 0, out
    for name in ("hru_elev", "soil_type", "rad_trncf", "snarea_curve"):
        assert name in out


def test_missing_merged_inputs_exit_one_with_a_clear_message(tmp_path):
    """Pointing the export at a data root with no merged/ must fail loudly, not traceback."""
    base = tmp_path / "base.yml"
    base.write_text(
        "data_root: " + str(tmp_path) + "\n"
        "default_fabric: f\n"
        "fabrics:\n  f:\n    id_feature: nat_hru_id\n    expected_max_hru_id: 4\n"
    )
    r = _run("--base_config", str(base), "--fabric", "f")
    assert r.returncode == 1, r.stdout + r.stderr
    assert "export failed, nothing written" in r.stdout + r.stderr

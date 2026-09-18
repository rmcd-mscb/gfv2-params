"""Step selection in scripts/build_depstor_rasters.py: --step, --from, --stop-before.

``--stop-before`` exists so a complete re-run can split the depstor stack around
``dprst_depth`` (issue #221). ``dprst_depth`` has two execution paths -- a 150-way
tiled SLURM array, and a serial in-process fallback used when no per-batch parquets
exist. The re-run manifest used to run the WHOLE stack first, so on a fresh CONUS
fabric the builder took the in-process path: ~a week of serial compute against an
18 h job limit (gfv2r2, job 4404803: 9.4% of 392,672 polygons in 11 h). The fix runs
``--stop-before dprst_depth``, then the tiled stage, then ``--from dprst_depth``.

Both halves name the SAME step, so the split cannot leave a gap. Hand-picking two
names instead (``--until hru_id`` / ``--from vpu_id``) would silently skip any step
later inserted between them -- which is why the partition property below is the test
that matters.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

from gfv2_params.depstor_builders import STEP_ORDER

REPO_ROOT = Path(__file__).resolve().parent.parent
ORCHESTRATOR = REPO_ROOT / "scripts" / "build_depstor_rasters.py"


def _orchestrator():
    spec = importlib.util.spec_from_file_location("build_depstor_rasters", ORCHESTRATOR)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _steps():
    """Step dicts in canonical order -- the shape _select_steps receives."""
    return [{"name": n} for n in STEP_ORDER]


def _names(steps):
    return [s["name"] for s in steps]


class TestStopBefore:
    def test_runs_every_step_strictly_before_the_boundary(self):
        sel = _orchestrator()._select_steps(_steps(), None, None, "dprst_depth")
        k = STEP_ORDER.index("dprst_depth")
        assert _names(sel) == STEP_ORDER[:k]

    def test_excludes_the_boundary_step_itself(self):
        sel = _orchestrator()._select_steps(_steps(), None, None, "dprst_depth")
        assert "dprst_depth" not in _names(sel)

    @pytest.mark.parametrize("boundary", STEP_ORDER[1:])
    def test_stop_before_and_from_partition_every_step(self, boundary):
        """The guarantee the manifest split rests on: at any boundary, the two halves
        together run every step exactly once, in order, with nothing skipped or
        repeated. Parametrised over EVERY possible boundary, so it also holds for a step
        added to STEP_ORDER later."""
        mod = _orchestrator()
        first = _names(mod._select_steps(_steps(), None, None, boundary))
        second = _names(mod._select_steps(_steps(), None, boundary, None))
        assert first + second == STEP_ORDER

    def test_combines_with_from_as_a_half_open_range(self):
        sel = _orchestrator()._select_steps(_steps(), None, "waterbody", "dprst")
        lo, hi = STEP_ORDER.index("waterbody"), STEP_ORDER.index("dprst")
        assert _names(sel) == STEP_ORDER[lo:hi]

    def test_an_unknown_boundary_raises(self):
        with pytest.raises(ValueError, match="--stop-before"):
            _orchestrator()._select_steps(_steps(), None, None, "no_such_step")

    def test_stopping_before_the_first_step_raises(self):
        """Zero steps selected would log "Running 0 step(s)" and exit 0 -- a run that
        did nothing and reported success. Refuse it."""
        with pytest.raises(ValueError, match="no steps"):
            _orchestrator()._select_steps(_steps(), None, None, STEP_ORDER[0])

    def test_a_boundary_at_or_before_from_raises(self):
        """--from dprst --stop-before waterbody selects nothing, for the same reason."""
        with pytest.raises(ValueError, match="no steps"):
            _orchestrator()._select_steps(_steps(), None, "dprst", "waterbody")


class TestExistingSelectionUnchanged:
    """_select_steps had no tests before #221; pin the behaviour it already had."""

    def test_no_selector_runs_everything(self):
        assert _names(_orchestrator()._select_steps(_steps(), None, None, None)) == STEP_ORDER

    def test_step_runs_exactly_that_step(self):
        assert _names(_orchestrator()._select_steps(_steps(), "dprst", None, None)) == ["dprst"]

    def test_from_resumes_at_the_named_step(self):
        k = STEP_ORDER.index("vpu_id")
        assert _names(_orchestrator()._select_steps(_steps(), None, "vpu_id", None)) == STEP_ORDER[k:]


class TestCli:
    def test_step_and_stop_before_are_mutually_exclusive(self, tmp_path):
        """Checked before any config is loaded, so a dummy --config is never opened."""
        r = subprocess.run(
            [
                sys.executable, str(ORCHESTRATOR),
                "--config", str(tmp_path / "unused.yml"),
                "--step", "dprst", "--stop-before", "dprst_depth",
            ],
            capture_output=True, text=True, cwd=REPO_ROOT,
        )
        assert r.returncode == 2, r.stderr
        # Not merely "exit 2 and the flag is named": argparse gives exactly that for an
        # UNRECOGNISED flag, so a looser assertion passes before --stop-before exists.
        assert "unrecognized arguments" not in r.stderr, r.stderr
        assert "mutually exclusive" in r.stderr, r.stderr

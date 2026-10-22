"""The generated README coverage regions must match the configs and the dictionary.

Same CI-gate shape as tests/test_build_workflow_doc.py: the checked-in README is
compared against a fresh render via `--check`, plus a tamper test so the gate itself is
proven to bite.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
README = REPO / "README.md"
GEN = REPO / "scripts" / "build_prms_coverage_doc.py"
REGIONS = ("prms-summary", "prms-covered", "prms-not-covered")


def _run(*args):
    return subprocess.run([sys.executable, str(GEN), *args], cwd=REPO, capture_output=True, text=True,
                          env={**os.environ, "PYTHONPATH": str(REPO / "src")})


def test_generated_regions_are_up_to_date():
    """The CI gate. If this fails, run: python scripts/build_prms_coverage_doc.py"""
    r = _run("--check")
    assert r.returncode == 0, (
        "README.md PRMS coverage section is STALE -- run "
        f"`python scripts/build_prms_coverage_doc.py`\n{r.stdout}{r.stderr}"
    )


def test_readme_carries_every_marker_pair():
    text = README.read_text()
    for name in REGIONS:
        assert f"<!-- BEGIN GENERATED: {name} -->" in text
        assert f"<!-- END GENERATED: {name} -->" in text


def test_check_fails_on_a_tampered_region(tmp_path):
    scratch = tmp_path / "README.md"
    scratch.write_text(README.read_text().replace("| `hru_elev` |", "| `hru_elevX` |", 1))
    assert _run("--check", "--path", str(scratch)).returncode == 1


def test_rewrite_repairs_a_tampered_copy_and_touches_nothing_outside_the_markers(tmp_path):
    scratch = tmp_path / "README.md"
    original = README.read_text()
    tampered = original.replace("| `hru_elev` |", "| `hru_elevX` |", 1)
    assert tampered != original, "tamper target not found; this test would pass vacuously"
    scratch.write_text(tampered)
    assert _run("--path", str(scratch)).returncode == 0
    assert scratch.read_text() == original


def test_missing_marker_is_a_loud_failure_not_an_append(tmp_path):
    scratch = tmp_path / "README.md"
    scratch.write_text("# no markers here\n")
    r = _run("--path", str(scratch))
    assert r.returncode != 0
    assert "generated region" in (r.stdout + r.stderr)
    assert scratch.read_text() == "# no markers here\n"

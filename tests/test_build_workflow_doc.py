"""The generated runbook section must match the stage manifest.

This is the mechanism that keeps the one-command path and the individual commands from
drifting: both come from configs/workflow/fabric_rerun.yml, and CI fails if the rendered
section is stale. Same marker convention and ``--check`` contract as
scripts/build_parameter_index.py.

The requirement this serves is explicit: a scientist must be able to run the whole
workflow with one command OR run each stage by hand, and the hand commands must be the
real ones. Prose that merely *describes* the commands would rot; a generated region
cannot.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
RUNME = REPO / "slurm_batch" / "RUNME.md"
MANIFEST = REPO / "configs" / "workflow" / "fabric_rerun.yml"
GEN = REPO / "scripts" / "build_workflow_doc.py"

BEGIN = "<!-- BEGIN GENERATED: workflow -->"
END = "<!-- END GENERATED: workflow -->"


def _stages():
    return yaml.safe_load(MANIFEST.read_text())["stages"]


@pytest.fixture(scope="module")
def region():
    text = RUNME.read_text()
    assert BEGIN in text and END in text, f"RUNME.md has no {BEGIN} / {END} pair"
    return text[text.index(BEGIN) : text.index(END)]


def test_generated_region_is_up_to_date():
    """The CI gate. If this fails, run: python scripts/build_workflow_doc.py"""
    r = subprocess.run(
        [sys.executable, str(GEN), "--check"], cwd=REPO, capture_output=True, text=True
    )
    assert r.returncode == 0, (
        f"RUNME.md workflow section is STALE -- run `python scripts/build_workflow_doc.py`"
        f"\n{r.stdout}{r.stderr}"
    )


def test_every_stage_appears_in_the_generated_region(region):
    for s in _stages():
        assert s["name"] in region, f"stage {s['name']} missing from the generated section"


def test_individual_commands_appear_verbatim(region):
    """The whole point: a scientist copying one line out of the runbook runs the same
    command the driver would. Compared against the manifest, not against prose."""
    for s in _stages():
        assert s["command"] in region, (
            f"{s['name']}: manifest command not present verbatim in the runbook.\n"
            f"  expected: {s['command']}"
        )


def test_shared_stage_is_rendered_with_its_warning(region):
    """The driver SKIPS shared stages, but the doc must show them.

    This is the only place the "rebuilding shared rasters obliges a re-run of every
    fabric" trap is documented, and it is the trap that silently staled every fabric's
    elevation product in #215.
    """
    shared = [s for s in _stages() if s["scope"] == "shared"]
    assert shared, "manifest declares no shared stage"
    for s in shared:
        assert s["name"] in region
        assert s["note"].split()[0] in region, "the shared stage's note was not rendered"
    assert "every fabric" in region.lower()


def test_shared_stage_is_marked_as_not_run_by_the_driver(region):
    """A reader must not copy the shared command believing the one-command path covers
    it. Rendering it identically to the fabric stages would invite exactly that."""
    assert "skip" in region.lower() or "not run" in region.lower()


def test_the_one_command_path_is_rendered(region):
    """Both halves of the user requirement live in this region: the single command and
    the individual ones. Losing either silently would defeat the section's purpose."""
    assert "submit_fabric_rerun.sh" in region
    assert "--dry-run" in region, "the runbook must lead with the dry-run"


def test_force_is_documented_only_where_it_applies(region):
    """--force is meaningful on exactly one stage. Saying otherwise would send an
    operator looking for an effect that does not exist."""
    forcing = [s["name"] for s in _stages() if s["accepts_force"] and s["scope"] == "fabric"]
    assert forcing, "no fabric stage accepts --force; this test needs revisiting"
    for name in forcing:
        assert name in region


def test_check_detects_a_hand_edit(tmp_path):
    """A staleness gate that cannot detect staleness is worse than none.

    Exercised on a COPY: mutating the real RUNME.md would leave the repo dirty if this
    test failed partway.
    """
    scratch = tmp_path / "RUNME.md"
    text = RUNME.read_text()
    end = text.index(END)
    # Insert rather than substitute: any specific word to swap could stop appearing in
    # the rendered output, which would silently turn this into a no-op perturbation --
    # and a mutation test that mutates nothing always "passes".
    mutated = text[:end] + "A HAND EDIT THAT THE GENERATOR WOULD NEVER PRODUCE\n" + text[end:]
    assert mutated != text, "could not perturb the generated region"
    scratch.write_text(mutated)

    r = subprocess.run(
        [sys.executable, str(GEN), "--check", "--path", str(scratch)],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 1, (
        f"--check passed on a hand-edited region:\n{r.stdout}{r.stderr}"
    )


def test_writing_is_idempotent(tmp_path):
    """Running the generator twice must not keep changing the file -- otherwise the CI
    gate would fail immediately after a legitimate regeneration."""
    scratch = tmp_path / "RUNME.md"
    scratch.write_text(RUNME.read_text())
    for _ in range(2):
        r = subprocess.run(
            [sys.executable, str(GEN), "--path", str(scratch)],
            cwd=REPO,
            capture_output=True,
            text=True,
        )
        assert r.returncode == 0, r.stderr
    r = subprocess.run(
        [sys.executable, str(GEN), "--check", "--path", str(scratch)],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, f"generator is not idempotent:\n{r.stdout}{r.stderr}"


def test_missing_region_is_a_clear_error(tmp_path):
    """Never silently append. A doc without the markers is a mistake to report, not to
    'fix' by dumping the section somewhere arbitrary."""
    scratch = tmp_path / "RUNME.md"
    scratch.write_text("# no markers here\n")
    r = subprocess.run(
        [sys.executable, str(GEN), "--path", str(scratch)],
        cwd=REPO,
        capture_output=True,
        text=True,
    )
    assert r.returncode != 0
    assert BEGIN in r.stdout + r.stderr, "the error does not show the expected markers"

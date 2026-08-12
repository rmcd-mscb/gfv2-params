"""The fabric-rerun manifest is the single source of truth for the driver and the runbook.

Pure-YAML and filesystem checks -- no data root, no SLURM -- so they run in CI.

Why pin it at all: the manifest's ``command`` strings are simultaneously what a scientist
copies out of the generated runbook and what ``slurm_batch/submit_fabric_rerun.sh``
executes. A stage naming a script that does not exist, or claiming a ``--force`` the
command would reject, is a documentation defect and an execution defect in one edit.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
MANIFEST = REPO / "configs" / "workflow" / "fabric_rerun.yml"

REQUIRED_KEYS = {
    "name",
    "command",
    "kind",
    "scope",
    "accepts_force",
    "consumes",
    "produces",
    "resources",
}
OPTIONAL_KEYS = {"note"}
PLACEHOLDERS = {"batches", "fabric", "base_config"}


@pytest.fixture(scope="module")
def stages():
    return yaml.safe_load(MANIFEST.read_text())["stages"]


def test_manifest_declares_stages(stages):
    assert stages, "manifest declares no stages"


def test_every_stage_has_exactly_the_expected_keys(stages):
    """Unknown keys are rejected too: a typo'd key would otherwise be silently ignored,
    and a mistyped ``accepts_force`` would read as False and quietly drop the flag."""
    for s in stages:
        missing = REQUIRED_KEYS - set(s)
        unknown = set(s) - REQUIRED_KEYS - OPTIONAL_KEYS
        assert not missing, f"stage {s.get('name')!r} missing {sorted(missing)}"
        assert not unknown, f"stage {s['name']!r} has unknown keys {sorted(unknown)}"


def test_stage_names_are_unique(stages):
    names = [s["name"] for s in stages]
    assert len(names) == len(set(names)), f"duplicate stage names in {names}"


def test_scope_is_fabric_or_shared(stages):
    for s in stages:
        assert s["scope"] in {"fabric", "shared"}, f"{s['name']}: bad scope {s['scope']!r}"


def test_kind_is_wrapper_or_sbatch(stages):
    """The driver treats the two differently -- a wrapper takes ``--after`` and prints
    ``TERMINAL_JOB_ID``; a plain sbatch takes ``--dependency`` and prints "Submitted batch
    job N". Getting this wrong means an unchained stage."""
    for s in stages:
        assert s["kind"] in {"wrapper", "sbatch"}, f"{s['name']}: bad kind {s['kind']!r}"


def test_accepts_force_is_a_bool(stages):
    for s in stages:
        assert isinstance(s["accepts_force"], bool), (
            f"{s['name']}: accepts_force must be a bool, got {s['accepts_force']!r}"
        )


def test_every_command_names_a_file_that_exists(stages):
    """A manifest naming a missing script ships a broken command into the runbook AND the
    driver at the same time."""
    for s in stages:
        for token in s["command"].split():
            if token.startswith(("slurm_batch/", "./slurm_batch/", "scripts/")):
                path = REPO / token.lstrip("./")
                assert path.exists(), f"{s['name']}: command names missing file {token}"


def test_placeholders_are_from_the_known_set(stages):
    """The driver substitutes exactly these. An unknown placeholder would survive into the
    submitted command as a literal brace."""
    for s in stages:
        for ph in re.findall(r"\{(\w+)\}", s["command"]):
            assert ph in PLACEHOLDERS, f"{s['name']}: unknown placeholder {{{ph}}} in command"


def test_wrapper_kind_matches_a_real_submit_wrapper(stages):
    """``kind: wrapper`` asserts the command honours the contract from
    tests/test_wrapper_contract.py. Check the command really is one of those wrappers."""
    for s in stages:
        if s["kind"] != "wrapper":
            continue
        head = s["command"].split()[0]
        assert head.endswith(".sh") and "submit_" in head, (
            f"{s['name']}: kind=wrapper but command does not invoke a submit_*.sh: {head}"
        )
        script = REPO / head.lstrip("./")
        text = script.read_text()
        assert "TERMINAL_JOB_ID=" in text, f"{head} does not print TERMINAL_JOB_ID"
        assert "--after" in text, f"{head} does not accept --after"


def test_sbatch_kind_commands_start_with_sbatch(stages):
    for s in stages:
        if s["kind"] == "sbatch":
            assert s["command"].split()[0] == "sbatch", (
                f"{s['name']}: kind=sbatch but command does not start with sbatch"
            )


def test_accepts_force_is_true_only_where_the_command_really_takes_it(stages):
    """The claim has to be true of the actual script, not of the manifest's wishes.

    A stage wrongly marked ``accepts_force: true`` would abort the whole chain -- the
    submit wrappers reject unknown leading flags by design. A stage wrongly marked
    ``false`` is worse in the other direction: the re-run silently would not force, and
    an operator who passed --force would believe it had.
    """
    for s in stages:
        if not s["accepts_force"]:
            continue
        # Resolve the batch script the command submits, then the python it drives.
        batch = next(
            (t for t in s["command"].split() if t.lstrip("./").startswith("slurm_batch/")),
            None,
        )
        assert batch, f"{s['name']}: accepts_force but no slurm_batch/ script in command"
        batch_text = (REPO / batch.lstrip("./")).read_text()
        assert '"$@"' in batch_text, (
            f"{s['name']}: accepts_force but {batch} does not forward \"$@\" to python, "
            f"so --force would never reach the orchestrator"
        )
        script = re.search(r"scripts/(\w+)\.py", batch_text)
        assert script, f"{s['name']}: cannot find the python script {batch} drives"
        py = (REPO / "scripts" / f"{script.group(1)}.py").read_text()
        assert '"--force"' in py, (
            f"{s['name']}: accepts_force but scripts/{script.group(1)}.py has no --force"
        )


def test_exactly_one_shared_scope_stage_is_declared(stages):
    """build_shared_rasters must appear so the generated doc renders it with its warning,
    even though the driver skips it. Dropping it would silently remove the only place the
    shared-raster trap -- rebuilding it stales EVERY fabric -- is documented."""
    shared = [s for s in stages if s["scope"] == "shared"]
    assert len(shared) == 1, f"expected exactly one shared stage, got {[s['name'] for s in shared]}"
    assert shared[0]["note"], "the shared stage must carry its warning in `note`"


def test_the_shared_stage_comes_first(stages):
    """It is upstream of everything. Rendering it anywhere else would misread as optional
    cleanup rather than the prerequisite it is."""
    assert stages[0]["scope"] == "shared"


def test_fill_is_the_last_stage(stages):
    """The gap-fill rewrites merged/*.csv IN PLACE, reading every other stage's output.
    Anything scheduled after it would have its product left unfilled -- and because the
    fill is in-place, that is invisible in the filename."""
    assert stages[-1]["name"] == "fill", f"last stage is {stages[-1]['name']!r}, not 'fill'"


def test_snarea_precedes_fill(stages):
    """snarea WRITES into {fabric}/params/merged/, which fill then reads and rewrites.

    This is the claim that made the originally-specified `gate: false` unsafe: snarea is
    independent of the depstor and zonal chains, but NOT of the stage after it.
    """
    names = [s["name"] for s in stages]
    assert names.index("snarea") < names.index("fill")

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


def test_every_fabric_stage_names_the_fabric_and_the_config(stages):
    """A fabric-scope command MUST carry {fabric} and {base_config}.

    Every consumer defaults to gfv2 when the value is absent -- `FABRIC=${FABRIC:-gfv2}`
    in build_depstor_rasters.batch and merge_and_fill_params.batch, `FABRIC="${2:-gfv2}"`
    in all three submit wrappers. So dropping the placeholder from one entry does not
    fail: it silently retargets that stage at the CONUS production fabric, rebuilding
    gfv2's depstor rasters (384G/18h) or gap-filling gfv2's merged CSVs while reporting
    success for the fabric the operator named. The driver's dry-run tests cannot catch it
    -- they assert the fabric appears in the output, which the OTHER stages satisfy.
    """
    for s in stages:
        if s["scope"] != "fabric":
            continue
        for ph in ("{fabric}", "{base_config}"):
            assert ph in s["command"], (
                f"{s['name']}: fabric-scope command omits {ph}, so it would fall back to "
                f"the gfv2 default:\n  {s['command']}"
            )


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


def test_recommended_zonal_params_are_real_params():
    """`recommended_zonal_params` is rendered straight into the runbook as an export a
    scientist copies, so a typo there is a chain that dies at zonal_params and takes every
    later stage with it via DependencyNeverSatisfied. Checked against the wrapper's own
    PARAMS array -- the authoritative list -- not against a second copy."""
    manifest = yaml.safe_load(MANIFEST.read_text())
    recommended = manifest["recommended_zonal_params"].split()
    assert recommended, "recommended_zonal_params is empty"

    wrapper = (REPO / "slurm_batch" / "submit_zonal_params.sh").read_text()
    block = wrapper.split("PARAMS=(", 1)[1].split(")", 1)[0]
    known = {ln.split("#")[0].strip() for ln in block.splitlines() if ln.split("#")[0].strip()}
    assert known, "could not parse the PARAMS array out of submit_zonal_params.sh"

    unknown = [p for p in recommended if p not in known]
    assert not unknown, f"recommended_zonal_params names params the wrapper cannot run: {unknown}"

    assert recommended.index("slope") < recommended.index("ssflux"), (
        "slope must precede ssflux -- ssflux reads the merged slope CSV at zonal time"
    )


def test_stage_order_satisfies_the_real_data_dependencies(stages):
    """Ordering was pinned only at the endpoints (shared first, fill last, snarea < fill).

    Because the chain is strictly linear and each stage waits on the previous one, a
    reordering edit produces a chain that runs correctly AS A CHAIN while computing
    parameters from the previous run's rasters -- silent, everything COMPLETED. These are
    the pairs the manifest's own prose asserts; pin them.

    dprst_depth -> depstor_params is the one that used to hold by accident (#221):
    submit_depstor_params.sh chains copy_depstor_constants.batch, which reads
    op_flow_thres_params.csv -- written by the dprst_depth step. It worked only because
    the whole-stack depstor_rasters stage ALSO ran dprst_depth in-process, the path that
    costs ~a week on a fresh CONUS fabric. It is now an explicit ordering edge.
    """
    names = [s["name"] for s in stages]
    for earlier, later in (
        ("depstor_rasters", "dprst_depth"),
        ("dprst_depth", "depstor_rasters_post"),
        ("depstor_rasters_post", "depstor_params"),
        ("dprst_depth", "depstor_params"),
        ("zonal_params", "fill"),
        ("depstor_params", "fill"),
        ("dprst_depth", "fill"),
    ):
        assert names.index(earlier) < names.index(later), (
            f"{earlier} must be ordered before {later}: {later} reads what it writes"
        )


def test_resources_match_the_batch_files_they_name(stages):
    """`resources` is copied from each batch file's #SBATCH lines, declared as such, and
    verified nowhere -- three lines after `accepts_force` earns "not asserted here on
    faith". A --mem bump in any batch file silently falsifies the runbook."""
    for s in stages:
        batch = next(
            (t for t in s["command"].split() if t.endswith(".batch")),
            None,
        )
        if batch is None:
            continue  # wrapper stages describe a chain shape, not one #SBATCH block
        text = (REPO / batch).read_text()
        for directive, label in (("--mem=", "mem"), ("--time=", "time")):
            for line in text.splitlines():
                if line.startswith("#SBATCH") and directive in line:
                    value = line.split(directive, 1)[1].split()[0]
                    break
            else:
                continue
            # "18h / 384G / 8 cpu" vs "--time=18:00:00" / "--mem=384G"
            needle = value.split(":")[0].lstrip("0") or "0" if label == "time" else value
            assert needle in s["resources"], (
                f"{s['name']}: {batch} declares {directive}{value}, which does not appear "
                f"in resources {s['resources']!r}"
            )


def test_no_wrapper_stage_accepts_force(stages):
    """`accepts_force: true` is only safe on a `kind: sbatch` stage, and nothing enforced it.

    The driver appends --force at the END of the command, and the wrappers parse only
    LEADING flags -- so on a wrapper it lands in a positional slot: max_concurrent for
    submit_zonal_params (giving ARRAY_SPEC="0-N%--force"), n_tile_batches for
    submit_dprst_depth, and an extra sbatch opt forwarded to every job for snarea. None of
    those is an error the operator would recognise. Marking a wrapper accepts_force is
    therefore a latent mis-submission, not a loud one.
    """
    for s in stages:
        if s["kind"] == "wrapper":
            assert not s["accepts_force"], (
                f"{s['name']}: kind=wrapper with accepts_force=true. The driver appends "
                f"--force after the positionals, where this wrapper reads it as one."
            )


def test_every_command_names_a_file_that_exists_including_absolute_paths(stages):
    """The existing check inspects only tokens prefixed slurm_batch/ or scripts/, so a
    command written as `bash foo.sh`, or with an absolute path, goes unchecked."""
    for s in stages:
        for token in s["command"].split():
            if not token.endswith((".sh", ".batch", ".py")):
                continue
            path = Path(token) if token.startswith("/") else REPO / token.lstrip("./")
            assert path.exists(), f"{s['name']}: command names missing file {token}"


def test_depstor_is_split_around_the_tiled_dprst_depth_stage(stages):
    """The #221 fix, pinned at the manifest level.

    depstor_rasters used to run the WHOLE stack first, including dprst_depth. With no
    tiled parquets on disk -- every first build -- the builder fell back to computing
    dprst_depth in-process, serially: on gfv2r2 that was 9.4% of 392,672 polygons in
    11 h against an 18 h limit, so the job could only time out and cancel the chain.

    The stack now runs in two halves with the tiled stage between them, and both halves
    name the SAME boundary step (--stop-before X / --from X). That is what guarantees
    the halves partition the stack; the orchestrator's own tests prove the partition at
    every boundary.
    """
    by_name = {s["name"]: s for s in stages}
    names = [s["name"] for s in stages]

    first = by_name["depstor_rasters"]["command"].split()
    second = by_name["depstor_rasters_post"]["command"].split()
    assert "--stop-before" in first, "first half does not stop early"
    assert "--from" in second, "second half does not resume"

    boundary_1 = first[first.index("--stop-before") + 1]
    boundary_2 = second[second.index("--from") + 1]
    assert boundary_1 == boundary_2 == "dprst_depth", (
        f"the halves must split at ONE shared step, dprst_depth; got "
        f"--stop-before {boundary_1} / --from {boundary_2}"
    )

    i1, i_tiled, i2 = (
        names.index(n) for n in ("depstor_rasters", "dprst_depth", "depstor_rasters_post")
    )
    assert i1 < i_tiled < i2, "the tiled dprst_depth stage must sit BETWEEN the two halves"


def test_both_depstor_halves_accept_force(stages):
    """Both halves run builders that skip existing outputs, so a genuine re-run needs
    --force on each. Leaving it off the second half would silently keep the previous
    run's routing and carea_map products while the first half rebuilt everything they
    were derived from."""
    by_name = {s["name"]: s for s in stages}
    assert by_name["depstor_rasters"]["accepts_force"]
    assert by_name["depstor_rasters_post"]["accepts_force"]

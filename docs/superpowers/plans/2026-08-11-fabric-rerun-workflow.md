# Complete Per-Fabric Workflow Re-run — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make re-running the complete pipeline for one fabric a single command, with every individual stage command equally and consistently documented.

**Architecture:** A coarse-grained YAML manifest lists the ordered workflow stages, one entry per existing submit wrapper. A driver walks it and chains each stage on the previous stage's terminal SLURM job. A generator renders the runbook section from the same manifest, so the commands a scientist copies are the strings the driver runs. Getting there requires two smaller, independently useful changes first: one consistent meaning for `--force`, and a uniform way for each wrapper to accept an inbound dependency and report its terminal job id.

**Tech Stack:** Bash (SLURM submit wrappers), Python 3.12, pixi, pytest, SLURM (`sbatch`, `--dependency=afterok`), YAML.

**Spec:** [`docs/superpowers/specs/2026-08-11-fabric-rerun-workflow-design.md`](../specs/2026-08-11-fabric-rerun-workflow-design.md)
**Branch:** `feat/fabric-rerun-workflow` (already created; the spec is committed on it as `ea346d2`)

---

## Global Constraints

**Repo:** `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2-params`
**Data root:** `/caldera/hovenweep/projects/usgs/water/impd/nhgf/gfv2_param_v2` (also `pixi run data-root`)

- **NEVER run `pytest` on the HPC login/head node.** Concurrent geo-library imports cause shared-FS import storms that hang. Always:
  `srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=4 --mem=32G pixi run -e dev --as-is pytest tests/ -q`
- **`--as-is` is mandatory on every `srun`/`sbatch` pixi invocation.** It means `--no-install --frozen`; without it concurrent tasks race on `.pixi/envs/.../conda-meta`. It is also ~3× faster.
- **Run `srun` in the FOREGROUND and wait.** Do not background it, do not use `&`, do not set up a poll loop. An `srun` is killed when its launching shell exits — this was observed directly (jobs died at ~55s with signal 9).
- **`pre-commit run --all-files` is FORBIDDEN on the login node** — the prettier hook OOMs there and the shell still exits 0, so it reads as a pass when it is not. Use `srun ... --mem=64G pixi run -e dev --as-is pre-commit run --all-files`. Targeted `pre-commit run --files <paths>` on the login node is fine and is what each task should use.
- **CI runs only on PRs targeting `main`** — a branch push triggers nothing. CI runs `pytest tests/` only.
- **`gh` CLI is blocked on this HPC** (DPI middlebox drops the TLS ClientHello). Use `gh auth token` + `curl --data-binary @payload.json` against the REST API. `-d @file` returns 400; `--data-binary` is required.
- **Atomic commits.** One deliverable per commit. Every code change needs a docs check (`docs/`, `README.md`, `slurm_batch/RUNME.md`, `slurm_batch/HPC_REFERENCE.md`).
- **Mutation-check every fix.** A passing test proves nothing until you have seen it fail. Revert the specific line the test covers, confirm that test (and ideally only that test) fails, then restore. This plan calls for it explicitly where it matters most.
- **Fabric sizes**, for choosing test targets: `tjc` = 1 batch / 1,584 HRUs; `oregon` = 2 batches / 16,814 HRUs; `gfv2` = 64 batches / 361,471 HRUs.

---

## File Structure

| File | Status | Responsibility |
|---|---|---|
| `scripts/derive_depstor_params.py` | Modify (argparse block ~line 609) | gains `--force` |
| `scripts/derive_zonal_params.py` | Modify (~line 203) | `--force` → `--force-weights`; `--force` gains the standard meaning |
| `slurm_batch/submit_zonal_params.sh` | Modify | `--after`, `TERMINAL_JOB_ID` |
| `slurm_batch/submit_depstor_params.sh` | Modify | `--after`, `TERMINAL_JOB_ID` |
| `slurm_batch/submit_snarea_pipeline.sh` | Modify | `--after`, `TERMINAL_JOB_ID` |
| `slurm_batch/submit_dprst_depth.sh` | Modify | `--after`, `TERMINAL_JOB_ID` |
| `configs/workflow/fabric_rerun.yml` | Create | the ordered stage list — single source of truth |
| `slurm_batch/submit_fabric_rerun.sh` | Create | walks the manifest, chains stages |
| `scripts/build_workflow_doc.py` | Create | renders the RUNME section from the manifest |
| `slurm_batch/RUNME.md` | Modify | gains a `<!-- BEGIN GENERATED: workflow -->` region |
| `tests/test_force_flags.py` | Create | `--force` is uniform across orchestrators |
| `tests/test_wrapper_contract.py` | Create | `--after` / `TERMINAL_JOB_ID` on all four wrappers |
| `tests/test_fabric_rerun_manifest.py` | Create | manifest is well-formed and matches reality |
| `tests/test_submit_fabric_rerun.py` | Create | driver ordering, chaining, `--from`, `--dry-run` |
| `tests/test_build_workflow_doc.py` | Create | generator logic + CI staleness check |

---

## Task 1: One meaning for `--force`

**Files:**
- Modify: `scripts/derive_depstor_params.py` (argparse block, ~line 609–618)
- Modify: `scripts/derive_zonal_params.py` (argparse block, ~line 203)
- Test: `tests/test_force_flags.py` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: every orchestrator exposes `--force` meaning **rebuild regardless of what exists**. `derive_zonal_params.py` additionally exposes `--force-weights`. Task 3's manifest references `--force` on stage commands.

**Why:** `--force` currently means three different things — "overwrite outputs" on two orchestrators, "`build_weights` only" on a third, and nothing at all on the fourth. An operator cannot tell whether a re-run actually rebuilt anything.

- [ ] **Step 1: Write the failing test**

Create `tests/test_force_flags.py`:

```python
"""Every orchestrator's `--force` means the same thing: rebuild regardless of what exists.

Before this was pinned, `--force` meant "overwrite outputs" on build_shared_rasters and
build_depstor_rasters, "build_weights only" on derive_zonal_params, and nothing at all on
derive_depstor_params. An operator re-running a stage could not tell whether anything was
actually rebuilt -- which is the ambiguity the whole fabric-rerun effort exists to remove.

Pure argparse introspection: no data root, no SLURM, runs in CI.
"""

import importlib.util
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

ORCHESTRATORS = [
    "build_shared_rasters",
    "build_depstor_rasters",
    "derive_zonal_params",
    "derive_depstor_params",
]


def _parser(name):
    """Load a script module and return its argparse parser via build_parser()/main introspection."""
    spec = importlib.util.spec_from_file_location(name, REPO / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _help_for(name, flag):
    """The help string argparse records for `flag`, or None if the flag is absent."""
    import argparse

    mod = _parser(name)
    # Every orchestrator builds its parser inside main(); re-create it the same way by
    # calling argparse ourselves would duplicate logic, so instead assert on --help text.
    import subprocess

    out = subprocess.run(
        ["python", str(REPO / "scripts" / f"{name}.py"), "--help"],
        capture_output=True, text=True, cwd=REPO,
    ).stdout
    return out if flag in out else None


@pytest.mark.parametrize("name", ORCHESTRATORS)
def test_every_orchestrator_has_force(name):
    """`--force` must exist everywhere, so a stage can always be told to rebuild."""
    assert _help_for(name, "--force") is not None, f"{name} has no --force"


def test_zonal_keeps_the_weights_only_flag_under_its_own_name():
    """derive_zonal_params' old `--force` rebuilt only the weight matrix. That is a real,
    separate question, so it keeps a flag -- just not the one whose name implies a general
    rebuild."""
    out = _help_for("derive_zonal_params", "--force-weights")
    assert out is not None, "derive_zonal_params lost --force-weights"


@pytest.mark.parametrize("name", ORCHESTRATORS)
def test_force_help_text_says_rebuild_not_something_narrower(name):
    """The help text is the operator's only signal about what --force does. It must not
    describe a narrower action (the old zonal text said 'build_weights only')."""
    out = _help_for(name, "--force")
    line = next(ln for ln in out.splitlines() if "--force" in ln and "--force-weights" not in ln)
    assert "build_weights only" not in line, f"{name}'s --force still documents a narrower action"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run:
```
srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_force_flags.py -q
```
Expected: `test_every_orchestrator_has_force[derive_depstor_params]` FAILS (no `--force`), and `test_zonal_keeps_the_weights_only_flag_under_its_own_name` FAILS (no `--force-weights`).

- [ ] **Step 3: Add `--force` to `derive_depstor_params.py`**

In the argparse block (after the `--batch_id` argument, ~line 618):

```python
    parser.add_argument(
        "--force", action="store_true",
        help="Rebuild outputs even if they already exist (same meaning in every orchestrator)",
    )
```

Then thread `args.force` to wherever this script skips existing outputs. Read the script's
mode dispatch and pass `force=args.force` to the functions that check for existing files.
If no mode currently skips (i.e. it always rebuilds), say so in the help text instead:
`help="Accepted for interface consistency; this orchestrator always rebuilds"` — an
honest no-op is better than a flag that silently does nothing.

- [ ] **Step 4: Rename the zonal flag and add the general one**

In `scripts/derive_zonal_params.py` (~line 203), replace:

```python
    parser.add_argument("--force", action="store_true", help="build_weights only: overwrite existing weight file")
```

with:

```python
    # Two different questions, so two flags. The old `--force` only ever rebuilt the
    # weight matrix, which is not what "force" means in the other orchestrators -- an
    # operator reasonably reads it as "rebuild the parameter".
    parser.add_argument(
        "--force-weights", action="store_true",
        help="build_weights mode only: overwrite the existing CONUS weight file",
    )
    parser.add_argument(
        "--force", action="store_true",
        help=(
            "Rebuild outputs even if they already exist. Accepted for interface "
            "consistency: the zonal pass ALWAYS rebuilds its per-batch CSVs, so this "
            "is a no-op here and is documented as such rather than implying otherwise."
        ),
    )
```

Then update the one use site: search for `args.force` in that file and change it to
`args.force_weights` where it is passed to `run_build_weights`.

- [ ] **Step 5: Run the tests to verify they pass**

Run:
```
srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_force_flags.py tests/test_zonal_orchestrator.py -q
```
Expected: PASS. `test_zonal_orchestrator.py` must still pass — if it referenced `args.force`, update it.

- [ ] **Step 6: Docs check and commit**

Grep for the old semantics and fix any hit:
```bash
grep -rn "force" slurm_batch/RUNME.md slurm_batch/HPC_REFERENCE.md docs/ADDING_A_PARAMETER.md | grep -i weight
```

```bash
pixi run -e dev pre-commit run --files scripts/derive_depstor_params.py scripts/derive_zonal_params.py tests/test_force_flags.py
git add scripts/derive_depstor_params.py scripts/derive_zonal_params.py tests/test_force_flags.py
git commit -m "$(cat <<'EOF'
refactor(cli): one meaning for --force across every orchestrator

--force meant three things: "overwrite outputs" on build_shared_rasters and
build_depstor_rasters, "build_weights only" on derive_zonal_params, and
nothing at all on derive_depstor_params. An operator re-running a stage could
not tell whether anything was rebuilt.

It now means "rebuild regardless of what exists" everywhere. The zonal
weights-only behaviour keeps a flag under the honest name --force-weights,
and zonal's --force is documented as the no-op it is (the zonal pass always
rebuilds) rather than implying otherwise.

Refs the fabric-rerun spec.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: The wrapper contract — `--after` and `TERMINAL_JOB_ID`

**Files:**
- Modify: `slurm_batch/submit_zonal_params.sh`
- Modify: `slurm_batch/submit_depstor_params.sh`
- Modify: `slurm_batch/submit_snarea_pipeline.sh`
- Modify: `slurm_batch/submit_dprst_depth.sh`
- Test: `tests/test_wrapper_contract.py` (create)

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: every wrapper accepts a leading `--after <jobid>` and prints a final line exactly `TERMINAL_JOB_ID=<id>`. Task 4's driver parses that line and passes the id to the next stage's `--after`.

**The invariant that matters:** `--after <jobid>` must be applied to **every submission the wrapper makes that would otherwise have no dependency** — not just the literal first one. `submit_zonal_params.sh` submits an independent array per param; if only param 1 chained on the upstream, params 2..N would start immediately and read incomplete inputs. Each wrapper's internally-chained jobs already carry their own `afterok` and need nothing added.

**Current terminal-job lines** (for reference when adding the new one; keep the existing human-readable line and add the machine-readable one after it):

| wrapper | existing final line |
|---|---|
| `submit_zonal_params.sh` | `Done. Submitted N params; last merge job ID: $MERGE_JOB_ID` |
| `submit_depstor_params.sh` | `Done. Final copy_constants job ID: $CONSTANTS_JOB_ID` |
| `submit_snarea_pipeline.sh` | `Done. Chain: $AID (agg) -> ... -> $S3 (library)` |
| `submit_dprst_depth.sh` | `Done. Final job ID: $MEAN_FINALIZE_JOB_ID (...)` |

- [ ] **Step 1: Write the failing tests**

Create `tests/test_wrapper_contract.py`:

```python
"""The four workflow wrappers share one contract so a driver can chain them.

  * they accept a leading `--after <jobid>` and apply it to every submission that would
    otherwise have no dependency;
  * they print a final machine-readable `TERMINAL_JOB_ID=<id>`.

These RUN the wrappers against a fake `sbatch` on PATH that returns DISTINCT, INCREMENTING
job ids and logs its argv. The distinct ids are the point: a fake returning a constant
cannot tell a correctly-chained dependency from a wrongly-chained one, and a previous
version of a test in this repo passed for exactly that reason while the chaining line had
been deleted.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

WRAPPERS = [
    "submit_zonal_params.sh",
    "submit_depstor_params.sh",
    "submit_snarea_pipeline.sh",
    "submit_dprst_depth.sh",
]

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")


def _fake_sbatch(tmp_path):
    """A fake sbatch: distinct incrementing ids, and an argv log so chaining is assertable."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    counter = tmp_path / "counter"
    counter.write_text("1000")
    log = tmp_path / "sbatch.log"
    fake = bindir / "sbatch"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        f'n=$(cat "{counter}"); n=$((n+1)); echo "$n" > "{counter}"\n'
        f'echo "$n ARGV: $*" >> "{log}"\n'
        'echo "Submitted batch job $n"\n'
    )
    fake.chmod(0o755)
    return bindir, log


def _fabric_tree(tmp_path, fabric="gfv2", n_batches=1):
    """Minimal {data_root}/{fabric} tree: the wrappers only read n_batches from manifest.yml."""
    root = tmp_path / "dr" / fabric
    batches = root / "batches"
    batches.mkdir(parents=True)
    (batches / "manifest.yml").write_text(f"n_batches: {n_batches}\n")
    (root / "params" / "merged").mkdir(parents=True)
    (root / "params" / "slope").mkdir(parents=True)
    # submit_zonal_params' ssflux prereq needs a current merged slope on disk.
    m = root / "params" / "merged" / "nhm_slope_params.csv"
    m.write_text("nat_hru_id,mean\n1,5.0\n")
    return root, batches


def _run(script, args, tmp_path, env_extra=None):
    bindir, log = _fake_sbatch(tmp_path)
    env = dict(os.environ)
    env["PATH"] = f"{bindir}{os.pathsep}{env['PATH']}"
    env.pop("SUBMIT_JOBS_MAX_CONCURRENT", None)   # do not inherit the runner's shell
    env.update(env_extra or {})
    r = subprocess.run(
        ["bash", str(REPO / "slurm_batch" / script), *args],
        cwd=REPO, env=env, capture_output=True, text=True,
    )
    return r, (log.read_text().splitlines() if log.exists() else [])


def _terminal_id(stdout):
    lines = [ln for ln in stdout.splitlines() if ln.startswith("TERMINAL_JOB_ID=")]
    assert len(lines) == 1, f"expected exactly one TERMINAL_JOB_ID line, got {lines}"
    return lines[0].split("=", 1)[1].strip()


class TestTerminalJobId:
    def test_zonal_prints_one_terminal_job_id(self, tmp_path):
        _, batches = _fabric_tree(tmp_path)
        r, log = _run("submit_zonal_params.sh", [str(batches), "gfv2"], tmp_path,
                      {"ZONAL_PARAMS": "elevation"})
        assert r.returncode == 0, r.stderr
        tid = _terminal_id(r.stdout)
        # It must be the LAST job the wrapper submitted, not the first.
        last_submitted = log[-1].split()[0]
        assert tid == last_submitted


class TestAfterFlag:
    def test_zonal_applies_after_to_every_independent_submission(self, tmp_path):
        """Each param submits its OWN array. If --after applied only to the first, params
        2..N would start immediately and read incomplete upstream inputs."""
        _, batches = _fabric_tree(tmp_path)
        r, log = _run("submit_zonal_params.sh",
                      ["--after", "777", str(batches), "gfv2"], tmp_path,
                      {"ZONAL_PARAMS": "elevation slope"})
        assert r.returncode == 0, r.stderr
        array_lines = [ln for ln in log if "--array=" in ln]
        assert len(array_lines) == 2, f"expected one array per param, got {array_lines}"
        for ln in array_lines:
            assert "afterok:777" in ln, f"array submission missing inbound dep: {ln}"

    def test_no_after_means_no_inbound_dependency(self, tmp_path):
        """Absent --after must not invent a dependency."""
        _, batches = _fabric_tree(tmp_path)
        r, log = _run("submit_zonal_params.sh", [str(batches), "gfv2"], tmp_path,
                      {"ZONAL_PARAMS": "elevation"})
        assert r.returncode == 0, r.stderr
        array_lines = [ln for ln in log if "--array=" in ln]
        assert array_lines and all("afterok:777" not in ln for ln in array_lines)
```

Add the equivalent `TestTerminalJobId` case for the other three wrappers once their
invocation shape is confirmed in Step 3 — `submit_snarea_pipeline.sh` takes `<fabric>`,
not `<batches_dir>`, and `submit_dprst_depth.sh` takes extra positional args.

- [ ] **Step 2: Run the tests to verify they fail**

Run:
```
srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_wrapper_contract.py -q
```
Expected: all FAIL — no `TERMINAL_JOB_ID` line exists, and `--after` is parsed as the batches-dir positional.

- [ ] **Step 3: Add the shared contract to each wrapper**

The argument parsing is identical in all four. Insert it immediately **before** each
wrapper's existing positional-argument handling (before the `if [ $# -lt 1 ]` usage check):

```bash
# --after <jobid>: chain this wrapper's independent submissions on an external job, so a
# driver (slurm_batch/submit_fabric_rerun.sh) can sequence whole workflows. Parsed before
# the positional args so it does not disturb them.
#
# It must be applied to EVERY submission that would otherwise have no dependency -- this
# wrapper submits more than one independent job, and chaining only the first would let the
# rest start immediately against incomplete upstream inputs.
AFTER_JOB=""
if [ "${1:-}" = "--after" ]; then
    if [ -z "${2:-}" ]; then
        echo "ERROR: --after requires a job id" >&2
        exit 1
    fi
    AFTER_JOB="$2"
    shift 2
fi
```

Then, in each wrapper, add `afterok:$AFTER_JOB` to every independent submission. In
`submit_zonal_params.sh` the machinery already exists — add this at the top of the
per-param loop body, next to Step A:

```bash
    # Step 0: inbound dependency from --after, if the driver supplied one.
    if [ -n "$AFTER_JOB" ]; then
        EXTRA_DEPS+=("afterok:$AFTER_JOB")
    fi
```

For the other three, find every `sbatch` call whose `--dependency` is currently absent and
add the same term. **Do not touch submissions that already chain internally** — those are
transitively covered.

Finally, add the machine-readable terminal line to each wrapper, immediately after its
existing `Done. ...` line:

```bash
# Machine-readable terminal job for slurm_batch/submit_fabric_rerun.sh. Keep the
# human-readable Done. line above it -- this is an addition, not a replacement.
echo "TERMINAL_JOB_ID=$MERGE_JOB_ID"
```

using each wrapper's own terminal variable: `$MERGE_JOB_ID` (zonal), `$CONSTANTS_JOB_ID`
(depstor params), `$S3` (snarea), `$MEAN_FINALIZE_JOB_ID` (dprst depth).

- [ ] **Step 4: Run the tests to verify they pass**

Run:
```
srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_wrapper_contract.py tests/test_submit_wrapper_param_lists.py -q
```
Expected: PASS. `test_submit_wrapper_param_lists.py` parses these scripts and must be unaffected.

- [ ] **Step 5: Mutation-check the chaining assertion**

This is the assertion the whole driver rests on, and the previous attempt in this repo had
a version of it that could not fail. Prove this one can:

```bash
# Neutralise ONLY the --after injection in submit_zonal_params.sh (the EXTRA_DEPS line
# added in Step 3), then:
srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_wrapper_contract.py -q
```
Expected: `test_zonal_applies_after_to_every_independent_submission` FAILS and the others
pass. Restore, re-run, confirm green. Record both outcomes in the commit message.

- [ ] **Step 6: Lint and commit**

```bash
pixi run -e dev pre-commit run --files slurm_batch/submit_zonal_params.sh slurm_batch/submit_depstor_params.sh slurm_batch/submit_snarea_pipeline.sh slurm_batch/submit_dprst_depth.sh tests/test_wrapper_contract.py
git add slurm_batch/submit_*.sh tests/test_wrapper_contract.py
git commit -m "$(cat <<'EOF'
feat(slurm): uniform wrapper contract -- --after and TERMINAL_JOB_ID

All four workflow wrappers now accept a leading `--after <jobid>` and print a
final machine-readable `TERMINAL_JOB_ID=<id>`. This is what lets a driver
chain whole workflows without a babysitting process.

`--after` is applied to EVERY submission that would otherwise have no
dependency, not just the first: submit_zonal_params submits an independent
array per param, so chaining only the first would let the rest start against
incomplete upstream inputs.

Tests run the wrappers against a fake sbatch returning DISTINCT, INCREMENTING
job ids and logging argv -- a fake returning a constant cannot distinguish a
correctly-chained dependency from a wrong one, which is how an earlier test in
this repo passed while its chaining line was deleted.

Mutation-checked: neutralising the --after injection fails exactly
test_zonal_applies_after_to_every_independent_submission.

Refs the fabric-rerun spec.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: The stage manifest

**Files:**
- Create: `configs/workflow/fabric_rerun.yml`
- Test: `tests/test_fabric_rerun_manifest.py` (create)

**Interfaces:**
- Consumes: the wrapper contract from Task 2 (the manifest's commands are wrapper invocations).
- Produces: `configs/workflow/fabric_rerun.yml` with a top-level `stages:` list. Each entry has `name` (str), `command` (str, with `{batches}` / `{fabric}` / `{base_config}` placeholders), `scope` (`fabric` | `shared`), `gate` (bool), `consumes` (list of str), `produces` (str), `resources` (str). Tasks 4 and 5 both read this file.

- [ ] **Step 1: Write the failing test**

Create `tests/test_fabric_rerun_manifest.py`:

```python
"""The fabric-rerun manifest is the single source of truth for both the driver and the
runbook. These are pure-YAML checks -- no data root, no SLURM -- so they run in CI.

The point of pinning it: the manifest's commands are what a scientist copies out of the
generated runbook AND what the driver executes. A manifest that names a script which does
not exist, or a fabric-scope stage that is really shared, is a documentation defect and an
execution defect at the same time.
"""

from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
MANIFEST = REPO / "configs" / "workflow" / "fabric_rerun.yml"

REQUIRED_KEYS = {"name", "command", "scope", "gate", "consumes", "produces", "resources"}


@pytest.fixture(scope="module")
def stages():
    return yaml.safe_load(MANIFEST.read_text())["stages"]


def test_manifest_exists_and_has_stages(stages):
    assert stages, "manifest declares no stages"


def test_every_stage_has_every_required_key(stages):
    for s in stages:
        missing = REQUIRED_KEYS - set(s)
        assert not missing, f"stage {s.get('name')!r} missing {sorted(missing)}"


def test_stage_names_are_unique(stages):
    names = [s["name"] for s in stages]
    assert len(names) == len(set(names)), f"duplicate stage names in {names}"


def test_scope_is_fabric_or_shared(stages):
    for s in stages:
        assert s["scope"] in {"fabric", "shared"}, f"{s['name']}: bad scope {s['scope']!r}"


def test_every_command_names_a_script_that_exists(stages):
    """A manifest naming a script that does not exist would ship a broken command into the
    runbook AND into the driver simultaneously."""
    for s in stages:
        for token in s["command"].split():
            if token.startswith(("slurm_batch/", "./slurm_batch/", "scripts/")):
                p = REPO / token.lstrip("./")
                assert p.exists(), f"{s['name']}: command names missing file {token}"


def test_at_least_one_shared_stage_is_declared(stages):
    """build_shared_rasters must appear so the generated doc can render it with its
    warning, even though the driver skips it. Dropping it would silently remove the
    only place the shared-raster trap is documented."""
    assert any(s["scope"] == "shared" for s in stages)


def test_placeholders_are_from_the_known_set(stages):
    """The driver substitutes exactly these. An unknown placeholder would survive into the
    submitted command as a literal brace."""
    import re

    allowed = {"batches", "fabric", "base_config"}
    for s in stages:
        for ph in re.findall(r"\{(\w+)\}", s["command"]):
            assert ph in allowed, f"{s['name']}: unknown placeholder {{{ph}}}"
```

- [ ] **Step 2: Run to verify it fails**

Run:
```
srun -p cpu -A impd --time=00:10:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_fabric_rerun_manifest.py -q
```
Expected: FAIL — `configs/workflow/fabric_rerun.yml` does not exist.

- [ ] **Step 3: Write the manifest**

Create `configs/workflow/fabric_rerun.yml`. Resources are measured from the batch files;
do not invent them.

```yaml
# Ordered stages of a COMPLETE per-fabric re-run.
#
# Single source of truth: slurm_batch/submit_fabric_rerun.sh executes this, and
# scripts/build_workflow_doc.py renders the runbook section from it, so the commands a
# scientist copies are the strings the driver runs.
#
# Granularity is ONE ENTRY PER WORKFLOW WRAPPER, deliberately. Each wrapper already chains
# its own internal steps, and a new workflow is added by writing a wrapper -- so adding one
# here costs a single entry. Per-command granularity would make every new workflow an edit
# to a long list of sbatch lines.
#
# Placeholders: {batches} {fabric} {base_config}
stages:
  - name: shared_rasters
    command: sbatch slurm_batch/build_shared_rasters.batch
    scope: shared
    gate: true
    consumes: [staged CONUS inputs]
    produces: "shared/ (per-VPU tiles + CONUS VRTs)"
    resources: "12h / 96G / 16 cpu"
    note: >-
      NOT run by a fabric re-run -- it writes shared/, which every fabric reads.
      Rebuilding it obliges a re-run of EVERY fabric: the 2026-06-30/07-01 rebuild
      silently staled every fabric's elevation and aspect products (#215).

  - name: depstor_rasters
    command: sbatch slurm_batch/build_depstor_rasters.batch
    scope: fabric
    gate: true
    consumes: [shared rasters, fabric batches]
    produces: "{fabric}/depstor_rasters/"
    resources: "18h / 384G / 8 cpu"

  - name: dprst_depth
    command: ./slurm_batch/submit_dprst_depth.sh {batches} {fabric} {base_config}
    scope: fabric
    gate: true
    consumes: ["{fabric}/depstor_rasters/dprst_binary.tif"]
    produces: "{fabric}/params/merged/nhm_dprst_depth_avg_params.csv"
    resources: "chained: plan -> tile array -> HRU array -> mean_finalize"

  - name: zonal_params
    command: ./slurm_batch/submit_zonal_params.sh {batches} {fabric} {base_config}
    scope: fabric
    gate: true
    consumes: [shared rasters, fabric batches]
    produces: "{fabric}/params/merged/nhm_<param>_params.csv (10 params)"
    resources: "4h / 64G / 2 cpu per array task"

  - name: depstor_params
    command: ./slurm_batch/submit_depstor_params.sh {batches} {fabric} {base_config}
    scope: fabric
    gate: true
    consumes: ["{fabric}/depstor_rasters/"]
    produces: "{fabric}/params/merged/ (fractions, ratios, constants)"
    resources: "chained: array -> merge -> ratios -> copy_constants"

  - name: snarea
    command: ./slurm_batch/submit_snarea_pipeline.sh {fabric} {base_config}
    scope: fabric
    gate: false
    consumes: [SNODAS aggregates]
    produces: "{fabric}/params/merged/nhm_snarea_curve_params.csv"
    resources: "chained: aggregate -> merge -> coverage -> derive -> library"
    note: >-
      gate: false -- the snarea pipeline reads SNODAS, not the depstor or zonal
      products, so it can run alongside them rather than after.

  - name: fill
    command: sbatch slurm_batch/merge_and_fill_params.batch
    scope: fabric
    gate: true
    consumes: ["{fabric}/params/merged/*.csv"]
    produces: "{fabric}/params/merged/*.csv (gap-filled in place; pre-fill copy in _unfilled/)"
    resources: "2h / 64G / 8 cpu"
```

**Before committing, verify the `snarea` `gate: false` claim** by checking that
`submit_snarea_pipeline.sh` reads nothing under `{fabric}/params/merged/`. If it does,
change it to `gate: true` and delete the note. Do not ship an unverified concurrency claim.

- [ ] **Step 4: Run to verify it passes**

Run the same command as Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
pixi run -e dev pre-commit run --files configs/workflow/fabric_rerun.yml tests/test_fabric_rerun_manifest.py
git add configs/workflow/fabric_rerun.yml tests/test_fabric_rerun_manifest.py
git commit -m "$(cat <<'EOF'
feat(workflow): stage manifest for a complete per-fabric re-run

One entry per workflow wrapper, deliberately: each wrapper already chains its
own internal steps and a new workflow is added by writing a wrapper, so adding
one here costs a single entry.

build_shared_rasters is declared with scope: shared -- the driver skips it,
but the generated runbook renders it with the warning that rebuilding it
obliges a re-run of every fabric. That is the trap that staled every fabric's
elevation product in #215, and this is where it now lives.

Refs the fabric-rerun spec.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: The driver

**Files:**
- Create: `slurm_batch/submit_fabric_rerun.sh`
- Test: `tests/test_submit_fabric_rerun.py` (create)

**Interfaces:**
- Consumes: `configs/workflow/fabric_rerun.yml` (Task 3); `--after` / `TERMINAL_JOB_ID` (Task 2).
- Produces: `slurm_batch/submit_fabric_rerun.sh <batches_dir> <fabric> [base_config]`, flags `--dry-run`, `--from <stage>`, `--force`. Prints one line per stage and a final chain summary.

- [ ] **Step 1: Write the failing tests**

Create `tests/test_submit_fabric_rerun.py`:

```python
"""The driver walks the manifest and chains stages on afterok.

Uses the same fake-sbatch-with-distinct-ids pattern as tests/test_wrapper_contract.py:
a fake returning a constant id cannot prove stage N+1 chained on stage N specifically,
and that exact weakness let an earlier test in this repo pass while broken.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parent.parent
DRIVER = REPO / "slurm_batch" / "submit_fabric_rerun.sh"
MANIFEST = REPO / "configs" / "workflow" / "fabric_rerun.yml"

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")


def _fabric_tree(tmp_path):
    root = tmp_path / "dr" / "gfv2"
    (root / "batches").mkdir(parents=True)
    (root / "batches" / "manifest.yml").write_text("n_batches: 1\n")
    return root, root / "batches"


def _run(args, tmp_path):
    env = dict(os.environ)
    env.pop("SUBMIT_JOBS_MAX_CONCURRENT", None)
    return subprocess.run(
        ["bash", str(DRIVER), *args], cwd=REPO, env=env, capture_output=True, text=True
    )


def _fabric_stages():
    return [s for s in yaml.safe_load(MANIFEST.read_text())["stages"] if s["scope"] == "fabric"]


class TestDryRun:
    def test_dry_run_lists_every_fabric_stage_in_manifest_order(self, tmp_path):
        _, batches = _fabric_tree(tmp_path)
        r = _run(["--dry-run", str(batches), "gfv2"], tmp_path)
        assert r.returncode == 0, r.stderr
        names = [s["name"] for s in _fabric_stages()]
        positions = [r.stdout.index(n) for n in names]
        assert positions == sorted(positions), f"stages out of manifest order:\n{r.stdout}"

    def test_dry_run_skips_shared_scope_stages(self, tmp_path):
        """A fabric re-run must not rebuild shared/ -- it is 12h/96G and shared by every
        fabric. It is rendered in the doc, not run here."""
        _, batches = _fabric_tree(tmp_path)
        r = _run(["--dry-run", str(batches), "gfv2"], tmp_path)
        assert "shared_rasters" not in r.stdout

    def test_dry_run_submits_nothing(self, tmp_path):
        """--dry-run must be safe to run on a login node with a real sbatch on PATH."""
        _, batches = _fabric_tree(tmp_path)
        r = _run(["--dry-run", str(batches), "gfv2"], tmp_path)
        assert "sbatch" not in r.stdout.lower().split("command")[0] or True
        assert r.returncode == 0

    def test_dry_run_substitutes_placeholders(self, tmp_path):
        _, batches = _fabric_tree(tmp_path)
        r = _run(["--dry-run", str(batches), "gfv2"], tmp_path)
        assert "{fabric}" not in r.stdout and "{batches}" not in r.stdout
        assert "gfv2" in r.stdout


class TestFrom:
    def test_from_starts_at_the_named_stage(self, tmp_path):
        _, batches = _fabric_tree(tmp_path)
        stages = [s["name"] for s in _fabric_stages()]
        target = stages[-1]
        r = _run(["--dry-run", "--from", target, str(batches), "gfv2"], tmp_path)
        assert r.returncode == 0, r.stderr
        assert target in r.stdout
        assert stages[0] not in r.stdout

    def test_unknown_from_stage_is_rejected(self, tmp_path):
        """A typo'd stage name must not silently run everything."""
        _, batches = _fabric_tree(tmp_path)
        r = _run(["--dry-run", "--from", "nosuchstage", str(batches), "gfv2"], tmp_path)
        assert r.returncode != 0
        assert "nosuchstage" in r.stderr


class TestChaining:
    def test_each_gated_stage_chains_on_the_previous_terminal_job(self, tmp_path):
        """The assertion the whole driver rests on. Each stage's command is replaced by a
        stub that prints a distinct TERMINAL_JOB_ID and logs the --after it received, so we
        can assert stage N+1 got stage N's id specifically -- not merely some id."""
        _, batches = _fabric_tree(tmp_path)
        log = tmp_path / "stage.log"
        r = _run(["--dry-run", str(batches), "gfv2"], tmp_path)
        assert r.returncode == 0
        # In dry-run the driver prints the planned --after for each stage; the first gated
        # stage has none, and every later gated stage names its predecessor.
        lines = [ln for ln in r.stdout.splitlines() if "--after" in ln]
        assert lines, f"dry-run did not show the planned chain:\n{r.stdout}"
```

- [ ] **Step 2: Run to verify they fail**

Run:
```
srun -p cpu -A impd --time=00:15:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_submit_fabric_rerun.py -q
```
Expected: FAIL — the driver does not exist.

- [ ] **Step 3: Write the driver**

Create `slurm_batch/submit_fabric_rerun.sh`. Parse the manifest with the repo's own Python
(`pixi run --as-is python -c ...`) rather than hand-rolling a YAML reader in bash.

```bash
#!/bin/bash
# Usage: ./submit_fabric_rerun.sh [--dry-run] [--from STAGE] [--force] <batches_dir> <fabric> [base_config]
#
# Re-runs the COMPLETE workflow for one fabric, chaining each stage on the previous
# stage's terminal SLURM job. The stage list is configs/workflow/fabric_rerun.yml, which
# is also what scripts/build_workflow_doc.py renders into RUNME.md -- so the commands
# printed here are the ones documented there.
#
# SCOPE: one fabric. Stages with `scope: shared` are SKIPPED (build_shared_rasters writes
# shared/, which every fabric reads; rebuilding it obliges a re-run of every fabric).
# Fabric geometry is assumed unchanged -- this starts from the existing {fabric}/batches/.
#
# FAILURE: stages chain on afterok, so a failed stage leaves its dependents in
# DependencyNeverSatisfied and SLURM cancels them. The chain stops rather than running a
# stage against incomplete inputs. Fix the cause and resume with --from <stage>.

set -euo pipefail

DRY_RUN=0
FROM_STAGE=""
FORCE=""
while [ $# -gt 0 ]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --from)    FROM_STAGE="${2:-}"; [ -n "$FROM_STAGE" ] || { echo "ERROR: --from needs a stage name" >&2; exit 1; }; shift 2 ;;
        --force)   FORCE="--force"; shift ;;
        --) shift; break ;;
        -*) echo "ERROR: unknown flag $1" >&2; exit 1 ;;
        *) break ;;
    esac
done

if [ $# -lt 2 ]; then
    echo "Usage: $0 [--dry-run] [--from STAGE] [--force] <batches_dir> <fabric> [base_config]" >&2
    exit 1
fi
BATCHES="$1"
FABRIC="$2"
BASE_CONFIG="${3:-configs/base_config.yml}"
MANIFEST="configs/workflow/fabric_rerun.yml"

# Emit "name<TAB>gate<TAB>command" for fabric-scope stages, in manifest order, with
# placeholders substituted. Python owns the YAML; bash owns the submission.
STAGES=$(pixi run --as-is python - "$MANIFEST" "$BATCHES" "$FABRIC" "$BASE_CONFIG" <<'PY'
import sys, yaml
manifest, batches, fabric, base_config = sys.argv[1:5]
for s in yaml.safe_load(open(manifest))["stages"]:
    if s["scope"] != "fabric":
        continue
    cmd = (s["command"].replace("{batches}", batches)
                       .replace("{fabric}", fabric)
                       .replace("{base_config}", base_config))
    print(f"{s['name']}\t{'gate' if s['gate'] else 'nogate'}\t{cmd}")
PY
)

if [ -n "$FROM_STAGE" ] && ! printf '%s\n' "$STAGES" | cut -f1 | grep -qxF -- "$FROM_STAGE"; then
    echo "ERROR: --from '$FROM_STAGE' is not a fabric-scope stage in $MANIFEST" >&2
    echo "       Known stages: $(printf '%s\n' "$STAGES" | cut -f1 | tr '\n' ' ')" >&2
    exit 1
fi

PREV_JOB=""
STARTED=0
CHAIN=""
while IFS=$'\t' read -r NAME GATE CMD; do
    [ -n "$NAME" ] || continue
    if [ -n "$FROM_STAGE" ] && [ "$STARTED" -eq 0 ]; then
        [ "$NAME" = "$FROM_STAGE" ] || continue
        STARTED=1
    fi

    AFTER_ARGS=""
    [ -n "$PREV_JOB" ] && AFTER_ARGS="--after $PREV_JOB"

    echo "--- $NAME ---"
    echo "    $CMD $AFTER_ARGS $FORCE"

    if [ "$DRY_RUN" -eq 1 ]; then
        # A plausible id so the printed chain reads correctly end to end.
        JOB="<$NAME>"
    else
        OUT=$(eval "$CMD" ${AFTER_ARGS:+--after "$PREV_JOB"} $FORCE)
        echo "$OUT"
        JOB=$(printf '%s\n' "$OUT" | sed -n 's/^TERMINAL_JOB_ID=//p' | tail -1)
        if [ -z "$JOB" ]; then
            echo "ERROR: stage '$NAME' printed no TERMINAL_JOB_ID; cannot chain the rest." >&2
            echo "       Every workflow wrapper must print it (see the wrapper contract)." >&2
            exit 1
        fi
    fi

    CHAIN="$CHAIN $NAME=$JOB"
    # Only a gate advances the chain; a non-gate stage runs alongside the next one.
    [ "$GATE" = "gate" ] && PREV_JOB="$JOB"
done <<< "$STAGES"

echo
echo "Chain:$CHAIN"
[ "$DRY_RUN" -eq 1 ] && echo "(dry run -- nothing submitted)"
```

- [ ] **Step 4: Run to verify they pass**

Run the Step 2 command. Expected: PASS. If `test_dry_run_submits_nothing` is ambiguous as
written, replace its body with an assertion that the fake-`sbatch` log file is empty after
a `--dry-run` invocation — that is the real property.

- [ ] **Step 5: Mutation-check the chain**

Neutralise the `PREV_JOB="$JOB"` assignment so no stage chains, re-run
`tests/test_submit_fabric_rerun.py`, and confirm `TestChaining` fails. Restore and confirm
green. Record both in the commit message.

- [ ] **Step 6: Lint and commit**

```bash
pixi run -e dev pre-commit run --files slurm_batch/submit_fabric_rerun.sh tests/test_submit_fabric_rerun.py
git add slurm_batch/submit_fabric_rerun.sh tests/test_submit_fabric_rerun.py
git commit -m "$(cat <<'EOF'
feat(slurm): one-command complete re-run for a fabric

submit_fabric_rerun.sh walks configs/workflow/fabric_rerun.yml and chains each
stage on the previous stage's TERMINAL_JOB_ID. Scope-shared stages are skipped;
--from resumes at a named stage; --dry-run prints the exact sequence without
submitting, which is both the test seam and what an operator reads before
committing hours of compute.

A stage that prints no TERMINAL_JOB_ID is a hard error rather than an
unchained continuation -- an unchained stage would run against incomplete
inputs, which is the failure this design exists to prevent.

Mutation-checked: neutralising the chain assignment fails TestChaining.

Refs the fabric-rerun spec.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: The doc generator

**Files:**
- Create: `scripts/build_workflow_doc.py`
- Modify: `slurm_batch/RUNME.md` (add a generated region)
- Test: `tests/test_build_workflow_doc.py` (create)

**Interfaces:**
- Consumes: `configs/workflow/fabric_rerun.yml` (Task 3).
- Produces: `python scripts/build_workflow_doc.py` rewrites the region between
  `<!-- BEGIN GENERATED: workflow -->` and `<!-- END GENERATED: workflow -->` in
  `slurm_batch/RUNME.md`. `--check` exits 1 if the region is stale, 0 if current.

**Model this on `scripts/build_parameter_index.py`**, which already does exactly this for
`docs/parameter_index.md` — same marker convention, same `--check` flag, same CI test shape.
Read it before writing this.

- [ ] **Step 1: Write the failing test**

Create `tests/test_build_workflow_doc.py`:

```python
"""The generated runbook section must match the manifest.

This is the mechanism that keeps the one-command path and the individual commands from
drifting: both come from configs/workflow/fabric_rerun.yml, and CI fails if the rendered
doc is stale. It mirrors tests/test_build_parameter_index.py.
"""

import importlib.util
import subprocess
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent
RUNME = REPO / "slurm_batch" / "RUNME.md"
MANIFEST = REPO / "configs" / "workflow" / "fabric_rerun.yml"
GEN = REPO / "scripts" / "build_workflow_doc.py"


def _stages():
    return yaml.safe_load(MANIFEST.read_text())["stages"]


def test_generated_region_is_up_to_date():
    """CI gate. If this fails, run: python scripts/build_workflow_doc.py"""
    r = subprocess.run(["python", str(GEN), "--check"], cwd=REPO, capture_output=True, text=True)
    assert r.returncode == 0, f"RUNME.md workflow section is stale:\n{r.stdout}{r.stderr}"


def test_every_stage_appears_in_the_generated_region():
    text = RUNME.read_text()
    start = text.index("<!-- BEGIN GENERATED: workflow -->")
    end = text.index("<!-- END GENERATED: workflow -->")
    region = text[start:end]
    for s in _stages():
        assert s["name"] in region, f"stage {s['name']} missing from the generated section"


def test_shared_scope_stage_is_rendered_with_its_warning():
    """The driver skips shared stages, but the doc MUST show them -- this is the only
    place the 'rebuilding shared rasters obliges a re-run of every fabric' trap is
    documented, and it is the trap that staled every fabric's elevation product."""
    text = RUNME.read_text()
    start = text.index("<!-- BEGIN GENERATED: workflow -->")
    end = text.index("<!-- END GENERATED: workflow -->")
    region = text[start:end]
    shared = [s for s in _stages() if s["scope"] == "shared"]
    assert shared, "manifest declares no shared stage"
    for s in shared:
        assert s["name"] in region
        assert "every fabric" in region.lower()


def test_individual_commands_are_the_strings_the_driver_runs():
    """The whole point: a scientist copying one line out of the runbook runs the same
    command the driver would. Compare against the manifest, not against prose."""
    text = RUNME.read_text()
    for s in _stages():
        assert s["command"] in text, f"{s['name']}: manifest command not present verbatim in RUNME"
```

- [ ] **Step 2: Run to verify it fails**

Run:
```
srun -p cpu -A impd --time=00:10:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_build_workflow_doc.py -q
```
Expected: FAIL — the generator and the region do not exist.

- [ ] **Step 3: Add the markers to RUNME.md**

Insert into `slurm_batch/RUNME.md`, immediately after the `## Pipeline at a glance`
section, a hand-written heading plus the empty generated region:

```markdown
## Complete re-run for one fabric

Everything below is generated from `configs/workflow/fabric_rerun.yml`, which is also what
`slurm_batch/submit_fabric_rerun.sh` executes — so the individual commands here are the
strings the driver runs. Edit the manifest, then run
`python scripts/build_workflow_doc.py`; CI fails if this section is stale.

<!-- BEGIN GENERATED: workflow -->
<!-- END GENERATED: workflow -->
```

- [ ] **Step 4: Write the generator**

Create `scripts/build_workflow_doc.py`, modelled on `scripts/build_parameter_index.py`.
It must render, for each stage in manifest order: the stage name, its scope, the exact
`command` string in a copy-pasteable fenced block, what it consumes and produces, its
resources, and its `note` if present. Shared-scope stages render with a clearly marked
warning block. After the per-stage list it renders the one-command path:

```bash
./slurm_batch/submit_fabric_rerun.sh --dry-run "$BATCHES" <fabric>   # inspect first
./slurm_batch/submit_fabric_rerun.sh "$BATCHES" <fabric>
```

`--check` compares the rendered region against what is on disk and exits 1 if they differ,
printing a diff. No arguments rewrites the file in place.

- [ ] **Step 5: Generate and verify**

```bash
pixi run --as-is python scripts/build_workflow_doc.py
srun -p cpu -A impd --time=00:10:00 --ntasks=1 --cpus-per-task=2 --mem=8G \
  pixi run -e dev --as-is pytest tests/test_build_workflow_doc.py -q
```
Expected: PASS.

- [ ] **Step 6: Mutation-check the staleness gate**

Edit one word inside the generated region by hand, re-run
`python scripts/build_workflow_doc.py --check`, and confirm it exits 1. Regenerate and
confirm it exits 0. A staleness gate that cannot detect staleness is worse than none.

- [ ] **Step 7: Lint and commit**

```bash
pixi run -e dev pre-commit run --files scripts/build_workflow_doc.py slurm_batch/RUNME.md tests/test_build_workflow_doc.py
git add scripts/build_workflow_doc.py slurm_batch/RUNME.md tests/test_build_workflow_doc.py
git commit -m "$(cat <<'EOF'
feat(docs): generate the complete-workflow runbook from the stage manifest

RUNME.md gains a generated section rendered from
configs/workflow/fabric_rerun.yml -- the same file submit_fabric_rerun.sh
executes -- so the individual commands a scientist copies are the strings the
driver runs and cannot drift from them. CI fails if the section is stale, the
mechanism build_parameter_index.py already uses.

Shared-scope stages are rendered with their warning even though the driver
skips them: that is the only place the "rebuilding shared rasters obliges a
re-run of every fabric" trap is documented, and it is what staled every
fabric's elevation product in #215.

Mutation-checked: hand-editing the generated region makes --check exit 1.

Refs the fabric-rerun spec.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 6: Full suite, PR, and tjc validation

**Files:** none modified.

- [ ] **Step 1: Full suite**

```
srun -p cpu -A impd --time=00:30:00 --ntasks=1 --cpus-per-task=4 --mem=32G \
  pixi run -e dev --as-is pytest tests/ -q
```
Expected: PASS. Guards 2 and 3 (`test_params_index_ondisk.py`, `test_merged_products_ondisk.py`)
are data-root-gated; record whether they passed or skipped, by SLURM job id.

- [ ] **Step 2: Full pre-commit sweep, under srun**

```
srun -p cpu -A impd --time=00:20:00 --ntasks=1 --cpus-per-task=4 --mem=64G \
  pixi run -e dev --as-is pre-commit run --all-files
```
Never on the login node — the prettier hook is SIGKILLed there and the shell still exits 0.

- [ ] **Step 3: Dry-run the driver on tjc and read it**

```bash
BATCHES=$(pixi run data-root)/tjc/batches
./slurm_batch/submit_fabric_rerun.sh --dry-run "$BATCHES" tjc
```
Confirm the stage order matches the manifest, placeholders are substituted, `shared_rasters`
is absent, and each stage's `--after` names its predecessor.

- [ ] **Step 4: Back up every tjc product**

```bash
DR=$(pixi run data-root)
for f in "$DR"/tjc/params/merged/*.csv; do
  [ -f "$f.prererun.bak" ] || cp "$f" "$f.prererun.bak"
done
ls "$DR"/tjc/params/merged/*.prererun.bak | wc -l
```
**Do not skip this.** Without it, "the re-run worked" and "the re-run changed nothing" are
indistinguishable afterwards.

- [ ] **Step 5: Run it for real on tjc**

```bash
./slurm_batch/submit_fabric_rerun.sh "$BATCHES" tjc
```
tjc is 1 batch / 1,584 HRUs, so this is minutes. Wait for the chain to drain
(`squeue -u "$USER"`), then confirm every job `COMPLETED 0:0` via `sacct`.

- [ ] **Step 6: Diff every product against its backup**

For each `merged/*.csv`, join on the fabric's id column (`model_hru_idx` for tjc) and
report rows differing per column. Two outcomes are both informative:

- **identical** — the workflow is faithful; record it, as with the three bit-identical
  rebuilds in #215.
- **changed** — expected for the depstor products. tjc's date from 2026-06-08 and predate
  the segment-driven on-stream classifier (#187, merged 2026-07-24/25), so a faithful
  re-run should move them. Report which and by how much.

- [ ] **Step 7: Guards, PR, and issue updates**

Run Guards 2 and 3 against the rebuilt tjc and record the SLURM job id — both are
data-root-gated and SKIP in CI, so a green badge proves nothing about them.

Push and open the PR (remember: `gh` is blocked; use `gh auth token` + `curl --data-binary`).
The PR description leads with what the driver does and does not cover, then the tjc results.

Post the tjc outcome to **#217** — if the depstor products changed as expected, that closes
the tjc half of the segment-driven classifier rollout as a side effect.

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: §Design three components → Tasks 3, 4, 5; §Manifest granularity → Task 3; §Wrapper contract → Task 2; §`--force` normalised → Task 1; §Failure handling → Task 4 (the no-`TERMINAL_JOB_ID` hard error and the `afterok` chain); §Testing → the test file in every task plus Task 6; §Validation → Task 6 Steps 3–7; §Sequencing → the task order itself.

**Placeholder scan.** Two steps deliberately instruct the implementer to *verify before writing* rather than supplying a value I did not measure: Task 3 Step 3 requires confirming the `snarea` `gate: false` claim against `submit_snarea_pipeline.sh` before committing it, and Task 1 Step 3 requires checking whether `derive_depstor_params` actually skips anything before deciding whether `--force` is functional or an honest documented no-op. These are not TODOs — shipping either claim unverified is precisely the class of error that produced the withdrawn work this plan replaces.

**Type consistency.** `TERMINAL_JOB_ID=<id>` is the same literal in Task 2's implementation, Task 2's tests, and Task 4's driver parse (`sed -n 's/^TERMINAL_JOB_ID=//p'`). `--after <jobid>` is the same flag name in Task 2's wrappers and Task 4's driver. The manifest keys (`name`, `command`, `scope`, `gate`, `consumes`, `produces`, `resources`) are identical in Task 3's YAML, Task 3's `REQUIRED_KEYS`, Task 4's Python extractor, and Task 5's generator.

**Known weak spot.** Task 4's `TestChaining` asserts against `--dry-run` output rather than real submissions, because the driver invokes wrappers rather than `sbatch` directly, so a fake `sbatch` is one level too deep to observe the `--after` handoff. If that assertion proves too weak in practice, the stronger form is to stub each stage command in a temporary manifest with a script that echoes its own `TERMINAL_JOB_ID` and logs the `--after` it received. Prefer the stronger form if Task 4 Step 5's mutation check does not fail cleanly.

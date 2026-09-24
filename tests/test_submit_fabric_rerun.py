"""The driver walks the manifest and chains each stage on the previous one's terminal job.

Two layers of test, deliberately:

  * against the REAL manifest, with ``--dry-run`` -- checks ordering, scope filtering,
    placeholder substitution and ``--from``;
  * against a SYNTHETIC manifest whose stages are stub scripts that log the ``--after``
    they were handed and echo a distinct ``TERMINAL_JOB_ID`` -- which is the only way to
    observe the real handoff. A fake ``sbatch`` sits one level too deep: the driver
    invokes wrappers, and the wrappers invoke sbatch.

The synthetic layer is the important one. The withdrawn attempt at this problem shipped a
test whose fake returned a constant job id, so it could not tell a correct chain from a
wrong one and stayed green while the chaining line it covered had been deleted. Every
assertion here uses distinct ids and names which specific predecessor was expected.
"""

from __future__ import annotations

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


def _stages():
    return yaml.safe_load(MANIFEST.read_text())["stages"]


def _fabric_stages():
    return [s for s in _stages() if s["scope"] == "fabric"]


def _tree(tmp_path):
    """A batches dir and a base_config, enough for the driver's own argument handling."""
    batches = tmp_path / "dr" / "tjc" / "batches"
    batches.mkdir(parents=True, exist_ok=True)
    (batches / "manifest.yml").write_text("n_batches: 1\n")
    base_config = tmp_path / "base_config.yml"
    base_config.write_text(f"data_root: {tmp_path / 'dr'}\n")
    return batches, base_config


def _run(args, tmp_path, manifest=None, path_prefix=None, env_extra=None):
    env = dict(os.environ)
    env.pop("SUBMIT_JOBS_MAX_CONCURRENT", None)
    env.pop("ZONAL_PARAMS", None)
    if manifest is not None:
        env["FABRIC_RERUN_MANIFEST"] = str(manifest)
    if path_prefix is not None:
        env["PATH"] = f"{path_prefix}{os.pathsep}{env['PATH']}"
    env.update(env_extra or {})
    return subprocess.run(
        ["bash", str(DRIVER), *args],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        timeout=180,
    )


# --------------------------------------------------------------------------------------
# Layer 1: the real manifest, dry-run
# --------------------------------------------------------------------------------------


class TestDryRun:
    def test_lists_every_fabric_stage_in_manifest_order(self, tmp_path):
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path)
        assert r.returncode == 0, r.stderr
        names = [s["name"] for s in _fabric_stages()]
        positions = [r.stdout.index(n) for n in names]
        assert positions == sorted(positions), f"stages out of manifest order:\n{r.stdout}"

    def test_skips_shared_scope_stages(self, tmp_path):
        """A fabric re-run must not rebuild shared/: it is 12h/96G and every fabric reads
        it, so rebuilding it here would stale the other fabrics as a side effect. It is
        rendered in the runbook, not run by the driver."""
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path)
        shared = [s["name"] for s in _stages() if s["scope"] == "shared"]
        assert shared, "manifest declares no shared stage to skip"
        for name in shared:
            assert name not in r.stdout, f"driver planned the shared stage {name}"

    def test_substitutes_every_placeholder(self, tmp_path):
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path)
        assert "{fabric}" not in r.stdout
        assert "{batches}" not in r.stdout
        assert "{base_config}" not in r.stdout
        assert "tjc" in r.stdout and str(batches) in r.stdout

    def test_submits_nothing(self, tmp_path):
        """--dry-run must be safe on a login node with a REAL sbatch on PATH.

        Asserted by putting a fake sbatch on PATH that records any invocation, then
        requiring the record to be empty -- not by reading the driver's own output, which
        would only restate what the driver claims about itself.
        """
        batches, cfg = _tree(tmp_path)
        bindir = tmp_path / "bin"
        bindir.mkdir()
        marker = tmp_path / "sbatch-was-called"
        fake = bindir / "sbatch"
        fake.write_text(f'#!/usr/bin/env bash\ntouch "{marker}"\necho "Submitted batch job 1"\n')
        fake.chmod(0o755)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path, path_prefix=bindir)
        assert r.returncode == 0, r.stderr
        assert not marker.exists(), "--dry-run invoked sbatch"

    def test_force_is_shown_only_on_stages_that_accept_it(self, tmp_path):
        """--force must reach depstor_rasters and nothing else.

        The submit wrappers reject unknown leading flags by design, so passing --force to
        one would abort the chain; and a stage that silently dropped it would leave an
        operator believing a rebuild was forced when it was not.
        """
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", "--force", str(batches), "tjc", str(cfg)], tmp_path)
        assert r.returncode == 0, r.stderr
        planned = _planned_commands(r.stdout)
        by_name = {s["name"]: s for s in _fabric_stages()}
        for name, cmd in planned.items():
            expected = by_name[name]["accepts_force"]
            assert ("--force" in cmd) == expected, (
                f"{name}: accepts_force={expected} but planned command was:\n  {cmd}"
            )

    def test_force_lands_after_the_batch_script_so_it_reaches_the_orchestrator(self, tmp_path):
        """On a `kind: sbatch` stage --force must come AFTER the .batch path.

        build_depstor_rasters.batch forwards "$@" to the orchestrator, so the flag reaches
        python only from that position. Before the path, sbatch itself would reject it.
        Membership in the command string does not distinguish the two.
        """
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", "--force", str(batches), "tjc", str(cfg)], tmp_path)
        assert r.returncode == 0, r.stderr
        for name, cmd in _planned_commands(r.stdout).items():
            if "--force" not in cmd:
                continue
            batch = [t for t in cmd.split() if t.endswith(".batch")]
            if not batch:
                continue
            assert cmd.index("--force") > cmd.index(batch[0]), (
                f"{name}: --force precedes the batch script, so sbatch would eat it:\n  {cmd}"
            )

    def test_without_force_no_stage_gets_it(self, tmp_path):
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path)
        assert all("--force" not in cmd for cmd in _planned_commands(r.stdout).values())


def _planned_commands(stdout):
    """Map stage name -> the command line the driver printed for it.

    The driver's dry-run prints a `# <stage>` header followed by the command.
    """
    out, current = {}, None
    for line in stdout.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            current = stripped[2:].strip()
        elif current and stripped:
            out[current] = stripped
            current = None
    return out


class TestFrom:
    def test_starts_at_the_named_stage(self, tmp_path):
        batches, cfg = _tree(tmp_path)
        names = [s["name"] for s in _fabric_stages()]
        target = names[-1]
        r = _run(["--dry-run", "--from", target, str(batches), "tjc", str(cfg)], tmp_path)
        assert r.returncode == 0, r.stderr
        planned = _planned_commands(r.stdout)
        assert target in planned
        for skipped in names[:-1]:
            assert skipped not in planned, f"--from {target} still planned {skipped}"

    def test_unknown_stage_is_rejected(self, tmp_path):
        """A typo'd stage name must not silently run everything, or nothing."""
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", "--from", "nosuchstage", str(batches), "tjc", str(cfg)], tmp_path)
        assert r.returncode != 0
        assert "nosuchstage" in r.stderr

    def test_rejecting_an_unknown_stage_names_the_valid_ones(self, tmp_path):
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", "--from", "typo", str(batches), "tjc", str(cfg)], tmp_path)
        for name in [s["name"] for s in _fabric_stages()]:
            assert name in r.stderr, f"error message does not list the valid stage {name}"

    def test_from_a_shared_stage_is_rejected(self, tmp_path):
        """The driver never runs shared stages, so resuming at one cannot mean anything."""
        batches, cfg = _tree(tmp_path)
        shared = [s["name"] for s in _stages() if s["scope"] == "shared"][0]
        r = _run(["--dry-run", "--from", shared, str(batches), "tjc", str(cfg)], tmp_path)
        assert r.returncode != 0, f"--from {shared} was accepted"


class TestEnvironment:
    def test_environment_reaches_the_stage_commands(self, tmp_path):
        """Per-fabric knobs are env vars, and the driver must not swallow them.

        ZONAL_PARAMS is the concrete case and it is not cosmetic. submit_zonal_params.sh
        runs all 10 params by default; on a data root where a source is unstaged (neither
        lulc_nlcd nor lulc_foresce is staged here) that param's array fails, its merge
        fails, and because depstor_params waits on EVERY merge the whole remaining chain
        never runs. Observed for real on the first tjc run. The remedy is to export the
        subset -- which only works if the environment survives the driver.
        """
        batches, cfg = _tree(tmp_path)
        stubdir = tmp_path / "stubs"
        stubdir.mkdir(exist_ok=True)
        log = tmp_path / "env.log"
        stub = stubdir / "echoenv.sh"
        stub.write_text(
            "#!/usr/bin/env bash\n"
            f'echo "ZONAL_PARAMS=${{ZONAL_PARAMS:-unset}}" >> "{log}"\n'
            'echo "TERMINAL_JOB_ID=1"\n'
        )
        stub.chmod(0o755)
        manifest = tmp_path / "envtest.yml"
        manifest.write_text(
            yaml.safe_dump(
                {
                    "stages": [
                        {
                            "name": "only",
                            "command": f"{stub} {{fabric}}",
                            "kind": "wrapper",
                            "scope": "fabric",
                            "accepts_force": False,
                            "consumes": ["x"],
                            "produces": "y",
                            "resources": "z",
                        }
                    ]
                }
            )
        )
        r = _run(
            [str(batches), "tjc", str(cfg)],
            tmp_path,
            manifest=manifest,
            env_extra={"ZONAL_PARAMS": "elevation slope"},
        )
        assert r.returncode == 0, r.stderr
        assert log.read_text().strip() == "ZONAL_PARAMS=elevation slope"


    def test_does_not_depend_on_a_bare_python3_having_pyyaml(self, tmp_path):
        """The driver must parse its manifest with the pixi env's python, not `python3`.

        This is a regression test for a defect that PASSED the whole suite: pytest itself
        runs inside the pixi env, so a bare `python3` in the driver resolves to an
        interpreter that has PyYAML -- and then fails with ModuleNotFoundError for the
        scientist running it by hand on the login node, where the system python does not.
        Caught only by running the driver for real.

        Asserted behaviourally by shadowing `python3` with a stub that always fails: if
        the driver still works, it is not using it.
        """
        batches, cfg = _tree(tmp_path)
        bindir = tmp_path / "nopython"
        bindir.mkdir()
        for name in ("python3", "python"):
            stub = bindir / name
            stub.write_text(
                '#!/usr/bin/env bash\necho "no PyYAML here" >&2\nexit 1\n'
            )
            stub.chmod(0o755)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path, path_prefix=bindir)
        assert r.returncode == 0, (
            "driver used a bare python from PATH to parse the manifest:\n"
            f"{r.stdout}\n{r.stderr}"
        )
        assert "depstor_rasters" in r.stdout


class TestUsage:
    def test_missing_fabric_is_rejected(self, tmp_path):
        batches, _ = _tree(tmp_path)
        r = _run([str(batches)], tmp_path)
        assert r.returncode != 0
        assert "Usage" in r.stdout + r.stderr

    def test_unknown_flag_is_rejected(self, tmp_path):
        batches, cfg = _tree(tmp_path)
        r = _run(["--bogus", str(batches), "tjc", str(cfg)], tmp_path)
        assert r.returncode != 0
        assert "--bogus" in r.stderr

    def test_a_flag_after_the_positionals_is_rejected(self, tmp_path):
        """A trailing flag must not be absorbed as ``base_config``.

        The option loop stops at the first positional, so anything after <batches>
        <fabric> lands in $3. Unguarded, that turns the one command whose entire purpose
        is NOT to submit into a full submission: no DRY RUN line, no error, and a
        completely normal-looking log. Trailing is where the flag is natural to type.
        """
        batches, _cfg = _tree(tmp_path)
        bindir = tmp_path / "bin"
        bindir.mkdir()
        marker = tmp_path / "sbatch-was-called"
        fake = bindir / "sbatch"
        fake.write_text(f'#!/usr/bin/env bash\ntouch "{marker}"\necho "Submitted batch job 1"\n')
        fake.chmod(0o755)
        r = _run([str(batches), "tjc", "--dry-run"], tmp_path, path_prefix=bindir)
        assert r.returncode != 0, f"a trailing --dry-run was accepted:\n{r.stdout}"
        assert not marker.exists(), "a trailing --dry-run submitted the chain for real"

    def test_a_base_config_that_does_not_exist_is_rejected(self, tmp_path):
        """base_config was the one positional whose existence went unchecked, though the
        manifest and the batches dir are both checked two lines later. A typo resolves
        every stage against a different data root than the operator passed."""
        batches, _cfg = _tree(tmp_path)
        missing = tmp_path / "nope.yml"
        r = _run(["--dry-run", str(batches), "tjc", str(missing)], tmp_path)
        assert r.returncode != 0, "a nonexistent base_config was accepted"
        assert "nope.yml" in r.stdout + r.stderr

    def test_a_batches_dir_without_a_manifest_is_rejected(self, tmp_path):
        """The driver's second existence guard, previously untested unlike the --from and
        unknown-flag paths. A re-run starts from the EXISTING batches; an empty or wrong
        directory means the fabric was never prepared, and every stage would fail one by
        one instead of the driver saying so once, up front."""
        empty = tmp_path / "not-batches"
        empty.mkdir()
        _batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", str(empty), "tjc", str(cfg)], tmp_path)
        assert r.returncode != 0
        assert "manifest.yml" in r.stderr

    def test_an_extra_positional_is_rejected(self, tmp_path):
        """A 4th positional is silently dropped, so a mistyped invocation runs with
        arguments the operator did not intend and cannot see were ignored."""
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg), "EXTRA"], tmp_path)
        assert r.returncode != 0, "a 4th positional was silently dropped"


class TestManifestValidation:
    """The driver honours FABRIC_RERUN_MANIFEST, so the checked-in manifest's own schema
    tests do not cover the manifest it actually reads at runtime."""

    @staticmethod
    def _stage(**over):
        s = {
            "name": "a",
            "command": "/bin/true {fabric}",
            "kind": "wrapper",
            "scope": "fabric",
            "accepts_force": False,
            "consumes": ["x"],
            "produces": "y",
            "resources": "z",
        }
        s.update(over)
        return s

    def _manifest(self, tmp_path, stages):
        m = tmp_path / "custom.yml"
        m.write_text(yaml.safe_dump({"stages": stages}))
        return m

    def test_a_manifest_with_no_fabric_stage_is_an_error(self, tmp_path):
        """Having nothing to run must not look like a completed run.

        The scope filter yields an empty list, the read loop skips the single empty line,
        and the driver prints `Chain:` and the monitor hint and exits 0 -- an operator who
        just ran the "complete re-run" concludes their fabric was rebuilt.
        """
        batches, cfg = _tree(tmp_path)
        m = self._manifest(tmp_path, [self._stage(scope="shared")])
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path, manifest=m)
        assert r.returncode != 0, f"a manifest with no fabric stage exited 0:\n{r.stdout}"

    def test_an_unrecognised_scope_is_an_error(self, tmp_path):
        """A mistyped scope must not silently drop just that stage.

        `Fabric` is not `fabric`, so that stage vanishes from the plan while every other
        stage runs and reports success -- the canonical "skipped a stage entirely, and
        everything still reported COMPLETED".
        """
        batches, cfg = _tree(tmp_path)
        m = self._manifest(
            tmp_path, [self._stage(name="a", scope="Fabric"), self._stage(name="b")]
        )
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path, manifest=m)
        assert r.returncode != 0, f"scope 'Fabric' was silently skipped:\n{r.stdout}"


# --------------------------------------------------------------------------------------
# Layer 2: a synthetic manifest of stub stages -- observes the real --after handoff
# --------------------------------------------------------------------------------------


def _synthetic(tmp_path, n=3, terminal_ids=None, kinds=None):
    """Build a manifest of ``n`` stub stages and return (manifest_path, log_path).

    Each stub logs ``<name> AFTER=<what it received>`` and echoes a distinct
    ``TERMINAL_JOB_ID``, so a test can assert stage N+1 got stage N's id SPECIFICALLY.
    """
    stubdir = tmp_path / "stubs"
    stubdir.mkdir(exist_ok=True)
    log = tmp_path / "stage.log"
    terminal_ids = terminal_ids or [f"90{i}" for i in range(n)]
    kinds = kinds or ["wrapper"] * n

    stages = []
    for i in range(n):
        name = f"stage{i}"
        stub = stubdir / f"{name}.sh"
        if kinds[i] == "wrapper":
            # Mimics a submit wrapper: leading --after, final TERMINAL_JOB_ID.
            stub.write_text(
                "#!/usr/bin/env bash\nset -euo pipefail\nAFTER=none\n"
                'if [ "${1:-}" = "--after" ]; then AFTER="$2"; shift 2; fi\n'
                f'echo "{name} AFTER=$AFTER ARGS=$*" >> "{log}"\n'
                f'echo "TERMINAL_JOB_ID={terminal_ids[i]}"\n'
            )
        else:
            # Mimics `sbatch`: the driver must supply --dependency itself and parse
            # "Submitted batch job N" out of the output.
            stub.write_text(
                "#!/usr/bin/env bash\nset -euo pipefail\nAFTER=none\n"
                'for a in "$@"; do case "$a" in --dependency=*) AFTER="${a#--dependency=}";; esac; done\n'
                f'echo "{name} AFTER=$AFTER ARGS=$*" >> "{log}"\n'
                f'echo "Submitted batch job {terminal_ids[i]}"\n'
            )
        stub.chmod(0o755)
        stages.append(
            {
                "name": name,
                "command": f"{stub} {{fabric}}",
                "kind": kinds[i],
                "scope": "fabric",
                "accepts_force": False,
                "consumes": ["x"],
                "produces": "y",
                "resources": "z",
            }
        )
    manifest = tmp_path / "synthetic.yml"
    manifest.write_text(yaml.safe_dump({"stages": stages}))
    return manifest, log


def _log_entries(log):
    """[(stage_name, after_value)] in submission order."""
    entries = []
    for line in log.read_text().splitlines():
        name, rest = line.split(" AFTER=", 1)
        entries.append((name, rest.split(" ARGS=")[0]))
    return entries


class TestDriverRobustness:
    """Ways the driver could mis-handle a stage without saying so."""

    def test_a_stage_that_reads_stdin_does_not_truncate_the_chain(self, tmp_path):
        """The stage loop is fed by a here-string, which `eval` would otherwise inherit.

        A stage command that reads stdin then consumes the REMAINING stage list, so the
        chain stops after it and the driver exits 0 having silently skipped every later
        stage -- the exact failure this driver exists to prevent. None of today's four
        wrappers drains stdin, so this is latent, but the blast radius is the whole
        design goal and the fix is one redirection.
        """
        manifest, log = _synthetic(tmp_path, n=3)
        stages = yaml.safe_load(manifest.read_text())["stages"]
        greedy = tmp_path / "stubs" / "greedy.sh"
        greedy.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\ncat > /dev/null\n"
            f'echo "stage0 AFTER=none ARGS=$*" >> "{log}"\necho "TERMINAL_JOB_ID=900"\n'
        )
        greedy.chmod(0o755)
        stages[0]["command"] = f"{greedy} {{fabric}}"
        manifest.write_text(yaml.safe_dump({"stages": stages}))

        batches, cfg = _tree(tmp_path)
        r = _run([str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode == 0, r.stderr
        ran = [name for name, _ in _log_entries(log)]
        assert ran == ["stage0", "stage1", "stage2"], (
            f"a stdin-reading stage swallowed the rest of the chain; ran {ran}"
        )

    def test_a_single_token_command_is_not_duplicated(self, tmp_path):
        """`HEAD=${CMD%% *}` and `REST=${CMD#* }` both expand to the whole string when the
        command has no space, so the dependency insertion appends the command to itself:
        `cmd --after 1 cmd`. Silent corruption rather than an error, and one manifest edit
        away -- the manifest is explicitly designed to be extended."""
        manifest, _log = _synthetic(tmp_path, n=2)
        stages = yaml.safe_load(manifest.read_text())["stages"]
        solo = tmp_path / "stubs" / "solo.sh"
        solo.write_text('#!/usr/bin/env bash\necho "TERMINAL_JOB_ID=902"\n')
        solo.chmod(0o755)
        stages[1]["command"] = str(solo)  # no arguments at all
        manifest.write_text(yaml.safe_dump({"stages": stages}))

        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode == 0, r.stderr
        planned = _planned_commands(r.stdout)["stage1"]
        assert planned.count(str(solo)) == 1, f"command duplicated into its own argv:\n  {planned}"

    def test_a_non_numeric_terminal_job_id_is_rejected(self, tmp_path):
        """A job id is checked for emptiness but never for being a job id.

        submit_snarea_pipeline.sh prints the literal `DRYRUN` as a stand-in when DRYRUN is
        in the environment -- which the driver passes through -- so a stray export makes a
        stage submit nothing, report success, and hand `afterok:DRYRUN` to the next stage.
        That fails at the NEXT stage, naming the wrong one; and a terminal stage would end
        the chain in outright silent success.
        """
        manifest, _log = _synthetic(tmp_path, n=2, terminal_ids=["901", "DRYRUN"])
        batches, cfg = _tree(tmp_path)
        r = _run([str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode != 0, f"a non-numeric job id was chained on:\n{r.stdout}"
        assert "DRYRUN" in r.stderr

    def test_an_unresolved_placeholder_is_rejected(self, tmp_path):
        """The driver substitutes exactly three placeholders and validated none.

        Anything else survives into the submitted command as a literal brace --
        `VPU={vpu}` reaches sbatch verbatim, exit 0. derive_zonal_params._resolve_nested
        raises on this same condition; the manifest reader should too. The manifest's own
        schema test covers the checked-in file, not the FABRIC_RERUN_MANIFEST override.
        """
        manifest, _log = _synthetic(tmp_path, n=1)
        stages = yaml.safe_load(manifest.read_text())["stages"]
        stages[0]["command"] += " VPU={vpu}"
        manifest.write_text(yaml.safe_dump({"stages": stages}))
        batches, cfg = _tree(tmp_path)
        r = _run(["--dry-run", str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode != 0, "an unresolved {vpu} was submitted verbatim"
        assert "vpu" in r.stderr


class TestChaining:
    def test_each_stage_chains_on_its_immediate_predecessor(self, tmp_path):
        """THE assertion the whole driver rests on.

        Distinct terminal ids per stub, so this proves stage N+1 received stage N's id
        specifically -- not merely that it received *some* dependency.
        """
        batches, cfg = _tree(tmp_path)
        manifest, log = _synthetic(tmp_path, n=3, terminal_ids=["901", "902", "903"])
        r = _run([str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode == 0, r.stderr + r.stdout
        assert _log_entries(log) == [
            ("stage0", "none"),
            ("stage1", "901"),
            ("stage2", "902"),
        ]

    def test_sbatch_kind_stages_are_chained_too(self, tmp_path):
        """A plain-sbatch stage gets --dependency=afterok:<prev>, not --after.

        Both kinds appear in the real manifest, and getting the sbatch branch wrong is
        invisible: the stage still submits and still succeeds, just without waiting.
        """
        batches, cfg = _tree(tmp_path)
        manifest, log = _synthetic(
            tmp_path, n=3, terminal_ids=["911", "912", "913"],
            kinds=["wrapper", "sbatch", "wrapper"],
        )
        r = _run([str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode == 0, r.stderr + r.stdout
        assert _log_entries(log) == [
            ("stage0", "none"),
            ("stage1", "afterok:911"),
            ("stage2", "912"),
        ]

    def test_colon_joined_terminal_ids_are_passed_through_whole(self, tmp_path):
        """submit_zonal_params.sh reports every merge job, colon-joined.

        Truncating that to the first id would let the next stage start while sibling
        params were still writing -- so the driver must forward it verbatim.
        """
        batches, cfg = _tree(tmp_path)
        manifest, log = _synthetic(tmp_path, n=2, terminal_ids=["921:922:923", "930"])
        r = _run([str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode == 0, r.stderr + r.stdout
        assert _log_entries(log)[1] == ("stage1", "921:922:923")

    def test_a_stage_printing_no_terminal_job_id_is_a_hard_error(self, tmp_path):
        """Continuing unchained would run every later stage against incomplete inputs --
        the exact failure this driver exists to prevent -- and every job would still
        report COMPLETED."""
        batches, cfg = _tree(tmp_path)
        manifest, log = _synthetic(tmp_path, n=3)
        stages = yaml.safe_load(manifest.read_text())
        silent = tmp_path / "stubs" / "silent.sh"
        silent.write_text('#!/usr/bin/env bash\necho "no id here"\n')
        silent.chmod(0o755)
        stages["stages"][0]["command"] = str(silent)
        manifest.write_text(yaml.safe_dump(stages))

        r = _run([str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode != 0, "driver continued past a stage with no TERMINAL_JOB_ID"
        assert "TERMINAL_JOB_ID" in r.stderr
        # And it must stop THERE, not carry on with a blank dependency.
        assert not log.exists() or len(_log_entries(log)) == 0

    def test_a_failing_stage_stops_the_chain(self, tmp_path):
        """If a stage's submission command fails, later stages must not be submitted."""
        batches, cfg = _tree(tmp_path)
        manifest, log = _synthetic(tmp_path, n=3)
        stages = yaml.safe_load(manifest.read_text())
        boom = tmp_path / "stubs" / "boom.sh"
        boom.write_text('#!/usr/bin/env bash\necho "boom" >&2\nexit 3\n')
        boom.chmod(0o755)
        stages["stages"][1]["command"] = str(boom)
        manifest.write_text(yaml.safe_dump(stages))

        r = _run([str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode != 0
        names = [name for name, _ in _log_entries(log)]
        assert "stage2" not in names, f"driver continued past a failed stage: {names}"

    def test_from_resumes_without_an_inbound_dependency(self, tmp_path):
        """The resumed stage has no predecessor in THIS run, so it must start free.

        Inventing a dependency on a job that is not running would hold the whole resumed
        chain forever.
        """
        batches, cfg = _tree(tmp_path)
        manifest, log = _synthetic(tmp_path, n=3, terminal_ids=["941", "942", "943"])
        r = _run(
            ["--from", "stage1", str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest
        )
        assert r.returncode == 0, r.stderr + r.stdout
        assert _log_entries(log) == [("stage1", "none"), ("stage2", "942")]

    def test_the_chain_summary_reports_every_stage_and_its_job(self, tmp_path):
        """An operator needs the stage -> job map to scancel the tail deliberately."""
        batches, cfg = _tree(tmp_path)
        manifest, _ = _synthetic(tmp_path, n=3, terminal_ids=["951", "952", "953"])
        r = _run([str(batches), "tjc", str(cfg)], tmp_path, manifest=manifest)
        assert r.returncode == 0, r.stderr
        for name, job in [("stage0", "951"), ("stage1", "952"), ("stage2", "953")]:
            assert f"{name}={job}" in r.stdout, f"chain summary missing {name}={job}"

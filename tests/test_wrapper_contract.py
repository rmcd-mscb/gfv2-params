"""The four workflow wrappers share one contract so a driver can chain them.

  * they accept leading ``--after <jobid>`` and apply it to every submission that would
    otherwise have no dependency;
  * they print a final machine-readable ``TERMINAL_JOB_ID=<id>``;
  * they reject an unknown leading flag instead of swallowing it as a positional.

These RUN the wrappers against a fake ``sbatch`` on PATH that returns DISTINCT,
INCREMENTING job ids and logs its argv. The distinct ids are the point: a fake returning a
constant cannot tell a correctly-chained dependency from a wrongly-chained one, and an
earlier test in this repo passed for exactly that reason while the chaining line it
covered had been deleted.

The central assertion is a universal invariant rather than a per-wrapper line check: with
``--after`` supplied, EVERY submission must depend on either the external job or on a job
this same run submitted earlier. That catches a wrapper which chains only its first
submission -- the failure that would let later jobs start against incomplete inputs.

No SLURM and no data root: the fake sbatch and a synthetic fabric tree stand in for both,
so this runs in CI.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SLURM = REPO / "slurm_batch"

WRAPPERS = [
    "submit_zonal_params.sh",
    "submit_depstor_params.sh",
    "submit_snarea_pipeline.sh",
    "submit_dprst_depth.sh",
]

pytestmark = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")


def _fake_sbatch(tmp_path):
    """A fake ``sbatch``: distinct incrementing ids, plus an argv log so chaining is assertable.

    Honours ``--parsable`` (bare id) vs the default ("Submitted batch job N"), because
    submit_snarea_pipeline.sh reads the former and the other three ``awk '{print $NF}'``
    the latter. A fake that got this wrong would hand the snarea wrapper a whole sentence
    as a job id and the chain assertions would pass on nonsense.
    """
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
        'for a in "$@"; do\n'
        '  if [ "$a" = "--parsable" ]; then echo "$n"; exit 0; fi\n'
        "done\n"
        'echo "Submitted batch job $n"\n'
    )
    fake.chmod(0o755)
    return bindir, log


def _fabric_tree(tmp_path, fabric="gfv2", n_batches=2):
    """Minimal ``{data_root}/{fabric}`` tree plus a base_config the wrappers can grep."""
    data_root = tmp_path / "dr"
    batches = data_root / fabric / "batches"
    batches.mkdir(parents=True, exist_ok=True)
    (batches / "manifest.yml").write_text(f"n_batches: {n_batches}\n")
    base_config = tmp_path / "base_config.yml"
    base_config.write_text(f"data_root: {data_root}\ndefault_fabric: {fabric}\n")
    return data_root, batches, base_config


class _Run:
    """One wrapper invocation: exit status, stdout/stderr, and the parsed sbatch log."""

    _LOG = re.compile(r"^(\d+) ARGV: (.*)$")

    def __init__(self, proc, log_lines):
        self.proc = proc
        self.submissions = []
        for line in log_lines:
            m = self._LOG.match(line)
            if m:
                self.submissions.append((m.group(1), m.group(2)))

    @property
    def stdout(self):
        return self.proc.stdout

    @property
    def stderr(self):
        return self.proc.stderr

    @property
    def returncode(self):
        return self.proc.returncode

    def terminal_job_id(self):
        lines = [
            ln for ln in self.stdout.splitlines() if ln.startswith("TERMINAL_JOB_ID=")
        ]
        assert len(lines) == 1, (
            f"expected exactly one TERMINAL_JOB_ID line, got {lines}\n{self.stdout}"
        )
        return lines[0].split("=", 1)[1].strip()

    def terminal_ids(self):
        """``TERMINAL_JOB_ID`` as a set. It may be colon-joined -- see ``leaf_ids``."""
        return set(self.terminal_job_id().split(":"))

    def dependencies(self, argv):
        """Every job id named in ``argv``'s --dependency, if it has one."""
        m = re.search(r"--dependency=(\S+)", argv)
        return set(re.findall(r"\d+", m.group(1))) if m else set()

    def leaf_ids(self, external=()):
        """Jobs nothing else in this run waits on -- the run's true completion frontier.

        A wrapper that fans out (submit_zonal_params submits an independent array+merge
        per param) has SEVERAL leaves, and they finish in no particular order. Naming only
        the last-submitted one would let a downstream stage start while sibling params
        were still writing, so this is the set TERMINAL_JOB_ID has to cover.
        """
        submitted = {job_id for job_id, _ in self.submissions}
        waited_on: set[str] = set()
        for _, argv in self.submissions:
            waited_on |= self.dependencies(argv)
        return submitted - waited_on - set(external)


def _run(script, args, tmp_path, env_extra=None):
    bindir, log = _fake_sbatch(tmp_path)
    env = dict(os.environ)
    env["PATH"] = f"{bindir}{os.pathsep}{env['PATH']}"
    # Do not inherit the developer's shell settings into an assertion about defaults.
    env.pop("SUBMIT_JOBS_MAX_CONCURRENT", None)
    env.pop("ZONAL_PARAMS", None)
    env.pop("FORCE", None)
    env.pop("DRYRUN", None)
    env.update(env_extra or {})
    proc = subprocess.run(
        ["bash", str(SLURM / script), *args],
        cwd=REPO,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return _Run(proc, log.read_text().splitlines() if log.exists() else [])


def _invocation(script, tmp_path, lead=()):
    """The argv and env that make each wrapper do a small but representative run.

    The wrappers do not share a signature: three take ``<batches_dir> [fabric]
    [base_config]`` and submit_snarea_pipeline.sh takes ``<fabric> [base_config]``.
    """
    _, batches, base_config = _fabric_tree(tmp_path)
    if script == "submit_snarea_pipeline.sh":
        return [*lead, "gfv2", str(base_config)], {}
    if script == "submit_dprst_depth.sh":
        # 4th positional is n_tile_batches; keep the fake array small.
        return [*lead, str(batches), "gfv2", str(base_config), "2"], {}
    if script == "submit_zonal_params.sh":
        # slope before ssflux: ssflux reads the merged slope CSV, and the wrapper hard
        # errors if its upstream has not been submitted yet. Including ssflux also
        # exercises the build_weights job, which is an independent submission.
        return [*lead, str(batches), "gfv2", str(base_config)], {
            "ZONAL_PARAMS": "slope ssflux"
        }
    return [*lead, str(batches), "gfv2", str(base_config)], {}


class TestTerminalJobId:
    @pytest.mark.parametrize("script", WRAPPERS)
    def test_prints_exactly_one_terminal_job_id(self, script, tmp_path):
        args, env = _invocation(script, tmp_path)
        run = _run(script, args, tmp_path, env)
        assert run.returncode == 0, run.stderr
        run.terminal_job_id()  # asserts exactly one line

    @pytest.mark.parametrize("script", WRAPPERS)
    def test_terminal_job_covers_every_leaf_of_the_run(self, script, tmp_path):
        """It must name the run's whole completion frontier, not just its last submission.

        A driver chains the next stage on this id, so any leaf left out is a job the next
        stage will not wait for. submit_zonal_params.sh is the case that makes this bite:
        it submits an independent array+merge per param, so its leaves are ALL the merge
        jobs, which finish in no particular order. Naming only the last-submitted merge
        would start the gap-fill while other params were still writing their CSVs --
        silently, with every job reporting COMPLETED.
        """
        args, env = _invocation(script, tmp_path)
        run = _run(script, args, tmp_path, env)
        assert run.returncode == 0, run.stderr
        assert run.submissions, "wrapper submitted nothing"
        reported, leaves = run.terminal_ids(), run.leaf_ids()
        assert leaves, "no leaf jobs found -- dependency parsing is wrong"

        # Cover every leaf. Naming a NON-leaf too is harmless redundancy -- SLURM is
        # already waiting on its dependents -- so this is a superset check, not equality.
        # submit_zonal_params.sh reports every param's merge, and slope's merge is not a
        # leaf whenever ssflux is in the run.
        assert leaves <= reported, (
            f"{script}: TERMINAL_JOB_ID omits jobs the next stage must wait for.\n"
            f"  reported: {sorted(reported)}\n"
            f"  leaves:   {sorted(leaves)}\n"
            f"  missing:  {sorted(leaves - reported)}"
        )
        # ...but every id it names must be a job this run actually submitted, or the
        # driver would chain on something that will never run and SLURM would hold the
        # whole rest of the workflow forever.
        submitted = {job_id for job_id, _ in run.submissions}
        assert reported <= submitted, (
            f"{script}: TERMINAL_JOB_ID names jobs that were never submitted: "
            f"{sorted(reported - submitted)}"
        )

    def test_zonal_reports_every_leaf_when_it_fans_out(self, tmp_path):
        """Pins that the leaf check above actually exercises the fan-out case.

        Two INDEPENDENT params (no ssflux, so no slope->ssflux edge) give two unrelated
        merge jobs, both leaves. If this ever collapsed to a single reported id,
        ``test_terminal_job_covers_every_leaf_of_the_run`` would still pass while no
        longer testing the thing it exists for.
        """
        _, batches, base_config = _fabric_tree(tmp_path)
        run = _run(
            "submit_zonal_params.sh",
            [str(batches), "gfv2", str(base_config)],
            tmp_path,
            {"ZONAL_PARAMS": "elevation aspect"},
        )
        assert run.returncode == 0, run.stderr
        assert len(run.leaf_ids()) == 2, f"expected 2 independent leaves: {run.submissions}"
        assert run.terminal_ids() == run.leaf_ids()


class TestAfterFlag:
    @pytest.mark.parametrize("script", WRAPPERS)
    def test_every_submission_is_chained_when_after_is_given(self, script, tmp_path):
        """The invariant the driver rests on.

        With ``--after``, no submission may be dependency-free: each must depend on the
        external job or on one this run submitted earlier. A wrapper that chained only
        its first submission would let the rest start immediately against incomplete
        upstream inputs -- and every job would still report COMPLETED.
        """
        args, env = _invocation(script, tmp_path, lead=("--after", "777"))
        run = _run(script, args, tmp_path, env)
        assert run.returncode == 0, run.stderr
        assert run.submissions, "wrapper submitted nothing"

        seen: set[str] = set()
        for job_id, argv in run.submissions:
            deps = run.dependencies(argv)
            assert deps & ({"777"} | seen), (
                f"{script}: submission {job_id} has no inbound dependency.\n"
                f"  argv: {argv}\n"
                f"  expected afterok:777 or one of {sorted(seen)}"
            )
            seen.add(job_id)

    @pytest.mark.parametrize("script", WRAPPERS)
    def test_no_after_means_no_invented_dependency(self, script, tmp_path):
        """Absent ``--after``, the wrapper's first submission must stay free-standing."""
        args, env = _invocation(script, tmp_path)
        run = _run(script, args, tmp_path, env)
        assert run.returncode == 0, run.stderr
        first_argv = run.submissions[0][1]
        assert "777" not in first_argv
        assert not run.dependencies(first_argv), (
            f"{script}: first submission invented a dependency: {first_argv}"
        )

    @pytest.mark.parametrize("script", WRAPPERS)
    def test_after_without_a_value_is_rejected(self, script, tmp_path):
        """``--after`` with nothing after it must not silently mean "no dependency".

        ``--after`` is the WHOLE argv here. Putting it in front of the usual positionals
        would not test anything: it would consume the batches dir as its value.
        """
        run = _run(script, ["--after"], tmp_path, {})
        assert run.returncode != 0, f"{script} accepted a valueless --after"
        assert not run.submissions, f"{script} submitted jobs despite a bad --after"
        # Must fail FOR THIS REASON. Without naming the cause the test passes vacuously:
        # an unparsed `--after` also exits 1, as a nonexistent batches dir.
        assert "--after" in run.stderr, (
            f"{script} rejected the run but not as a --after error:\n{run.stderr}"
        )

    @pytest.mark.parametrize("script", WRAPPERS)
    def test_unknown_leading_flag_is_rejected(self, script, tmp_path):
        """An unrecognised leading flag must be an error, never a positional.

        Without this, a driver passing e.g. ``--force`` to a wrapper that does not take
        it would have the flag silently absorbed as the batches-dir or fabric argument.
        That is the same silent-degradation shape that sank the two withdrawn attempts at
        this problem: a check that cannot apply must fail loudly, not quietly pass.
        """
        args, env = _invocation(script, tmp_path, lead=("--not-a-real-flag",))
        run = _run(script, args, tmp_path, env)
        assert run.returncode != 0, f"{script} accepted an unknown leading flag"
        assert not run.submissions, f"{script} submitted jobs despite an unknown flag"
        # As above: naming the flag is what distinguishes a real rejection from the
        # incidental exit-1 you get when the flag is misread as a path.
        assert "--not-a-real-flag" in run.stderr, (
            f"{script} rejected the run but not as an unknown-flag error:\n{run.stderr}"
        )


class TestFakeSbatch:
    def test_fake_returns_distinct_ids(self, tmp_path):
        """Guards the guard.

        If the fake ever returned a constant, every chaining assertion above would pass
        vacuously -- which is precisely how an earlier version of this test in this repo
        stayed green while the code it covered was broken.
        """
        args, env = _invocation("submit_depstor_params.sh", tmp_path)
        run = _run("submit_depstor_params.sh", args, tmp_path, env)
        ids = [job_id for job_id, _ in run.submissions]
        assert len(ids) > 1, "need >1 submission to prove ids differ"
        assert len(set(ids)) == len(ids), f"fake sbatch reused a job id: {ids}"

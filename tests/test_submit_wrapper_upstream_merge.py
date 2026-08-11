"""Behavioural tests for `submit_zonal_params.sh`'s upstream-merge prereq (Step B).

`tests/test_submit_wrapper_param_lists.py` PARSES this script; nothing has ever RUN it.
These tests do, against a fake `sbatch` on PATH and a synthetic fabric tree, so the
guard's three outcomes are pinned by execution rather than by reading.

Why it matters: the guard originally required the upstream param to be in the SAME
submission, so re-running `ssflux` alone after a completed `slope` rebuild demanded
re-running all 64 slope batches purely to re-satisfy a job-ordering check. The guard's
real job is to stop a param reading a MISSING or STALE merge -- both of which are still
rejected here.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "slurm_batch" / "submit_zonal_params.sh"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None, reason="bash not available"
)


def _fabric(tmp_path, *, merged=True, stale=False, n_batches=2):
    """A synthetic {data_root}/{fabric} tree with slope's per-batch + merged CSVs."""
    root = tmp_path / "dr" / "gfv2"
    batches = root / "batches"
    batches.mkdir(parents=True)
    (batches / "manifest.yml").write_text(f"n_batches: {n_batches}\n")

    per_batch = root / "params" / "slope"
    per_batch.mkdir(parents=True)
    merged_dir = root / "params" / "merged"
    merged_dir.mkdir(parents=True)

    # Order the mtimes explicitly; a same-second write would make `find -newer`
    # non-deterministic and the test flaky.
    for i in range(n_batches):
        f = per_batch / f"base_nhm_slope_gfv2_batch_{i:04d}_param.csv"
        f.write_text("nat_hru_id,mean\n1,5.0\n")
        os.utime(f, (1_700_000_000, 1_700_000_000))

    if merged:
        m = merged_dir / "nhm_slope_params.csv"
        m.write_text("nat_hru_id,mean\n1,5.0\n")
        # stale -> merged OLDER than its inputs; fresh -> newer.
        ts = 1_600_000_000 if stale else 1_800_000_000
        os.utime(m, (ts, ts))
    return root, batches


def _run(batches, params, tmp_path):
    """Run the wrapper with a fake sbatch that prints a plausible job id."""
    bindir = tmp_path / "bin"
    bindir.mkdir(exist_ok=True)
    fake = bindir / "sbatch"
    fake.write_text("#!/usr/bin/env bash\necho 'Submitted batch job 12345'\n")
    fake.chmod(0o755)

    env = dict(os.environ)
    env["PATH"] = f"{bindir}{os.pathsep}{env['PATH']}"
    env["ZONAL_PARAMS"] = params
    return subprocess.run(
        ["bash", str(SCRIPT), str(batches), "gfv2"],
        cwd=REPO, env=env, capture_output=True, text=True,
    )


class TestUpstreamMergePrereq:
    def test_upstream_in_the_same_run_chains_on_its_merge_job(self, tmp_path):
        """The common path, unchanged: slope before ssflux chains afterok.

        Asserting only `"merge afterok" in stdout` would be worthless -- the script
        prints that line for EVERY param unconditionally, so it stays green even if
        Step B is deleted outright. What distinguishes case 1 from case 2 is that the
        on-disk branch is NOT taken.
        """
        _, batches = _fabric(tmp_path)
        r = _run(batches, "slope ssflux", tmp_path)
        assert r.returncode == 0, r.stderr
        assert "merge afterok" in r.stdout
        assert "using merged CSV already on disk" not in r.stdout

    def test_upstream_present_but_ordered_after_is_rejected(self, tmp_path):
        """The third case, and the one that can corrupt a whole product.

        `MERGE_JOB_BY_PARAM` is filled as the loop walks PARAMS, so an empty entry
        means EITHER "not in this run" OR "in this run, not reached yet". Treating
        the second as the first submits ssflux with no dependency while a slope
        rebuild runs concurrently; ssflux reads merged slope at ZONAL time and its
        normalisation is fabric-wide, so the entire product is wrong.

        A current on-disk merge is deliberately present here: the point is that the
        wrapper must refuse ANYWAY, because the file it would use is about to be
        replaced by the slope job in this very submission.
        """
        _, batches = _fabric(tmp_path)
        r = _run(batches, "ssflux slope", tmp_path)
        assert r.returncode != 0
        assert "AFTER" in r.stderr
        assert "Reorder" in r.stderr
        # The false reassurance the regression printed must be gone.
        assert "not in this run" not in r.stdout

    def test_upstream_already_merged_on_disk_is_accepted(self, tmp_path):
        """The case that used to be rejected. ssflux alone, with a current merged
        slope on disk, must proceed -- not demand a 64-batch slope re-run."""
        _, batches = _fabric(tmp_path)
        r = _run(batches, "ssflux", tmp_path)
        assert r.returncode == 0, r.stderr
        assert "using merged CSV already on disk" in r.stdout

    def test_missing_upstream_merge_is_rejected(self, tmp_path):
        """No merge job and nothing on disk -- ssflux would read a file that does
        not exist, so this must still fail loudly."""
        _, batches = _fabric(tmp_path, merged=False)
        r = _run(batches, "ssflux", tmp_path)
        assert r.returncode != 0
        assert "does not exist on disk" in r.stderr

    def test_stale_upstream_merge_is_rejected(self, tmp_path):
        """The subtle one. A merged CSV OLDER than its own per-batch inputs means the
        merge predates the zonal pass that produced them; ssflux would silently read a
        stale slope. Accepting merely because the file exists would have re-created,
        in the wrapper, exactly the last-write-vs-derivation confusion that made
        nhm_slope_params.csv look current for weeks."""
        _, batches = _fabric(tmp_path, stale=True)
        r = _run(batches, "ssflux", tmp_path)
        assert r.returncode != 0
        assert "STALE" in r.stderr
        assert "--mode merge --param slope" in r.stderr

    def test_a_param_with_no_upstream_prereq_is_unaffected(self, tmp_path):
        """elevation declares no NEEDS_MERGE_OF entry; Step B must not touch it."""
        _, batches = _fabric(tmp_path, merged=False)
        r = _run(batches, "elevation", tmp_path)
        assert r.returncode == 0, r.stderr

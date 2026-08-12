"""Every orchestrator's ``--force`` means the same thing: rebuild regardless of what exists.

Before this was pinned, ``--force`` meant three different things: "overwrite outputs" on
``build_shared_rasters`` and ``build_depstor_rasters``, "build_weights only" on
``derive_zonal_params``, and nothing at all on ``derive_depstor_params``. An operator
re-running a stage could not tell whether anything was actually rebuilt -- which is the
ambiguity the whole fabric-rerun effort exists to remove.

These tests drive each script's real ``--help``, so they prove argparse actually accepts
the flag rather than that the source merely mentions it. That costs a subprocess per
script (~4-20s each: the orchestrators import rasterio/GDAL/gdptools at module scope), so
the output is cached and each script is invoked exactly ONCE for the whole module. Do not
replace the cache with a per-test call -- CLAUDE.md warns that concurrent geo-library
imports on a shared filesystem cause metadata storms.

No data root and no SLURM, so this runs in CI.
"""

from __future__ import annotations

import subprocess
import sys
from functools import lru_cache
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent

ORCHESTRATORS = [
    "build_shared_rasters",
    "build_depstor_rasters",
    "derive_zonal_params",
    "derive_depstor_params",
]


@lru_cache(maxsize=None)
def _help_text(name: str) -> str:
    """``<script> --help`` output. Cached: one subprocess per script per test session."""
    script = REPO / "scripts" / f"{name}.py"
    result = subprocess.run(
        [sys.executable, str(script), "--help"],
        capture_output=True,
        text=True,
        cwd=REPO,
        timeout=300,
    )
    assert result.returncode == 0, f"{name} --help failed:\n{result.stderr}"
    return result.stdout


def _option_block(help_text: str, flag: str) -> str | None:
    """The help entry for ``flag`` -- its own line plus any continuation lines.

    argparse wraps long help onto following lines indented past the flag column, so a
    single-line match would miss most of the text we need to assert on.
    """
    lines = help_text.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        # Match the flag as a whole token: `--force` must not match `--force-weights`.
        if not stripped.startswith(flag):
            continue
        if stripped[len(flag) : len(flag) + 1] not in ("", " ", ",", "="):
            continue
        block = [line]
        indent = len(line) - len(line.lstrip())
        for nxt in lines[i + 1 :]:
            if not nxt.strip() or (len(nxt) - len(nxt.lstrip())) <= indent:
                break
            block.append(nxt)
        return "\n".join(block)
    return None


@pytest.mark.parametrize("name", ORCHESTRATORS)
def test_every_orchestrator_accepts_force(name):
    """``--force`` must exist everywhere, so any stage can always be told to rebuild.

    The driver passes one ``--force`` to every stage; a stage that rejected it would abort
    the whole chain.
    """
    assert _option_block(_help_text(name), "--force") is not None, f"{name} has no --force"


def test_zonal_keeps_the_weights_only_behaviour_under_an_honest_name():
    """The old zonal ``--force`` rebuilt only the lithology weight matrix.

    That is a real and separate question, so it keeps a flag -- just not the one whose
    name implies a general rebuild.
    """
    block = _option_block(_help_text("derive_zonal_params"), "--force-weights")
    assert block is not None, "derive_zonal_params lost --force-weights"
    assert "weight" in block.lower()


@pytest.mark.parametrize("name", ORCHESTRATORS)
def test_force_help_does_not_document_a_narrower_action(name):
    """Help text is the operator's only signal about what ``--force`` does.

    It must not describe something narrower than a rebuild -- the old zonal text said
    "build_weights only", which is what made the flag mean four different things.
    """
    block = _option_block(_help_text(name), "--force")
    assert "build_weights only" not in block, (
        f"{name}'s --force still documents a narrower action:\n{block}"
    )


def test_the_weights_batch_passes_the_renamed_flag():
    """``build_zonal_weights.batch`` documents ``FORCE=1`` as "overwrite the weight CSV".

    That is now ``--force-weights``. If this batch still passed plain ``--force``, the
    rename would have turned FORCE=1 into a silent no-op: the weight matrix would quietly
    not be rebuilt, nothing would error, and ssflux -- whose aggregation is fabric-wide --
    would go on consuming the stale matrix. Pinned because the rename is exactly the kind
    of change that leaves a caller behind.
    """
    batch = (REPO / "slurm_batch" / "build_zonal_weights.batch").read_text()
    invocation = [ln for ln in batch.splitlines() if "FORCE_FLAG=" in ln and "${FORCE" in ln]
    assert invocation, "build_zonal_weights.batch no longer derives FORCE_FLAG from FORCE"
    assert "--force-weights" in invocation[0], (
        f"weights batch passes a flag that no longer overwrites the matrix: {invocation[0]}"
    )


@pytest.mark.parametrize("name", ["derive_zonal_params", "derive_depstor_params"])
def test_no_op_force_says_so_out_loud(name):
    """Where ``--force`` cannot do anything, the help must admit it.

    Neither of these orchestrators skips existing outputs -- every ``exists()`` check in
    them is an *input* precondition that raises. A flag that silently does nothing is
    worse than one documented as accepted-for-consistency, because an operator reads a
    silent ``--force`` as proof the product was rebuilt.
    """
    block = _option_block(_help_text(name), "--force").lower()
    assert "no-op" in block or "always rebuilds" in block, (
        f"{name}'s --force is a no-op but does not say so:\n{block}"
    )

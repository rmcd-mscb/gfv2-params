"""Tests for the startup-heartbeat gating in gfv2_params.zonal_runners.

The package __init__ prints a `[startup ...]` line so a SLURM array task that
hangs on geo-library import can be localised. Interactive imports (Jupyter,
plain scripts) should stay silent so a geoscientist doesn't mistake the
heartbeat for an error.

Each test runs the import in a subprocess so the side-effect fires fresh (the
print only happens at first import; re-importing in-process is a no-op).
This also keeps the parent test-collector process geo-lib-free.
"""

from __future__ import annotations

import os
import subprocess
import sys

_IMPORT_SNIPPET = "import gfv2_params.zonal_runners  # noqa: F401"


def _run_import(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    """Run a fresh subprocess that imports zonal_runners under the given env."""
    return subprocess.run(
        [sys.executable, "-c", _IMPORT_SNIPPET],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )


def test_heartbeat_silent_without_slurm():
    """Without SLURM_ARRAY_TASK_ID, importing the package prints nothing."""
    env = {k: v for k, v in os.environ.items() if k != "SLURM_ARRAY_TASK_ID"}
    result = _run_import(env)
    assert "[startup" not in result.stdout, (
        f"expected no [startup ...] line in stdout, got:\n{result.stdout!r}"
    )
    assert result.stdout == "", (
        f"expected empty stdout outside SLURM, got:\n{result.stdout!r}"
    )


def test_heartbeat_fires_under_slurm():
    """With SLURM_ARRAY_TASK_ID set, the heartbeat prints and includes the task id."""
    env = {**os.environ, "SLURM_ARRAY_TASK_ID": "42"}
    result = _run_import(env)
    assert "[startup " in result.stdout, (
        f"expected [startup ...] line in stdout, got:\n{result.stdout!r}"
    )
    assert "task=42" in result.stdout, (
        f"expected task=42 in heartbeat output, got:\n{result.stdout!r}"
    )

"""Shared WhiteboxTools subprocess helpers.

Two helpers used by every WBT call site in this package:

- :func:`find_whitebox_tools_binary` locates the bundled rust binary inside
  the ``whitebox`` pip package, triggering its first-use auto-download.
- :func:`run_streamed` invokes a WBT command and streams the combined
  stdout/stderr line-by-line to ``logger.info`` so progress is visible in
  real time (e.g. in SLURM logs), rather than buffered until exit. Raises
  :class:`RuntimeError` on non-zero exit; the streamed output is the
  diagnostic record.

See issue #48 for the prior ``subprocess.run(capture_output=True)`` behaviour
this replaces.
"""

from __future__ import annotations

import logging
import os
import subprocess


def find_whitebox_tools_binary() -> str:
    """Return the path to the WhiteboxTools executable bundled with `whitebox`.

    Instantiates ``WhiteboxTools()`` first so the rust binary auto-downloads
    on a fresh env (idempotent on subsequent calls), then searches the known
    install locations inside the pip-installed ``whitebox`` package.
    """
    import whitebox
    from whitebox import WhiteboxTools

    WhiteboxTools()  # auto-downloads the binary on first use

    pkg_dir = os.path.dirname(whitebox.__file__)
    candidates = [
        os.path.join(pkg_dir, "whitebox_tools.exe"),
        os.path.join(pkg_dir, "whitebox_tools"),
        os.path.join(pkg_dir, "bin", "whitebox_tools.exe"),
        os.path.join(pkg_dir, "bin", "whitebox_tools"),
    ]
    runner = next((c for c in candidates if os.path.isfile(c)), None)
    if runner is None:
        raise FileNotFoundError(
            "WhiteboxTools binary not found inside `whitebox` package. "
            "Reinstall the `whitebox` pip package."
        )
    return runner


def run_streamed(cmd: list[str], tool: str, logger: logging.Logger) -> None:
    """Run ``cmd`` and stream combined stdout/stderr line-by-line to ``logger``.

    Each output line is logged at INFO with a ``WBT:`` prefix as it arrives,
    so long-running jobs (Watershed, FlowAccumulation, FillDepressions on
    CONUS-scale grids) show progress in real time instead of going silent
    until exit. ``stderr`` is merged into ``stdout`` so error messages stay
    in chronological order with progress output.

    Raises ``RuntimeError`` on non-zero exit; the streamed lines above the
    failure are the diagnostic — no separate stderr block to log.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,  # line-buffered
    )
    assert proc.stdout is not None  # for type checkers; PIPE guarantees this
    for line in proc.stdout:
        logger.info("  WBT: %s", line.rstrip())
    proc.wait()
    if proc.returncode != 0:
        raise RuntimeError(
            f"WhiteboxTools {tool} failed (exit code {proc.returncode}). "
            "See WBT output above."
        )

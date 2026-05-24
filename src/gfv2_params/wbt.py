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
from collections import deque


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


TAIL_LINES = 50


def run_streamed(cmd: list[str], tool: str, logger: logging.Logger) -> None:
    """Run ``cmd`` and stream combined stdout/stderr line-by-line to ``logger``.

    Each output line is logged at INFO with a ``WBT:`` prefix as it arrives,
    so long-running jobs (Watershed, FlowAccumulation, FillDepressions on
    CONUS-scale grids) show progress in real time instead of going silent
    until exit. ``stderr`` is merged into ``stdout`` so error messages stay
    in chronological order with progress output. ``errors="replace"`` guards
    against a stray non-UTF-8 byte raising ``UnicodeDecodeError`` mid-stream
    on a ``LANG=C`` cluster node.

    On exception during iteration (decode failure, ``KeyboardInterrupt``,
    logger-handler exception), the child is killed and reaped before the
    exception propagates — otherwise the WBT child keeps running on a
    CONUS-scale grid and pins the SLURM allocation until wallclock.

    On non-zero exit the last ``TAIL_LINES`` lines of streamed output are
    re-emitted at ERROR before the ``RuntimeError`` is raised, so the
    failure remains diagnosable even if the surrounding log handler is
    configured above INFO.
    """
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        errors="replace",
        bufsize=1,  # line-buffered; only honored with text=True
    )
    if proc.stdout is None:  # pragma: no cover — PIPE guarantees this
        proc.kill()
        proc.wait()
        raise RuntimeError(
            f"WhiteboxTools {tool}: subprocess stdout pipe is None."
        )
    tail: deque[str] = deque(maxlen=TAIL_LINES)
    try:
        for line in proc.stdout:
            stripped = line.rstrip()
            tail.append(stripped)
            logger.info("  WBT: %s", stripped)
    except BaseException:
        proc.kill()
        proc.wait()
        raise
    proc.wait()
    if proc.returncode != 0:
        if tail:
            logger.error(
                "WhiteboxTools %s last %d output line(s):\n  %s",
                tool, len(tail), "\n  ".join(tail),
            )
        raise RuntimeError(
            f"WhiteboxTools {tool} failed (exit code {proc.returncode})."
        )

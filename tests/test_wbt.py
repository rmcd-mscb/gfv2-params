"""Tests for gfv2_params.wbt.run_streamed.

Uses ``sys.executable -c`` as a fake WBT binary so the tests don't depend
on the ``whitebox`` package being importable or its rust binary being
present. The streaming primitive's only contract is "every stdout/stderr
line reaches the logger; non-zero exit raises", which is fully exercisable
with a plain Python subprocess.

``find_whitebox_tools_binary`` is not unit-tested here — it's an
``import whitebox`` + filesystem probe whose failure modes are obvious
on first call.
"""

from __future__ import annotations

import logging
import sys

import pytest

from gfv2_params.wbt import run_streamed


def _python_cmd(snippet: str) -> list[str]:
    """Return a cross-platform [python, -c, snippet] command."""
    return [sys.executable, "-c", snippet]


def test_run_streamed_logs_each_line_at_info(caplog):
    """Every line printed by the subprocess reaches the logger at INFO."""
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_wbt_streaming")
    cmd = _python_cmd("print('first'); print('second'); print('third')")
    run_streamed(cmd, tool="FakeTool", logger=logger)
    messages = [r.getMessage() for r in caplog.records]
    assert any("WBT: first" in m for m in messages), messages
    assert any("WBT: second" in m for m in messages), messages
    assert any("WBT: third" in m for m in messages), messages


def test_run_streamed_merges_stderr_into_stdout(caplog):
    """stderr lines must also reach the logger (stderr=STDOUT is the point)."""
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_wbt_streaming_stderr")
    cmd = _python_cmd(
        "import sys; print('out-line'); print('err-line', file=sys.stderr)"
    )
    run_streamed(cmd, tool="FakeTool", logger=logger)
    messages = [r.getMessage() for r in caplog.records]
    assert any("WBT: out-line" in m for m in messages), messages
    assert any("WBT: err-line" in m for m in messages), messages


def test_run_streamed_raises_on_nonzero_exit(caplog):
    """Non-zero exit → RuntimeError naming the tool and exit code."""
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_wbt_streaming_fail")
    cmd = _python_cmd("import sys; print('before crash'); sys.exit(7)")
    with pytest.raises(RuntimeError) as excinfo:
        run_streamed(cmd, tool="FakeTool", logger=logger)
    assert "FakeTool" in str(excinfo.value)
    assert "exit code 7" in str(excinfo.value)
    # The pre-exit output still streamed before the failure:
    assert any("WBT: before crash" in r.getMessage() for r in caplog.records)


def test_run_streamed_success_returns_none(caplog):
    """Successful run returns None (no exception, no return value)."""
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_wbt_streaming_ok")
    cmd = _python_cmd("pass")
    result = run_streamed(cmd, tool="FakeTool", logger=logger)
    assert result is None

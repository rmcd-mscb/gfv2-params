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


def test_run_streamed_raises_when_nonzero_exit_with_no_output(caplog):
    """Worst-UX path: subprocess exits non-zero having printed nothing.

    Must still raise cleanly (no hang on the empty pipe, no spurious WBT log
    records, no tail-ERROR record since the tail is empty).
    """
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_wbt_streaming_silent_fail")
    cmd = _python_cmd("import sys; sys.exit(2)")
    with pytest.raises(RuntimeError) as excinfo:
        run_streamed(cmd, tool="FakeTool", logger=logger)
    assert "FakeTool" in str(excinfo.value)
    assert "exit code 2" in str(excinfo.value)
    wbt_messages = [r.getMessage() for r in caplog.records if "WBT:" in r.getMessage()]
    assert wbt_messages == [], wbt_messages
    tail_errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert tail_errors == [], [r.getMessage() for r in tail_errors]


def test_run_streamed_logs_tail_at_error_on_nonzero_exit(caplog):
    """When output exists, the last lines are re-emitted at ERROR before the raise.

    Pins #5 from the multi-agent review: a log handler configured above INFO
    would otherwise see the RuntimeError with zero context.
    """
    caplog.set_level(logging.INFO)
    logger = logging.getLogger("test_wbt_streaming_tail_error")
    cmd = _python_cmd("print('progress-1'); print('progress-2'); import sys; sys.exit(3)")
    with pytest.raises(RuntimeError):
        run_streamed(cmd, tool="FakeTool", logger=logger)
    tail_errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert len(tail_errors) == 1, [r.getMessage() for r in tail_errors]
    err_msg = tail_errors[0].getMessage()
    assert "progress-1" in err_msg
    assert "progress-2" in err_msg
    assert "FakeTool" in err_msg

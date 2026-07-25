"""Tests for retry rendering in the progress display and summaries (WIL-108)."""

import io

from rich.console import Console

from brasa.engine.reporting import (
    ProgressDisplay,
    TaskResult,
    TaskStatus,
    Verbosity,
)


def _make_result(status=TaskStatus.FAILED, retries=None):
    result = TaskResult(status=status, operation="download", template_name="tpl")
    if retries is not None:
        result.extra_info["retry_attempts_used"] = str(retries)
    return result


def _render(result):
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, width=200)
    display = ProgressDisplay(
        total=1,
        operation="download",
        template_name="tpl",
        verbosity=Verbosity.NORMAL,
        console=console,
    )
    display.update(result)
    return buf.getvalue()


def test_retries_render_before_status_char():
    assert "rrrF" in _render(_make_result(retries=3))


def test_no_retries_render_plain_symbol():
    out = _render(_make_result(retries=None))
    assert "r" not in out.split("[")[0]
    assert "F" in out

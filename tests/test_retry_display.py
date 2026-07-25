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


def test_task_report_summary_counts_retries():
    from brasa.engine.reporting import TaskReport

    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, width=200)
    report = TaskReport(
        operation="download",
        template_name="tpl",
        verbosity=Verbosity.NORMAL,
        console=console,
    )
    report.start(total=2)
    report.add_result(_make_result(retries=3))
    report.add_result(_make_result(status=TaskStatus.PASSED, retries=2))
    report.finish()
    assert "5 retries" in buf.getvalue()


def test_plan_report_status_str_counts_retries():
    from brasa.engine.download_plan import DownloadPlanReport
    from brasa.engine.reporting import TaskReport

    report = TaskReport(operation="download", template_name="tpl")
    report.results = [_make_result(retries=2), _make_result(retries=None)]
    line = DownloadPlanReport._report_status_str(report)
    assert "2 retries" in line


def test_summary_omits_retries_when_zero():
    from brasa.engine.download_plan import DownloadPlanReport
    from brasa.engine.reporting import TaskReport

    report = TaskReport(operation="download", template_name="tpl")
    report.results = [_make_result(retries=None)]
    assert "retries" not in DownloadPlanReport._report_status_str(report)

"""`python -m brasa` entry point and `brasa process` exit codes (WIL-178)."""

import subprocess
import sys

import pytest

from brasa import cli
from brasa.engine.reporting import (
    TaskReport,
    Verbosity,
    create_task_result_from_exception,
    create_task_result_success,
)


def _report(template: str, result) -> TaskReport:
    report = TaskReport(
        operation="process", template_name=template, verbosity=Verbosity.QUIET
    )
    report.start(total=1)
    report.add_result(result)
    report.finish()
    return report


def test_python_dash_m_brasa_help():
    proc = subprocess.run(
        [sys.executable, "-m", "brasa", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0
    assert "run-all" in proc.stdout


def test_process_exits_1_when_a_result_fails(monkeypatch):
    def fake_process(template_name, **kwargs):
        result = create_task_result_from_exception(
            exception=RuntimeError("boom"),
            operation="process",
            template_name=template_name,
            args={},
            duration=0.0,
        )
        return _report(template_name, result)

    monkeypatch.setattr("brasa.cli.process_marketdata", fake_process)
    with pytest.raises(SystemExit) as exc:
        cli.main(["process", "b3-bvbg028", "-q"])
    assert exc.value.code == 1


def test_process_exits_0_on_success(monkeypatch):
    def fake_process(template_name, **kwargs):
        result = create_task_result_success(
            operation="process",
            template_name=template_name,
            args={},
            duration=0.0,
        )
        return _report(template_name, result)

    monkeypatch.setattr("brasa.cli.process_marketdata", fake_process)
    cli.main(["process", "b3-bvbg028", "-q"])  # must not raise SystemExit

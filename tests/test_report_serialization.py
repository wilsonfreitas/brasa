"""Tests for report serialization with download status fields.

TEST-009: Verify report JSON contains download_status_code,
download_status_name, download_status_reason, and http_status.
"""

import json
import tempfile
from pathlib import Path

from brasa.engine.reporting import (
    TaskReport,
    TaskResult,
    TaskStatus,
    Verbosity,
)


def _make_result(
    status: TaskStatus,
    download_code: str,
    download_name: str,
    reason: str = "",
    http_status: str | None = None,
) -> TaskResult:
    """Helper to create a TaskResult with download status extra_info."""
    result = TaskResult(
        status=status,
        operation="download",
        template_name="test-template",
        args={"refdate": "2025-01-01"},
        duration_seconds=0.5,
    )
    result.extra_info["download_status_code"] = download_code
    result.extra_info["download_status_name"] = download_name
    result.extra_info["download_status_reason"] = reason
    if http_status:
        result.extra_info["http_status"] = http_status
    return result


class TestReportJsonContainsStatusFields:
    """TEST-009: JSON report includes deterministic status fields."""

    def test_passed_status_in_json(self):
        report = TaskReport("download", "test-template", Verbosity.QUIET)
        report.start(total=1)
        result = _make_result(TaskStatus.PASSED, ".", "PASSED")
        report.add_result(result)
        report.finish()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            filepath = f.name

        report.save_report(filepath, format="json")
        with Path(filepath).open() as f:
            data = json.load(f)

        r = data["results"][0]
        assert r["extra_info"]["download_status_code"] == "."
        assert r["extra_info"]["download_status_name"] == "PASSED"
        Path(filepath).unlink()

    def test_failed_status_with_http_in_json(self):
        report = TaskReport("download", "test-template", Verbosity.QUIET)
        report.start(total=1)
        result = _make_result(
            TaskStatus.FAILED,
            "F",
            "FAILED",
            reason="status_code = 404",
            http_status="404",
        )
        report.add_result(result)
        report.finish()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            filepath = f.name

        report.save_report(filepath, format="json")
        with Path(filepath).open() as f:
            data = json.load(f)

        r = data["results"][0]
        assert r["extra_info"]["download_status_code"] == "F"
        assert r["extra_info"]["http_status"] == "404"
        Path(filepath).unlink()

    def test_skipped_status_in_json(self):
        report = TaskReport("download", "test-template", Verbosity.QUIET)
        report.start(total=1)
        result = _make_result(TaskStatus.SKIPPED, "S", "SKIPPED")
        report.add_result(result)
        report.finish()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            filepath = f.name

        report.save_report(filepath, format="json")
        with Path(filepath).open() as f:
            data = json.load(f)

        r = data["results"][0]
        assert r["extra_info"]["download_status_code"] == "S"
        Path(filepath).unlink()

    def test_duplicated_status_in_json(self):
        report = TaskReport("download", "test-template", Verbosity.QUIET)
        report.start(total=1)
        result = _make_result(
            TaskStatus.PASSED, "D", "DUPLICATED", reason="folder exists"
        )
        report.add_result(result)
        report.finish()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            filepath = f.name

        report.save_report(filepath, format="json")
        with Path(filepath).open() as f:
            data = json.load(f)

        r = data["results"][0]
        assert r["extra_info"]["download_status_code"] == "D"
        assert data["summary"]["duplicated"] == 1
        Path(filepath).unlink()

    def test_invalid_status_in_json(self):
        report = TaskReport("download", "test-template", Verbosity.QUIET)
        report.start(total=1)
        result = _make_result(
            TaskStatus.FAILED,
            "I",
            "INVALID",
            reason="validation failed for file",
        )
        report.add_result(result)
        report.finish()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            filepath = f.name

        report.save_report(filepath, format="json")
        with Path(filepath).open() as f:
            data = json.load(f)

        r = data["results"][0]
        assert r["extra_info"]["download_status_code"] == "I"
        assert data["summary"]["invalid"] == 1
        Path(filepath).unlink()

    def test_error_status_in_json(self):
        report = TaskReport("download", "test-template", Verbosity.QUIET)
        report.start(total=1)
        result = _make_result(TaskStatus.ERROR, "E", "ERROR", reason="unexpected crash")
        report.add_result(result)
        report.finish()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            filepath = f.name

        report.save_report(filepath, format="json")
        with Path(filepath).open() as f:
            data = json.load(f)

        r = data["results"][0]
        assert r["extra_info"]["download_status_code"] == "E"
        Path(filepath).unlink()

    def test_summary_counts_all_status_types(self):
        report = TaskReport("download", "test-template", Verbosity.QUIET)
        report.start(total=4)
        report.add_result(_make_result(TaskStatus.PASSED, ".", "PASSED"))
        report.add_result(_make_result(TaskStatus.PASSED, "D", "DUPLICATED"))
        report.add_result(_make_result(TaskStatus.FAILED, "I", "INVALID"))
        report.add_result(_make_result(TaskStatus.FAILED, "C", "CORRUPTED"))
        report.finish()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            filepath = f.name

        report.save_report(filepath, format="json")
        with Path(filepath).open() as f:
            data = json.load(f)

        assert data["summary"]["duplicated"] == 1
        assert data["summary"]["invalid"] == 1
        assert data["summary"]["corrupted"] == 1
        assert data["summary"]["passed"] == 2
        assert data["summary"]["failed"] == 2
        Path(filepath).unlink()

    def test_corrupted_status_in_json(self):
        report = TaskReport("download", "test-template", Verbosity.QUIET)
        report.start(total=1)
        result = _make_result(
            TaskStatus.FAILED,
            "C",
            "CORRUPTED",
            reason="truncated file detected",
        )
        report.add_result(result)
        report.finish()

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            filepath = f.name

        report.save_report(filepath, format="json")
        with Path(filepath).open() as f:
            data = json.load(f)

        r = data["results"][0]
        assert r["extra_info"]["download_status_code"] == "C"
        assert data["summary"]["corrupted"] == 1
        Path(filepath).unlink()

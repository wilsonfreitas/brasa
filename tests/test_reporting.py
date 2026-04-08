"""Tests for DownloadAttemptStatus enum and helpers in reporting module.

Tests cover:
- Symbol set uniqueness and completeness
- Deterministic exception-to-status mapping
- to_task_status conversion compatibility
"""

from __future__ import annotations

from typing import ClassVar

from brasa.engine.exceptions import (
    CorruptedContentException,
    DownloadException,
    DuplicatedFolderException,
    InvalidContentException,
)
from brasa.engine.reporting import (
    DownloadAttemptStatus,
    TaskStatus,
    map_exception_to_download_status,
    to_task_status,
)


class TestDownloadAttemptStatusSymbols:
    """TEST-001: Verify symbol uniqueness and completeness."""

    EXPECTED_SYMBOLS: ClassVar[set[str]] = {".", "F", "E", "S", "D", "I", "C", "W"}

    def test_symbol_set_matches_spec(self):
        """All expected symbols are present."""
        symbols = {s.symbol for s in DownloadAttemptStatus}
        assert symbols == self.EXPECTED_SYMBOLS

    def test_symbols_are_unique(self):
        """No two statuses share a symbol."""
        symbols = [s.symbol for s in DownloadAttemptStatus]
        assert len(symbols) == len(set(symbols))

    def test_symbols_are_single_character(self):
        """Each symbol is exactly one character."""
        for status in DownloadAttemptStatus:
            assert len(status.symbol) == 1, f"{status.name} symbol is not single-char"

    def test_all_members_present(self):
        """Enum contains exactly the 8 defined members."""
        expected_names = {
            "PASSED",
            "FAILED",
            "ERROR",
            "SKIPPED",
            "DUPLICATED",
            "INVALID",
            "CORRUPTED",
            "WARNING",
        }
        assert {s.name for s in DownloadAttemptStatus} == expected_names

    def test_specific_symbol_assignments(self):
        """Verify each status maps to its specified symbol."""
        assert DownloadAttemptStatus.PASSED.symbol == "."
        assert DownloadAttemptStatus.FAILED.symbol == "F"
        assert DownloadAttemptStatus.ERROR.symbol == "E"
        assert DownloadAttemptStatus.SKIPPED.symbol == "S"
        assert DownloadAttemptStatus.DUPLICATED.symbol == "D"
        assert DownloadAttemptStatus.INVALID.symbol == "I"
        assert DownloadAttemptStatus.CORRUPTED.symbol == "C"
        assert DownloadAttemptStatus.WARNING.symbol == "W"


class TestMapExceptionToDownloadStatus:
    """TEST-002 through TEST-005: Deterministic exception mapping."""

    def test_none_maps_to_passed(self):
        """None -> PASSED."""
        assert map_exception_to_download_status(None) == (DownloadAttemptStatus.PASSED)

    def test_download_exception_maps_to_failed(self):
        """DownloadException -> FAILED (TEST-004)."""
        ex = DownloadException("status_code = 404")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.FAILED)

    def test_duplicated_folder_maps_to_duplicated(self):
        """DuplicatedFolderException -> DUPLICATED (TEST-002)."""
        ex = DuplicatedFolderException("folder already exists")
        assert map_exception_to_download_status(ex) == (
            DownloadAttemptStatus.DUPLICATED
        )

    def test_invalid_content_maps_to_invalid(self):
        """InvalidContentException -> INVALID (TEST-003)."""
        ex = InvalidContentException("validation failed")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.INVALID)

    def test_corrupted_content_maps_to_corrupted(self):
        """CorruptedContentException -> CORRUPTED."""
        ex = CorruptedContentException("truncated file")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.CORRUPTED)

    def test_generic_exception_maps_to_error(self):
        """Generic Exception -> ERROR (TEST-005)."""
        ex = RuntimeError("unexpected error")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.ERROR)

    def test_value_error_maps_to_error(self):
        """ValueError -> ERROR."""
        ex = ValueError("bad value")
        assert map_exception_to_download_status(ex) == (DownloadAttemptStatus.ERROR)


class TestToTaskStatus:
    """Verify to_task_status conversion preserves backward compatibility."""

    def test_passed_to_task_passed(self):
        assert to_task_status(DownloadAttemptStatus.PASSED) == TaskStatus.PASSED

    def test_failed_to_task_failed(self):
        assert to_task_status(DownloadAttemptStatus.FAILED) == TaskStatus.FAILED

    def test_error_to_task_error(self):
        assert to_task_status(DownloadAttemptStatus.ERROR) == TaskStatus.ERROR

    def test_skipped_to_task_skipped(self):
        assert to_task_status(DownloadAttemptStatus.SKIPPED) == TaskStatus.SKIPPED

    def test_duplicated_to_task_passed(self):
        """DUPLICATED is cache-reusable, maps to PASSED."""
        assert to_task_status(DownloadAttemptStatus.DUPLICATED) == (
            TaskStatus.DUPLICATED
        )

    def test_invalid_to_task_failed(self):
        """INVALID is a content-validation failure, maps to FAILED."""
        assert to_task_status(DownloadAttemptStatus.INVALID) == (TaskStatus.INVALID)

    def test_corrupted_to_task_failed(self):
        """CORRUPTED is a transient failure, maps to FAILED."""
        assert to_task_status(DownloadAttemptStatus.CORRUPTED) == (TaskStatus.CORRUPTED)

    def test_warning_to_task_warning(self):
        assert to_task_status(DownloadAttemptStatus.WARNING) == (TaskStatus.WARNING)

    def test_all_statuses_have_mapping(self):
        """Every DownloadAttemptStatus must have a to_task_status mapping."""
        for status in DownloadAttemptStatus:
            result = to_task_status(status)
            assert isinstance(result, TaskStatus), f"{status.name} has no mapping"


class TestDownloadAttemptStatusColors:
    """Verify every status has a color attribute."""

    def test_all_statuses_have_color(self):
        for status in DownloadAttemptStatus:
            assert isinstance(status.color, str)
            assert len(status.color) > 0


class TestProgressDisplayShowSkipped:
    """Tests for ProgressDisplay show_skipped parameter."""

    from io import StringIO

    from brasa.engine.reporting import (
        ProgressDisplay,
        TaskResult,
        TaskStatus,
        Verbosity,
    )

    def test_progress_display_suppresses_skipped_by_default(self):
        """S symbols should be suppressed when show_skipped=False."""
        import re
        from io import StringIO

        from rich.console import Console

        from brasa.engine.reporting import ProgressDisplay, TaskResult, TaskStatus

        output = StringIO()
        console = Console(file=output, force_terminal=True)

        progress = ProgressDisplay(
            total=3,
            operation="process",
            template_name="test",
            show_skipped=False,
            console=console,
        )
        progress.start()

        # Add results: PASSED, SKIPPED, SKIPPED
        result1 = TaskResult(
            operation="process",
            template_name="test",
            args={},
            status=TaskStatus.PASSED,
            duration_seconds=0.1,
            downloaded_files=[],
            is_processed=True,
        )
        result2 = TaskResult(
            operation="process",
            template_name="test",
            args={},
            status=TaskStatus.SKIPPED,
            duration_seconds=0.0,
            downloaded_files=[],
            is_processed=True,
        )
        result3 = TaskResult(
            operation="process",
            template_name="test",
            args={},
            status=TaskStatus.SKIPPED,
            duration_seconds=0.0,
            downloaded_files=[],
            is_processed=True,
        )

        progress.update(result1)
        progress.update(result2)
        progress.update(result3)
        progress.finish()

        output_str = output.getvalue()
        # Remove ANSI codes for easier assertions
        clean_output = re.sub(r"\x1b\[[0-9;]*m", "", output_str)

        # Should show . but not S
        assert "." in clean_output
        assert clean_output.count("S") == 0, "S symbols should be suppressed"
        # Counter should still show 3/3 at the end
        assert "[3/3]" in clean_output, f"Counter [3/3] not found in: {clean_output}"

    def test_progress_display_shows_skipped_with_flag(self):
        """S symbols should be shown when show_skipped=True."""
        import re
        from io import StringIO

        from rich.console import Console

        from brasa.engine.reporting import ProgressDisplay, TaskResult, TaskStatus

        output = StringIO()
        console = Console(file=output, force_terminal=True)

        progress = ProgressDisplay(
            total=3,
            operation="process",
            template_name="test",
            show_skipped=True,
            console=console,
        )
        progress.start()

        result1 = TaskResult(
            operation="process",
            template_name="test",
            args={},
            status=TaskStatus.PASSED,
            duration_seconds=0.1,
            downloaded_files=[],
            is_processed=True,
        )
        result2 = TaskResult(
            operation="process",
            template_name="test",
            args={},
            status=TaskStatus.SKIPPED,
            duration_seconds=0.0,
            downloaded_files=[],
            is_processed=True,
        )

        progress.update(result1)
        progress.update(result2)
        progress.finish()

        output_str = output.getvalue()
        clean_output = re.sub(r"\x1b\[[0-9;]*m", "", output_str)

        # Should show both . and S
        assert "." in clean_output
        assert "S" in clean_output, "S symbols should be shown"

    def test_verbose_mode_always_shows_skipped(self):
        """In VERBOSE mode, S symbols should always be shown regardless of flag."""
        from io import StringIO

        from rich.console import Console

        from brasa.engine.reporting import (
            ProgressDisplay,
            TaskResult,
            TaskStatus,
            Verbosity,
        )

        output = StringIO()
        console = Console(file=output, force_terminal=True)

        progress = ProgressDisplay(
            total=2,
            operation="process",
            template_name="test",
            verbosity=Verbosity.VERBOSE,
            show_skipped=False,  # Even though False, VERBOSE should override
            console=console,
        )
        progress.start()

        result1 = TaskResult(
            operation="process",
            template_name="test",
            args={},
            status=TaskStatus.SKIPPED,
            duration_seconds=0.0,
            downloaded_files=[],
            is_processed=True,
        )

        progress.update(result1)
        progress.finish()

        output_str = output.getvalue()
        # VERBOSE mode shows full status names, should include "SKIPPED"
        assert "SKIPPED" in output_str


class TestTaskReportCachedCount:
    """Tests for TaskReport prefiltered_skip_count display."""

    def test_summary_line_shows_cached_count(self):
        """Summary should include 'N cached' when prefiltered_skip_count > 0."""
        import re
        from io import StringIO

        from rich.console import Console

        from brasa.engine.reporting import TaskReport, TaskResult, TaskStatus

        output = StringIO()
        console = Console(file=output, force_terminal=True)

        report = TaskReport(
            operation="process",
            template_name="test-template",
            console=console,
        )
        report.start(total=5, prefiltered_skip_count=2)

        # Add some results
        for i in range(3):
            result = TaskResult(
                operation="process",
                template_name="test-template",
                args={"idx": i},
                status=TaskStatus.PASSED if i == 0 else TaskStatus.SKIPPED,
                duration_seconds=0.1,
                downloaded_files=[],
                is_processed=True,
            )
            report.add_result(result)

        report.finish()
        output_str = output.getvalue()
        clean_output = re.sub(r"\x1b\[[0-9;]*m", "", output_str)

        # Should contain the cached count in the summary
        assert "2 cached" in clean_output, (
            f"Summary should show cached count. Got: {clean_output}"
        )

    def test_summary_without_cached_count(self):
        """Summary should not show 'cached' when prefiltered_skip_count is 0."""
        import re
        from io import StringIO

        from rich.console import Console

        from brasa.engine.reporting import TaskReport, TaskResult, TaskStatus

        output = StringIO()
        console = Console(file=output, force_terminal=True)

        report = TaskReport(
            operation="process",
            template_name="test-template",
            console=console,
        )
        report.start(total=1, prefiltered_skip_count=0)

        result = TaskResult(
            operation="process",
            template_name="test-template",
            args={},
            status=TaskStatus.PASSED,
            duration_seconds=0.1,
            downloaded_files=[],
            is_processed=True,
        )
        report.add_result(result)
        report.finish()

        output_str = output.getvalue()
        clean_output = re.sub(r"\x1b\[[0-9;]*m", "", output_str)

        # Should NOT contain "cached" when count is 0
        assert "cached" not in clean_output, (
            "Summary should not show cached when count is 0"
        )

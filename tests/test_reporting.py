"""Tests for the reporting module's live progress/report display."""

from __future__ import annotations


class TestProgressDisplayShowSkipped:
    """Tests for ProgressDisplay show_skipped parameter."""

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

"""Progress reporting and error display for market data operations.

This module provides a pytest-style progress and error reporting system
for download, processing, and ETL operations. It captures all outcomes
including successes, failures, errors, and warnings, then displays
a comprehensive report at the end.

Classes:
    TaskStatus: Enum for task outcomes (PASSED, FAILED, ERROR, SKIPPED, WARNING)
    TaskResult: Captures the outcome of a single operation
    TaskReport: Collects results and generates final report
    ProgressDisplay: Handles real-time output (symbols + counter)
    Verbosity: Enum for output verbosity levels
"""

from __future__ import annotations

import json
import traceback
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.text import Text


class DownloadAttemptStatus(Enum):
    """Deterministic status code for every download attempt.

    Each download attempt produces exactly one status code that
    classifies the outcome. Symbols are single-character and
    non-conflicting.

    Attributes:
        PASSED: Successful download completion.
        FAILED: Expected download failure (DownloadException).
        ERROR: Unexpected unhandled exception.
        SKIPPED: Download skipped (_should_download returned False).
        DUPLICATED: Target raw folder already exists (DuplicatedFolderException).
        INVALID: Downloaded file fails validation (InvalidContentException).
        WARNING: Successful download with non-terminal warnings.
    """

    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"
    DUPLICATED = "duplicated"
    INVALID = "invalid"
    CORRUPTED = "corrupted"
    WARNING = "warning"

    @property
    def symbol(self) -> str:
        """Get the single-character symbol for this status.

        Returns:
            A single character representing the status.
        """
        symbols = {
            DownloadAttemptStatus.PASSED: ".",
            DownloadAttemptStatus.FAILED: "F",
            DownloadAttemptStatus.ERROR: "E",
            DownloadAttemptStatus.SKIPPED: "S",
            DownloadAttemptStatus.DUPLICATED: "D",
            DownloadAttemptStatus.INVALID: "I",
            DownloadAttemptStatus.CORRUPTED: "C",
            DownloadAttemptStatus.WARNING: "W",
        }
        return symbols[self]

    @property
    def color(self) -> str:
        """Get the rich color for this status.

        Returns:
            A rich color string for terminal display.
        """
        colors = {
            DownloadAttemptStatus.PASSED: "green",
            DownloadAttemptStatus.FAILED: "red",
            DownloadAttemptStatus.ERROR: "red bold",
            DownloadAttemptStatus.SKIPPED: "yellow",
            DownloadAttemptStatus.DUPLICATED: "cyan",
            DownloadAttemptStatus.INVALID: "magenta",
            DownloadAttemptStatus.CORRUPTED: "yellow",
            DownloadAttemptStatus.WARNING: "yellow",
        }
        return colors[self]


def map_exception_to_download_status(
    ex: Exception | None,
) -> DownloadAttemptStatus:
    """Map an exception to a deterministic DownloadAttemptStatus.

    Uses the exception taxonomy defined in brasa/engine/exceptions.py
    to classify each outcome:
    - None -> PASSED
    - DownloadException -> FAILED
    - DuplicatedFolderException -> DUPLICATED
    - InvalidContentException -> INVALID
    - Any other Exception -> ERROR

    Args:
        ex: The exception raised during download, or None for success.

    Returns:
        The corresponding DownloadAttemptStatus.
    """
    from .exceptions import (
        CorruptedContentException,
        DownloadException,
        DuplicatedFolderException,
        InvalidContentException,
    )

    if ex is None:
        return DownloadAttemptStatus.PASSED
    if isinstance(ex, DuplicatedFolderException):
        return DownloadAttemptStatus.DUPLICATED
    if isinstance(ex, CorruptedContentException):
        return DownloadAttemptStatus.CORRUPTED
    if isinstance(ex, InvalidContentException):
        return DownloadAttemptStatus.INVALID
    if isinstance(ex, DownloadException):
        return DownloadAttemptStatus.FAILED
    return DownloadAttemptStatus.ERROR


def to_task_status(
    download_status: DownloadAttemptStatus,
) -> TaskStatus:
    """Convert a DownloadAttemptStatus to the legacy TaskStatus.

    Maintains backward compatibility for downstream code that
    consumes TaskReport / TaskStatus.

    Mapping is now 1:1 — every DownloadAttemptStatus maps to the
    identically-named TaskStatus member so reports preserve the
    exact outcome.

    Args:
        download_status: The download attempt status to convert.

    Returns:
        The corresponding TaskStatus for report integration.
    """
    mapping = {
        DownloadAttemptStatus.PASSED: TaskStatus.PASSED,
        DownloadAttemptStatus.FAILED: TaskStatus.FAILED,
        DownloadAttemptStatus.ERROR: TaskStatus.ERROR,
        DownloadAttemptStatus.SKIPPED: TaskStatus.SKIPPED,
        DownloadAttemptStatus.DUPLICATED: TaskStatus.DUPLICATED,
        DownloadAttemptStatus.INVALID: TaskStatus.INVALID,
        DownloadAttemptStatus.CORRUPTED: TaskStatus.CORRUPTED,
        DownloadAttemptStatus.WARNING: TaskStatus.WARNING,
    }
    return mapping[download_status]


class TaskStatus(Enum):
    """Status of a task execution.

    Members mirror DownloadAttemptStatus so that reports expose
    the exact outcome without lossy mapping.
    """

    PASSED = "passed"
    FAILED = "failed"
    ERROR = "error"
    SKIPPED = "skipped"
    DUPLICATED = "duplicated"
    INVALID = "invalid"
    CORRUPTED = "corrupted"
    WARNING = "warning"

    @property
    def symbol(self) -> str:
        """Get the single-character symbol for this status."""
        symbols = {
            TaskStatus.PASSED: ".",
            TaskStatus.FAILED: "F",
            TaskStatus.ERROR: "E",
            TaskStatus.SKIPPED: "S",
            TaskStatus.DUPLICATED: "D",
            TaskStatus.INVALID: "I",
            TaskStatus.CORRUPTED: "C",
            TaskStatus.WARNING: "W",
        }
        return symbols[self]

    @property
    def color(self) -> str:
        """Get the rich color for this status."""
        colors = {
            TaskStatus.PASSED: "green",
            TaskStatus.FAILED: "red",
            TaskStatus.ERROR: "red bold",
            TaskStatus.SKIPPED: "yellow",
            TaskStatus.DUPLICATED: "cyan",
            TaskStatus.INVALID: "magenta",
            TaskStatus.CORRUPTED: "yellow",
            TaskStatus.WARNING: "yellow",
        }
        return colors[self]


class Verbosity(Enum):
    """Verbosity level for output."""

    QUIET = "quiet"
    NORMAL = "normal"
    VERBOSE = "verbose"


@dataclass
class TaskResult:
    """Result of a single task execution.

    Captures all relevant information about a task's outcome for
    reporting purposes.
    """

    status: TaskStatus
    operation: str
    template_name: str
    args: dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0
    error_type: str | None = None
    error_message: str | None = None
    error_traceback: str | None = None
    warnings: list[str] = field(default_factory=list)
    downloaded_files: list[str] = field(default_factory=list)
    processed_files: dict[str, str] = field(default_factory=dict)
    extra_info: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        """Convert result to a dictionary for JSON serialization."""
        return {
            "status": self.status.value,
            "operation": self.operation,
            "template_name": self.template_name,
            "args": {k: str(v) for k, v in self.args.items()},
            "duration_seconds": self.duration_seconds,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "error_traceback": self.error_traceback,
            "warnings": self.warnings,
            "downloaded_files": self.downloaded_files,
            "processed_files": self.processed_files,
            "extra_info": {k: str(v) for k, v in self.extra_info.items()},
            "timestamp": self.timestamp.isoformat(),
        }

    @property
    def args_summary(self) -> str:
        """Get a short summary of the task arguments."""
        if not self.args:
            return ""
        parts = []
        for key, value in self.args.items():
            if hasattr(value, "strftime"):
                parts.append(f"{key}={value.strftime('%Y-%m-%d')}")
            else:
                parts.append(f"{key}={value}")
        return ", ".join(parts)


class ProgressDisplay:
    """Handles real-time progress display.

    Shows pytest-style symbols (. for pass, F for fail, etc.) and
    a counter of completed/total tasks.
    """

    def __init__(
        self,
        total: int,
        operation: str,
        template_name: str,
        verbosity: Verbosity = Verbosity.NORMAL,
        console: Console | None = None,
    ) -> None:
        """Initialize progress display.

        Args:
            total: Total number of tasks to execute.
            operation: Name of the operation (download, process, etl).
            template_name: Name of the template being processed.
            verbosity: Output verbosity level.
            console: Rich console for output (uses stderr by default).
        """
        self.total = total
        self.current = 0
        self.operation = operation
        self.template_name = template_name
        self.verbosity = verbosity
        self.console = console or Console(stderr=True)
        self._line_length = 0
        self._start_time = datetime.now()

    def start(self) -> None:
        """Start the progress display."""
        if self.verbosity == Verbosity.QUIET:
            return

        header = f"{self.operation.capitalize()} {self.template_name} "
        if self.verbosity == Verbosity.NORMAL:
            self.console.print(header, end="")
            self._line_length = len(header)
        elif self.verbosity == Verbosity.VERBOSE:
            self.console.print(header)

    def update(self, result: TaskResult) -> None:
        """Update progress with a task result.

        Args:
            result: The result of the completed task.
        """
        self.current += 1

        if self.verbosity == Verbosity.QUIET:
            return

        if self.verbosity == Verbosity.NORMAL:
            symbol = Text(result.status.symbol, style=result.status.color)
            self.console.print(symbol, end="")
            self._line_length += 1

            # Show counter every 50 symbols or at the end
            if self.current % 50 == 0 or self.current == self.total:
                counter = f" [{self.current}/{self.total}]"
                self.console.print(counter, end="")
                self._line_length += len(counter)

                # Newline every 50 symbols
                if self.current % 50 == 0 and self.current < self.total:
                    self.console.print()
                    # Indent continuation lines
                    indent = " " * len(
                        f"{self.operation.capitalize()} {self.template_name} "
                    )
                    self.console.print(indent, end="")
                    self._line_length = len(indent)

        elif self.verbosity == Verbosity.VERBOSE:
            status_text = Text(result.status.value.upper(), style=result.status.color)
            args_str = result.args_summary or "(no args)"
            self.console.print(f"  {args_str} ", end="")
            self.console.print(status_text, end="")

            # Show skipped indicator
            if result.status == TaskStatus.SKIPPED:
                self.console.print(" [dim](skipped)[/dim]")
            else:
                self.console.print()

            # Show warnings inline in verbose mode
            for warning in result.warnings:
                self.console.print(f"    [yellow]⚠ {warning}[/yellow]")

    def finish(self) -> None:
        """Finish the progress display."""
        if self.verbosity == Verbosity.QUIET:
            return

        if self.verbosity == Verbosity.NORMAL:
            elapsed = (datetime.now() - self._start_time).total_seconds()
            self.console.print(f" ({elapsed:.1f}s)")


class TaskReport:
    """Collects task results and generates reports.

    Provides both real-time progress display and comprehensive
    end-of-run reporting with error details.
    """

    def __init__(
        self,
        operation: str,
        template_name: str,
        verbosity: Verbosity = Verbosity.NORMAL,
        console: Console | None = None,
    ) -> None:
        """Initialize the task report.

        Args:
            operation: Name of the operation (download, process, etl).
            template_name: Name of the template being processed.
            verbosity: Output verbosity level.
            console: Rich console for output.
        """
        self.operation = operation
        self.template_name = template_name
        self.verbosity = verbosity
        self.console = console or Console(stderr=True)
        self.results: list[TaskResult] = []
        self._start_time: datetime | None = None
        self._end_time: datetime | None = None
        self._progress: ProgressDisplay | None = None
        self._captured_warnings: list[tuple[str, TaskResult | None]] = []

    def start(self, total: int) -> None:
        """Start collecting results.

        Args:
            total: Total number of tasks to execute.
        """
        self._start_time = datetime.now()
        self._progress = ProgressDisplay(
            total=total,
            operation=self.operation,
            template_name=self.template_name,
            verbosity=self.verbosity,
            console=self.console,
        )
        self._progress.start()

    def add_result(self, result: TaskResult) -> None:
        """Add a task result.

        Args:
            result: The result to add.
        """
        self.results.append(result)
        if self._progress:
            self._progress.update(result)

    def finish(self) -> None:
        """Finish collecting results and display report."""
        self._end_time = datetime.now()
        if self._progress:
            self._progress.finish()

        # Always show report if there are failures/errors
        # In quiet mode, only show if there are problems
        _problem_statuses = (
            TaskStatus.FAILED,
            TaskStatus.ERROR,
            TaskStatus.INVALID,
            TaskStatus.CORRUPTED,
        )
        has_problems = any(r.status in _problem_statuses for r in self.results)
        has_warnings = any(
            r.status == TaskStatus.WARNING or r.warnings for r in self.results
        )

        if self.verbosity == Verbosity.QUIET and not has_problems:
            return

        if has_problems or (has_warnings and self.verbosity != Verbosity.QUIET):
            self._print_detailed_report()

        self._print_summary()

    def _print_detailed_report(self) -> None:
        """Print detailed report of failures, errors, and warnings."""
        failures = [
            r
            for r in self.results
            if r.status
            in (
                TaskStatus.FAILED,
                TaskStatus.INVALID,
                TaskStatus.CORRUPTED,
            )
        ]
        errors = [r for r in self.results if r.status == TaskStatus.ERROR]
        warnings_results = [
            r for r in self.results if r.status == TaskStatus.WARNING or r.warnings
        ]

        self.console.print()

        if failures or errors:
            self.console.print()
            self.console.rule("[bold red]FAILURES / ERRORS[/bold red]", style="red")
            self.console.print()

            for idx, result in enumerate(failures + errors, 1):
                self._print_result_detail(idx, result)

        if warnings_results:
            self.console.print()
            self.console.rule("[bold yellow]WARNINGS[/bold yellow]", style="yellow")
            self.console.print()

            for idx, result in enumerate(warnings_results, 1):
                self._print_warning_detail(idx, result)

    def _print_result_detail(self, index: int, result: TaskResult) -> None:
        """Print detailed information about a failed/error result."""
        status_color = (
            result.status.color if result.status != TaskStatus.ERROR else "red bold"
        )
        header = f"[{index}] {result.status.value.upper()} {result.operation} {result.args_summary}"

        self.console.print(
            Panel(
                self._format_result_content(result),
                title=header,
                title_align="left",
                border_style=status_color,
                padding=(0, 1),
            )
        )
        self.console.print()

    def _format_result_content(self, result: TaskResult) -> Text:
        """Format the content of a result for display."""
        text = Text()

        # Template and operation info
        text.append("Template:    ", style="bold")
        text.append(f"{result.template_name}\n")

        text.append("Operation:   ", style="bold")
        text.append(f"{result.operation}\n")

        text.append("Arguments:   ", style="bold")
        text.append(f"{result.args}\n")

        text.append("Duration:    ", style="bold")
        text.append(f"{result.duration_seconds:.2f}s\n")

        text.append("\n")

        # Error information
        if result.error_type:
            text.append("Error:       ", style="bold red")
            text.append(f"{result.error_type}\n", style="red")

        if result.error_message:
            text.append("Message:     ", style="bold red")
            text.append(f"{result.error_message}\n", style="red")

        # State information
        text.append("\n")
        text.append("State:\n", style="bold")

        text.append("  Downloaded files: ", style="dim")
        if result.downloaded_files:
            text.append(f"{len(result.downloaded_files)} files\n")
            for f in result.downloaded_files[:5]:  # Show first 5
                text.append(f"    - {f}\n", style="dim")
            if len(result.downloaded_files) > 5:
                text.append(
                    f"    ... and {len(result.downloaded_files) - 5} more\n",
                    style="dim",
                )
        else:
            text.append("(none)\n", style="dim")

        text.append("  Processed files:  ", style="dim")
        if result.processed_files:
            text.append(f"{len(result.processed_files)} files\n")
        else:
            text.append("(none)\n", style="dim")

        # Traceback
        if result.error_traceback:
            text.append("\n")
            text.append("Traceback:\n", style="bold")
            text.append(result.error_traceback, style="dim")

        return text

    def _print_warning_detail(self, index: int, result: TaskResult) -> None:
        """Print detailed information about a warning result."""
        header = f"[{index}] WARNING {result.operation} {result.args_summary}"

        content = Text()
        for warning in result.warnings:
            content.append(f"⚠ {warning}\n", style="yellow")

        self.console.print(
            Panel(
                content,
                title=header,
                title_align="left",
                border_style="yellow",
                padding=(0, 1),
            )
        )

    def _print_summary(self) -> None:
        """Print the summary line."""
        passed = sum(1 for r in self.results if r.status == TaskStatus.PASSED)
        failed = sum(1 for r in self.results if r.status == TaskStatus.FAILED)
        errors = sum(1 for r in self.results if r.status == TaskStatus.ERROR)
        skipped = sum(1 for r in self.results if r.status == TaskStatus.SKIPPED)
        duplicated = sum(1 for r in self.results if r.status == TaskStatus.DUPLICATED)
        invalid = sum(1 for r in self.results if r.status == TaskStatus.INVALID)
        corrupted = sum(1 for r in self.results if r.status == TaskStatus.CORRUPTED)
        warnings_count = sum(
            1 for r in self.results if r.status == TaskStatus.WARNING or r.warnings
        )

        elapsed = 0.0
        if self._start_time and self._end_time:
            elapsed = (self._end_time - self._start_time).total_seconds()

        self.console.print()
        self.console.rule("[bold]SUMMARY[/bold]")

        # Build summary parts
        parts = []
        if passed:
            parts.append(f"[green]{passed} passed[/green]")
        if failed:
            parts.append(f"[red]{failed} failed[/red]")
        if errors:
            parts.append(f"[red bold]{errors} error[/red bold]")
        if skipped:
            parts.append(f"[yellow]{skipped} skipped[/yellow]")
        if duplicated:
            parts.append(f"[cyan]{duplicated} duplicated[/cyan]")
        if invalid:
            parts.append(f"[magenta]{invalid} invalid[/magenta]")
        if corrupted:
            parts.append(f"[yellow]{corrupted} corrupted[/yellow]")
        if warnings_count:
            parts.append(f"[yellow]{warnings_count} warning[/yellow]")

        # Format elapsed time
        if elapsed >= 60:
            minutes = int(elapsed // 60)
            seconds = elapsed % 60
            time_str = f"{minutes}m {seconds:.1f}s"
        else:
            time_str = f"{elapsed:.1f}s"

        summary = (
            f"{self.template_name} {self.operation}: {', '.join(parts)} in {time_str}"
        )
        self.console.print(summary)
        self.console.print()

    def save_report(self, filepath: str | Path, format: str = "json") -> None:
        """Save the report to a file.

        Args:
            filepath: Path to save the report.
            format: Format of the report ('json' or 'txt').
        """
        filepath = Path(filepath)

        if format == "json":
            self._save_json_report(filepath)
        else:
            self._save_text_report(filepath)

    def _save_json_report(self, filepath: Path) -> None:
        """Save report as JSON."""
        elapsed = 0.0
        if self._start_time and self._end_time:
            elapsed = (self._end_time - self._start_time).total_seconds()

        report = {
            "template_name": self.template_name,
            "operation": self.operation,
            "start_time": self._start_time.isoformat() if self._start_time else None,
            "end_time": self._end_time.isoformat() if self._end_time else None,
            "elapsed_seconds": elapsed,
            "summary": {
                "total": len(self.results),
                "passed": sum(1 for r in self.results if r.status == TaskStatus.PASSED),
                "failed": sum(1 for r in self.results if r.status == TaskStatus.FAILED),
                "errors": sum(1 for r in self.results if r.status == TaskStatus.ERROR),
                "skipped": sum(
                    1 for r in self.results if r.status == TaskStatus.SKIPPED
                ),
                "duplicated": sum(
                    1 for r in self.results if r.status == TaskStatus.DUPLICATED
                ),
                "invalid": sum(
                    1 for r in self.results if r.status == TaskStatus.INVALID
                ),
                "corrupted": sum(
                    1 for r in self.results if r.status == TaskStatus.CORRUPTED
                ),
                "warnings": sum(
                    1
                    for r in self.results
                    if r.status == TaskStatus.WARNING or r.warnings
                ),
            },
            "results": [r.to_dict() for r in self.results],
        }

        with filepath.open("w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)

    def _save_text_report(self, filepath: Path) -> None:
        """Save report as plain text."""
        console = Console(
            file=filepath.open("w", encoding="utf-8"), force_terminal=False
        )

        # Re-print the report to file
        failures = [
            r
            for r in self.results
            if r.status
            in (
                TaskStatus.FAILED,
                TaskStatus.INVALID,
                TaskStatus.CORRUPTED,
            )
        ]
        errors = [r for r in self.results if r.status == TaskStatus.ERROR]
        warnings_results = [
            r for r in self.results if r.status == TaskStatus.WARNING or r.warnings
        ]

        console.print(f"Report for {self.template_name} {self.operation}")
        console.print(f"Generated at: {datetime.now().isoformat()}")
        console.print()

        if failures or errors:
            console.print("=" * 60)
            console.print("FAILURES / ERRORS")
            console.print("=" * 60)

            for idx, result in enumerate(failures + errors, 1):
                console.print(
                    f"\n[{idx}] {result.status.value.upper()} {result.args_summary}"
                )
                console.print(f"  Template: {result.template_name}")
                console.print(f"  Error: {result.error_type}: {result.error_message}")
                if result.error_traceback:
                    console.print(f"  Traceback:\n{result.error_traceback}")

        if warnings_results:
            console.print("\n" + "=" * 60)
            console.print("WARNINGS")
            console.print("=" * 60)

            for idx, result in enumerate(warnings_results, 1):
                console.print(f"\n[{idx}] {result.args_summary}")
                for warning in result.warnings:
                    console.print(f"  ⚠ {warning}")

        # Summary
        console.print("\n" + "=" * 60)
        console.print("SUMMARY")
        console.print("=" * 60)

        passed = sum(1 for r in self.results if r.status == TaskStatus.PASSED)
        failed = sum(1 for r in self.results if r.status == TaskStatus.FAILED)
        error_count = sum(1 for r in self.results if r.status == TaskStatus.ERROR)
        skipped = sum(1 for r in self.results if r.status == TaskStatus.SKIPPED)
        duplicated_count = sum(
            1 for r in self.results if r.status == TaskStatus.DUPLICATED
        )
        invalid_count = sum(1 for r in self.results if r.status == TaskStatus.INVALID)
        corrupted_count = sum(
            1 for r in self.results if r.status == TaskStatus.CORRUPTED
        )
        warning_count = sum(
            1 for r in self.results if r.status == TaskStatus.WARNING or r.warnings
        )

        console.print(f"Total: {len(self.results)}")
        console.print(f"Passed: {passed}")
        console.print(f"Failed: {failed}")
        console.print(f"Errors: {error_count}")
        console.print(f"Skipped: {skipped}")
        console.print(f"Duplicated: {duplicated_count}")
        console.print(f"Invalid: {invalid_count}")
        console.print(f"Corrupted: {corrupted_count}")
        console.print(f"Warnings: {warning_count}")


@contextmanager
def capture_warnings():
    """Context manager to capture Python warnings.

    Yields:
        A list that will be populated with warning messages.
    """
    captured: list[str] = []
    original_showwarning = warnings.showwarning

    def capture_showwarning(
        message, category, filename, lineno, _file=None, _line=None
    ):
        captured.append(f"{category.__name__}: {message} ({filename}:{lineno})")

    warnings.showwarning = capture_showwarning
    try:
        yield captured
    finally:
        warnings.showwarning = original_showwarning


def create_task_result_from_exception(
    exception: Exception,
    operation: str,
    template_name: str,
    args: dict[str, Any],
    duration: float,
    downloaded_files: list[str] | None = None,
    processed_files: dict[str, str] | None = None,
    captured_warnings: list[str] | None = None,
    is_expected_error: bool = False,
) -> TaskResult:
    """Create a TaskResult from an exception.

    Args:
        exception: The exception that was raised.
        operation: Name of the operation.
        template_name: Name of the template.
        args: Arguments used for the operation.
        duration: Duration of the operation in seconds.
        downloaded_files: List of downloaded files.
        processed_files: Dict of processed files.
        captured_warnings: List of captured warnings.
        is_expected_error: If True, marks as FAILED; otherwise ERROR.

    Returns:
        A TaskResult capturing the exception details.
    """
    return TaskResult(
        status=TaskStatus.FAILED if is_expected_error else TaskStatus.ERROR,
        operation=operation,
        template_name=template_name,
        args=args,
        duration_seconds=duration,
        error_type=type(exception).__name__,
        error_message=str(exception),
        error_traceback=traceback.format_exc(),
        warnings=captured_warnings or [],
        downloaded_files=downloaded_files or [],
        processed_files=processed_files or {},
    )


def create_task_result_success(
    operation: str,
    template_name: str,
    args: dict[str, Any],
    duration: float,
    downloaded_files: list[str] | None = None,
    processed_files: dict[str, str] | None = None,
    captured_warnings: list[str] | None = None,
) -> TaskResult:
    """Create a successful TaskResult.

    Args:
        operation: Name of the operation.
        template_name: Name of the template.
        args: Arguments used for the operation.
        duration: Duration of the operation in seconds.
        downloaded_files: List of downloaded files.
        processed_files: Dict of processed files.
        captured_warnings: List of captured warnings.

    Returns:
        A TaskResult indicating success.
    """
    # If there are warnings, mark as WARNING status
    status = TaskStatus.WARNING if captured_warnings else TaskStatus.PASSED

    return TaskResult(
        status=status,
        operation=operation,
        template_name=template_name,
        args=args,
        duration_seconds=duration,
        warnings=captured_warnings or [],
        downloaded_files=downloaded_files or [],
        processed_files=processed_files or {},
    )


def create_task_result_skipped(
    operation: str,
    template_name: str,
    args: dict[str, Any],
    duration: float,
    downloaded_files: list[str] | None = None,
    processed_files: dict[str, str] | None = None,
) -> TaskResult:
    """Create a skipped TaskResult.

    Args:
        operation: Name of the operation.
        template_name: Name of the template.
        args: Arguments used for the operation.
        duration: Duration of the operation in seconds.
        downloaded_files: List of downloaded files.
        processed_files: Dict of processed files.

    Returns:
        A TaskResult indicating the task was skipped.
    """
    return TaskResult(
        status=TaskStatus.SKIPPED,
        operation=operation,
        template_name=template_name,
        args=args,
        duration_seconds=duration,
        downloaded_files=downloaded_files or [],
        processed_files=processed_files or {},
    )

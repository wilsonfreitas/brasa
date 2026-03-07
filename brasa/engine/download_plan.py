"""Download plan execution for batch market data downloads.

This module provides a declarative YAML-based approach to defining and
executing batch download operations across multiple templates.

Classes:
    DownloadPlanTask: A single template task within a download plan.
    DownloadPlanDefaults: Default arguments shared across all tasks.
    DownloadPlan: A complete download plan loaded from a YAML file.
    DownloadPlanReport: Aggregated report from executing a download plan.

Functions:
    execute_download_plan: Execute all tasks in a download plan.
    resolve_plan_args: Resolve dynamic argument values (symbols, date ranges).
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from .reporting import TaskReport, TaskStatus, Verbosity
from .template import list_templates, retrieve_template

logger = logging.getLogger(__name__)

_INT_RANGE_RE = re.compile(r"^\d+:\d+$")


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class DownloadPlanDefaults:
    """Default arguments applied to every task in the plan.

    Attributes:
        refdate: DateRangeParser string (e.g. "2026-01-01:", "2026").
        calendar: Business calendar name for date parsing (default: "B3").
        reprocess: Default reprocess flag (default: False).
    """

    refdate: str | None = None
    calendar: str = "B3"
    reprocess: bool = False


@dataclass
class DownloadPlanTask:
    """A single template download task.

    Attributes:
        template: Template name to download.
        args: Per-task arguments that override defaults.
        reprocess: Reprocess flag for this task.
    """

    template: str
    args: dict[str, Any] = field(default_factory=dict)
    reprocess: bool = False


@dataclass
class DownloadPlan:
    """A complete download plan loaded from YAML.

    Attributes:
        name: Human-readable plan name.
        description: Optional plan description.
        defaults: Default arguments merged into every task.
        tasks: Ordered list of tasks to execute.
    """

    name: str
    description: str
    defaults: DownloadPlanDefaults
    tasks: list[DownloadPlanTask]

    @classmethod
    def from_dict(cls, data: dict) -> DownloadPlan:
        """Build a DownloadPlan from a parsed YAML dictionary.

        Args:
            data: Parsed YAML content.

        Returns:
            A DownloadPlan instance.

        Raises:
            ValueError: If required fields are missing or invalid.
        """
        if "name" not in data:
            raise ValueError("Download plan must have a 'name' field")
        if "tasks" not in data or not data["tasks"]:
            raise ValueError("Download plan must have a non-empty 'tasks' list")

        defaults_data = data.get("defaults", {}) or {}
        defaults = DownloadPlanDefaults(
            refdate=defaults_data.get("refdate"),
            calendar=str(defaults_data.get("calendar", "B3")),
            reprocess=bool(defaults_data.get("reprocess", False)),
        )

        tasks = []
        for i, task_data in enumerate(data["tasks"]):
            if not isinstance(task_data, dict) or "template" not in task_data:
                raise ValueError(f"Task {i} must be a mapping with a 'template' key")
            task = DownloadPlanTask(
                template=str(task_data["template"]),
                args=dict(task_data.get("args", {}) or {}),
                reprocess=bool(task_data.get("reprocess", defaults.reprocess)),
            )
            tasks.append(task)

        return cls(
            name=str(data["name"]),
            description=str(data.get("description", "")),
            defaults=defaults,
            tasks=tasks,
        )

    @classmethod
    def from_file(cls, path: str | Path) -> DownloadPlan:
        """Load a DownloadPlan from a YAML file.

        Args:
            path: Path to the YAML plan file.

        Returns:
            A DownloadPlan instance.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the YAML is invalid or missing required fields.
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Download plan file not found: {path}")
        with path.open() as f:
            data = yaml.safe_load(f)
        if not isinstance(data, dict):
            raise ValueError(
                f"Invalid download plan YAML in {path}: expected a mapping"
            )
        return cls.from_dict(data)

    def validate(self) -> list[str]:
        """Validate that all template names in the plan exist.

        Returns:
            List of error messages (empty if plan is valid).
        """
        available = set(list_templates())
        errors = []
        for task in self.tasks:
            if task.template not in available:
                errors.append(f"Unknown template: '{task.template}'")
        return errors


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class DownloadPlanReport:
    """Aggregated report from executing a download plan.

    Attributes:
        plan_name: Name of the download plan.
        task_reports: Mapping of template name to TaskReport.
    """

    plan_name: str
    task_reports: dict[str, TaskReport] = field(default_factory=dict)
    implicit_task_reports: dict[str, TaskReport] = field(default_factory=dict)
    _start_time: datetime | None = field(default=None, repr=False)
    _end_time: datetime | None = field(default=None, repr=False)

    @property
    def total_duration(self) -> float:
        """Total wall-clock duration in seconds."""
        if self._start_time and self._end_time:
            return (self._end_time - self._start_time).total_seconds()
        return 0.0

    @property
    def success(self) -> bool:
        """True if no task produced an ERROR or FAILED result."""
        for report in {**self.task_reports, **self.implicit_task_reports}.values():
            for result in report.results:
                if result.status in (TaskStatus.ERROR, TaskStatus.FAILED):
                    return False
        return True

    @staticmethod
    def _format_elapsed(elapsed: float) -> str:
        if elapsed >= 60:
            minutes = int(elapsed // 60)
            seconds = elapsed % 60
            return f"{minutes}m {seconds:.1f}s"
        return f"{elapsed:.1f}s"

    @staticmethod
    def _report_status_str(report: TaskReport, include_duplicated: bool = False) -> str:
        passed = sum(1 for r in report.results if r.status == TaskStatus.PASSED)
        failed = sum(
            1
            for r in report.results
            if r.status in (TaskStatus.FAILED, TaskStatus.ERROR)
        )
        skipped = sum(1 for r in report.results if r.status == TaskStatus.SKIPPED)
        duplicated = sum(1 for r in report.results if r.status == TaskStatus.DUPLICATED)
        parts = []
        if passed:
            parts.append(f"{passed} passed")
        if failed:
            parts.append(f"{failed} failed")
        if skipped:
            parts.append(f"{skipped} skipped")
        if include_duplicated and duplicated:
            parts.append(f"{duplicated} duplicated")
        return ", ".join(parts) if parts else "no results"

    def _implicit_summary_lines(self) -> list[str]:
        lines = ["", "  [auto] Dependencies executed:"]
        for template, report in self.implicit_task_reports.items():
            status_str = self._report_status_str(report)
            lines.append(f"    {template:<38}  {status_str}")
        return lines

    def summary(self) -> str:
        """Return a human-readable multi-line plan summary.

        Returns:
            Formatted string with per-template counts and overall totals.
        """
        time_str = self._format_elapsed(self.total_duration)
        sep = "═" * 60
        lines = ["", sep, f" PLAN SUMMARY — {self.plan_name}", sep]

        ok_count = 0
        fail_count = 0
        for template, report in self.task_reports.items():
            status_str = self._report_status_str(report, include_duplicated=True)
            failed_count = sum(
                1
                for r in report.results
                if r.status in (TaskStatus.FAILED, TaskStatus.ERROR)
            )
            lines.append(f"  {template:<40}  {status_str}")
            if failed_count:
                fail_count += 1
            else:
                ok_count += 1

        if self.implicit_task_reports:
            lines.extend(self._implicit_summary_lines())

        lines.append("")
        n = len(self.task_reports)
        n_auto = len(self.implicit_task_reports)
        auto_str = f", {n_auto} auto" if n_auto else ""
        lines.append(
            f"Download plan '{self.plan_name}': {n} tasks{auto_str} in {time_str}"
        )
        if fail_count:
            lines.append(
                f"Overall: {ok_count} templates ok, {fail_count} with failures"
            )
        else:
            lines.append(f"Overall: all {ok_count} templates ok")
        return "\n".join(lines)

    def save_report(self, filepath: str | Path, format: str = "json") -> None:
        """Save the plan report to a file.

        Args:
            filepath: Path to save the report.
            format: 'json' or 'txt'.
        """
        filepath = Path(filepath)
        if format == "json":
            data = {
                "plan_name": self.plan_name,
                "total_duration": self.total_duration,
                "success": self.success,
                "tasks": [
                    {
                        "template": template,
                        "results": [r.to_dict() for r in report.results],
                    }
                    for template, report in self.task_reports.items()
                ],
                "implicit_tasks": [
                    {
                        "template": template,
                        "results": [r.to_dict() for r in report.results],
                    }
                    for template, report in self.implicit_task_reports.items()
                ],
            }
            filepath.write_text(json.dumps(data, indent=2, default=str))
        else:
            filepath.write_text(self.summary())


# ---------------------------------------------------------------------------
# Argument resolution
# ---------------------------------------------------------------------------


def resolve_plan_args(args: dict) -> dict:
    """Resolve dynamic argument values in a task's args dict.

    Resolves:
    - ``"symbols:<type>"`` strings via ``get_symbols(type)``
    - ``"<int>:<int>"`` strings (e.g. ``"2020:2026"``) to integer lists
    - All other values are passed through unchanged

    Note: ``refdate`` keys are handled separately in ``execute_download_plan``
    and should not be included in *args* when calling this function.

    Args:
        args: Raw argument dict (without refdate).

    Returns:
        Resolved argument dict.
    """
    resolved = {}
    for key, value in args.items():
        if not isinstance(value, str):
            resolved[key] = value
            continue

        # symbols:<type>
        if value.startswith("symbols:"):
            from brasa.queries import get_symbols

            symbol_type = value[len("symbols:") :]
            resolved[key] = get_symbols(symbol_type)
            continue

        # <int>:<int> — integer range
        if _INT_RANGE_RE.match(value):
            parts = value.split(":")
            start_int, end_int = int(parts[0]), int(parts[1])
            resolved[key] = list(range(start_int, end_int + 1))
            continue

        resolved[key] = value

    return resolved


def _template_requires_refdate(template_name: str) -> bool:
    """Return True if a template's downloader expects a refdate argument.

    Args:
        template_name: Template name to inspect.

    Returns:
        True when ``refdate`` appears in the template's downloader args.
    """
    try:
        template = retrieve_template(template_name)
        return "refdate" in template.downloader.args
    except Exception:
        return False


def _parse_refdate(refdate_str: str, calendar: str) -> Any:
    """Parse a refdate string via DateRangeParser.

    Normalises the special token ``today`` to an open-ended range
    (``"2026-01-01:today"`` → ``"2026-01-01:"``).

    Args:
        refdate_str: Raw string from the plan YAML.
        calendar: Business calendar name.

    Returns:
        A DateRange or list of datetimes as returned by DateRangeParser.
    """
    from brasa.util import DateRangeParser

    normalized = refdate_str.replace(":today", ":")
    return DateRangeParser(calendar).parse(normalized)


# ---------------------------------------------------------------------------
# Execution helpers
# ---------------------------------------------------------------------------


def _resolve_task_refdate(
    merged_args: dict,
    refdate_override: Any | None,
    calendar: str,
) -> Any:
    """Determine the effective refdate for a single task.

    Priority: *refdate_override* > task/defaults ``refdate`` > ``None``.

    Args:
        merged_args: Merged defaults + task args (may contain ``"refdate"``).
        refdate_override: CLI-level override (highest priority).
        calendar: Business calendar name for parsing refdate strings.

    Returns:
        Resolved refdate value, or ``None`` if not applicable.
    """
    if refdate_override is not None:
        return refdate_override
    if "refdate" not in merged_args:
        return None
    refdate_val = merged_args["refdate"]
    if not isinstance(refdate_val, str):
        return refdate_val
    try:
        return _parse_refdate(refdate_val, calendar)
    except Exception:
        return refdate_val


def _execute_task(
    task: DownloadPlanTask,
    resolved_args: dict,
    verbosity: Verbosity,
) -> TaskReport:
    """Run a single plan task and return its TaskReport.

    Catches all exceptions so callers can continue with remaining tasks.

    Args:
        task: The plan task to execute.
        resolved_args: Fully resolved keyword arguments (may include refdate).
        verbosity: Output verbosity level.

    Returns:
        A TaskReport (may contain an ERROR result on unexpected exceptions).
    """
    from .api import download_marketdata
    from .reporting import create_task_result_from_exception

    try:
        return download_marketdata(
            task.template,
            reprocess=task.reprocess,
            verbosity=verbosity,
            **resolved_args,
        )
    except Exception as exc:
        logger.error(
            "Unexpected error executing task '%s': %s",
            task.template,
            exc,
            exc_info=True,
        )
        report = TaskReport(
            operation="download",
            template_name=task.template,
            verbosity=verbosity,
        )
        report.start(total=1)
        result = create_task_result_from_exception(
            exception=exc,
            operation="download",
            template_name=task.template,
            args={},
            duration=0.0,
        )
        report.add_result(result)
        report.finish()
        return report


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------


def execute_download_plan(
    plan: DownloadPlan,
    refdate_override: Any | None = None,
    verbosity: Verbosity = Verbosity.NORMAL,
    report_file: str | Path | None = None,
) -> DownloadPlanReport:
    """Execute all tasks in a download plan.

    For each task the following steps are applied:

    1. Merge ``defaults`` into task args (task args win on conflict).
    2. Resolve refdate with priority: *refdate_override* > task ``args.refdate``
       > ``defaults.refdate``.
    3. Resolve ``symbols:`` prefixed args and integer ranges.
    4. Smart-inject ``refdate`` only into templates that declare it.
    5. Call ``download_marketdata()`` and collect the ``TaskReport``.

    Tasks are independent — one failure does not abort the plan.

    Args:
        plan: The DownloadPlan to execute.
        refdate_override: CLI ``--date`` value (highest priority).
        verbosity: Output verbosity level.
        report_file: Optional path to save the aggregate plan report.

    Returns:
        DownloadPlanReport with results from all tasks.
    """
    if verbosity != Verbosity.QUIET:
        from rich.console import Console

        Console(stderr=True).print(
            "Status legend: .(passed) F(failed) E(error) "
            "S(skipped) D(duplicated) I(invalid) C(corrupted)"
        )

    plan_report = DownloadPlanReport(plan_name=plan.name)
    plan_report._start_time = datetime.now()

    for task in plan.tasks:
        # 1. Build merged args: defaults.refdate as base, then per-task args
        merged_args: dict[str, Any] = {}
        if plan.defaults.refdate is not None:
            merged_args["refdate"] = plan.defaults.refdate
        merged_args.update(task.args)

        # 2. Resolve refdate with priority ordering
        refdate = _resolve_task_refdate(
            merged_args, refdate_override, plan.defaults.calendar
        )

        # 3. Resolve remaining args (symbols, integer ranges), excluding refdate
        non_refdate = {k: v for k, v in merged_args.items() if k != "refdate"}
        resolved_args = resolve_plan_args(non_refdate)

        # 4. Smart injection: only pass refdate if the template actually wants it
        if refdate is not None and _template_requires_refdate(task.template):
            resolved_args["refdate"] = refdate

        # 5. Execute — continue on any error
        plan_report.task_reports[task.template] = _execute_task(
            task, resolved_args, verbosity
        )
        # Collect dependency reports from the task
        task_report = plan_report.task_reports[task.template]
        for dep_report in getattr(task_report, "dependency_reports", []):
            name = dep_report.template_name
            if name not in plan_report.implicit_task_reports:
                plan_report.implicit_task_reports[name] = dep_report

    plan_report._end_time = datetime.now()

    if verbosity != Verbosity.QUIET:
        from rich.console import Console

        Console(stderr=True).print(plan_report.summary())

    if report_file:
        filepath = Path(report_file)
        fmt = "txt" if filepath.suffix == ".txt" else "json"
        plan_report.save_report(filepath, format=fmt)

    return plan_report

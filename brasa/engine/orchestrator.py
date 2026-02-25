"""Pipeline orchestrator for dependency-aware template execution.

This module provides the :class:`PipelineOrchestrator` which uses the
:class:`~brasa.engine.dependency_graph.TemplateDependencyGraph` to
automatically resolve and execute upstream dependencies before running
a target template.

Classes:
    OrchestratorReport: Aggregated report from a multi-step execution.
    PipelineOrchestrator: Executes templates in topological order,
        respecting dependencies and staleness.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from .dependency_graph import ExecutionPlan, ExecutionStep, TemplateDependencyGraph
from .reporting import (
    TaskReport,
    TaskStatus,
    Verbosity,
    create_task_result_skipped,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# OrchestratorReport
# ---------------------------------------------------------------------------


@dataclass
class OrchestratorReport:
    """Aggregated report from a multi-step orchestrated execution.

    Attributes:
        target_template: The template that was requested.
        plan: The execution plan that was followed.
        step_reports: Mapping of template_id to the ``TaskReport``
            produced by executing that step.
        dry_run: Whether this was a dry-run (no actual execution).
    """

    target_template: str
    plan: ExecutionPlan
    step_reports: dict[str, TaskReport] = field(default_factory=dict)
    dry_run: bool = False
    _start_time: datetime | None = field(default=None, repr=False)
    _end_time: datetime | None = field(default=None, repr=False)

    @property
    def steps_executed(self) -> int:
        """Number of steps that were actually executed."""
        return len(
            [
                s
                for s in self.plan.steps
                if s.action != "skip" and s.template_id in self.step_reports
            ]
        )

    @property
    def steps_skipped(self) -> int:
        """Number of steps that were skipped."""
        return len(self.plan.steps_to_skip)

    @property
    def total_duration(self) -> float:
        """Total wall-clock duration of the orchestration in seconds."""
        if self._start_time and self._end_time:
            return (self._end_time - self._start_time).total_seconds()
        return 0.0

    @property
    def all_reports(self) -> list[TaskReport]:
        """All TaskReport instances in execution order."""
        result: list[TaskReport] = []
        for step in self.plan.steps:
            if step.template_id in self.step_reports:
                result.append(self.step_reports[step.template_id])
        return result

    @property
    def success(self) -> bool:
        """Whether all executed steps completed successfully.

        Returns ``True`` if no step produced an ERROR or FAILED result.
        Also returns ``True`` for dry runs (nothing executed).
        """
        if self.dry_run:
            return True
        for report in self.all_reports:
            for result in report.results:
                if result.status in (TaskStatus.ERROR, TaskStatus.FAILED):
                    return False
        return True

    def summary(self) -> str:
        """Return a human-readable summary of the orchestration.

        Returns:
            Multi-line string describing what was executed.
        """
        lines = [f"Orchestrator report for '{self.target_template}':"]

        if self.dry_run:
            lines.append("  Mode: DRY RUN (no steps executed)")
        else:
            lines.append("  Mode: EXECUTE")

        lines.append(
            f"  Steps: {self.steps_executed} executed, "
            f"{self.steps_skipped} skipped, "
            f"{len(self.plan.steps)} total"
        )

        if not self.dry_run:
            lines.append(f"  Duration: {self.total_duration:.1f}s")
            lines.append(f"  Success: {self.success}")

        for step in self.plan.steps:
            marker = "SKIP" if step.action == "skip" else step.action.upper()
            report = self.step_reports.get(step.template_id)
            if report and report.results:
                statuses = [r.status.value for r in report.results]
                status_str = ", ".join(statuses)
            elif step.action == "skip":
                status_str = "skipped"
            else:
                status_str = "planned" if self.dry_run else "not executed"
            lines.append(
                f"  [{marker}] {step.template_id} ({step.template_type}) "
                f"— {step.reason} [{status_str}]"
            )

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# PipelineOrchestrator
# ---------------------------------------------------------------------------


class PipelineOrchestrator:
    """Executes templates in dependency order using the graph.

    The orchestrator builds an :class:`ExecutionPlan` from the
    :class:`TemplateDependencyGraph`, then dispatches each step to the
    appropriate API function (``process_marketdata`` for download
    templates, ``process_etl`` for ETL templates).

    Args:
        graph: An optional pre-built dependency graph.  If ``None``,
            a new graph is constructed on first use.
    """

    def __init__(
        self,
        graph: TemplateDependencyGraph | None = None,
    ) -> None:
        self._graph = graph

    @property
    def graph(self) -> TemplateDependencyGraph:
        """The dependency graph used by this orchestrator.

        Lazily constructed on first access if not provided at init.
        """
        if self._graph is None:
            self._graph = TemplateDependencyGraph()
        return self._graph

    def execute(
        self,
        template_id: str,
        force: bool = False,
        dry_run: bool = False,
        verbosity: Verbosity = Verbosity.NORMAL,
    ) -> OrchestratorReport:
        """Execute a template with automatic dependency resolution.

        Builds an execution plan, then executes each non-skipped step
        in topological order.

        Args:
            template_id: The target template to process.
            force: If ``True``, re-execute all ancestors regardless
                of staleness.
            dry_run: If ``True``, build the plan but do not execute
                any steps.
            verbosity: Output verbosity level.

        Returns:
            An ``OrchestratorReport`` with results from all steps.

        Raises:
            KeyError: If *template_id* is not in the dependency graph.
        """
        plan = self.graph.get_execution_plan(template_id, force=force)

        report = OrchestratorReport(
            target_template=template_id,
            plan=plan,
            dry_run=dry_run,
        )
        report._start_time = datetime.now()

        if dry_run:
            logger.info("Dry run for '%s': %s", template_id, plan)
            report._end_time = datetime.now()
            return report

        logger.info(
            "Executing plan for '%s': %d steps, %d to execute",
            template_id,
            len(plan.steps),
            len(plan.steps_to_execute),
        )

        for step in plan.steps:
            if step.action == "skip":
                logger.debug("Skipping '%s': %s", step.template_id, step.reason)
                continue

            step_report = self._execute_step(step, verbosity)
            report.step_reports[step.template_id] = step_report

            # Check for failures — stop execution on error
            has_error = any(
                r.status in (TaskStatus.ERROR, TaskStatus.FAILED)
                for r in step_report.results
            )
            if has_error:
                logger.error(
                    "Step '%s' failed, aborting orchestration",
                    step.template_id,
                )
                break

        report._end_time = datetime.now()

        logger.info(
            "Orchestration complete for '%s': %s",
            template_id,
            "SUCCESS" if report.success else "FAILED",
        )

        return report

    def _execute_step(
        self,
        step: ExecutionStep,
        verbosity: Verbosity,
    ) -> TaskReport:
        """Execute a single step in the plan.

        Dispatches to ``process_marketdata`` for download templates
        or ``process_etl`` for ETL templates.

        Args:
            step: The execution step to run.
            verbosity: Output verbosity level.

        Returns:
            A ``TaskReport`` from the executed operation.
        """
        # Import here to avoid circular imports
        from .api import process_etl, process_marketdata

        if step.action == "process" and step.template_type == "download":
            logger.info(
                "Processing download template '%s': %s",
                step.template_id,
                step.reason,
            )
            return process_marketdata(
                step.template_id,
                verbosity=verbosity,
            )

        if step.action == "etl" and step.template_type == "etl":
            logger.info(
                "Running ETL template '%s': %s",
                step.template_id,
                step.reason,
            )
            return process_etl(
                step.template_id,
                verbosity=verbosity,
            )

        # Fallback — should not happen with a well-formed plan
        logger.warning(
            "Unexpected step action='%s' type='%s' for '%s', skipping",
            step.action,
            step.template_type,
            step.template_id,
        )
        fallback_report = TaskReport(
            operation=step.action,
            template_name=step.template_id,
            verbosity=verbosity,
        )
        fallback_report.start(total=1)
        result = create_task_result_skipped(
            operation=step.action,
            template_name=step.template_id,
            args={},
            duration=0.0,
        )
        fallback_report.add_result(result)
        fallback_report.finish()
        return fallback_report

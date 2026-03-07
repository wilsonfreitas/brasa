"""Integration tests for PipelineOrchestrator.

Tests cover:
- Full orchestration flow with mocked API functions
- Dry-run mode returns planned steps without execution
- Force mode includes all ancestors for execution
- Backward compatibility of process_etl without resolve_dependencies
- Error handling when upstream steps fail
- OrchestratorReport properties and summary
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from brasa.engine.dependency_graph import (
    ExecutionPlan,
    ExecutionStep,
    TemplateDependencyGraph,
)
from brasa.engine.orchestrator import OrchestratorReport, PipelineOrchestrator
from brasa.engine.reporting import (
    TaskReport,
    TaskResult,
    TaskStatus,
    Verbosity,
)
from brasa.engine.template import (
    MarketDataETL,
    MarketDataReader,
    MarketDataTemplate,
    MarketDataWriter,
)

# ---------------------------------------------------------------------------
# Helpers (reuse patterns from test_dependency_graph.py)
# ---------------------------------------------------------------------------


def _make_writer(
    layer: str = "input",
    dataset: str = "",
    template_id: str = "",
) -> MarketDataWriter:
    cfg: dict = {"layer": layer, "partitioning": []}
    if dataset:
        cfg["dataset"] = dataset
    return MarketDataWriter(cfg, template_id=template_id)


def _make_download_template(
    template_id: str,
    *,
    datasets: dict | None = None,
    writer_layer: str = "input",
    has_pipeline: bool = True,
) -> MarketDataTemplate:
    tmpl = MagicMock(spec=MarketDataTemplate)
    tmpl.id = template_id
    tmpl.has_reader = True
    tmpl.is_etl = False
    tmpl.writer = _make_writer(
        layer=writer_layer, dataset=template_id, template_id=template_id
    )
    reader = MagicMock(spec=MarketDataReader)
    reader.has_pipeline = has_pipeline
    tmpl.reader = reader

    if datasets is not None:
        from brasa.engine.template import DatasetConfig
        from brasa.fieldsets import Fieldset

        tmpl.datasets = {
            name: DatasetConfig(name=name, tag=tag, fields=Fieldset())
            for name, tag in datasets.items()
        }
    else:
        tmpl.datasets = None

    return tmpl


def _make_etl_template(
    template_id: str,
    input_datasets: list[str],
    *,
    writer_layer: str = "staging",
    writer_dataset: str = "",
    is_pipeline: bool = True,
) -> MarketDataTemplate:
    tmpl = MagicMock(spec=MarketDataTemplate)
    tmpl.id = template_id
    tmpl.has_reader = False
    tmpl.is_etl = True
    tmpl.datasets = None
    tmpl.writer = _make_writer(
        layer=writer_layer,
        dataset=writer_dataset or template_id,
        template_id=template_id,
    )
    etl = MagicMock(spec=MarketDataETL)
    etl.is_pipeline = is_pipeline
    etl.get_input_datasets.return_value = list(input_datasets)
    tmpl.etl = etl
    return tmpl


def _build_graph_from_templates(
    templates: list[MarketDataTemplate],
) -> TemplateDependencyGraph:
    name_map = {t.id: t for t in templates}
    with (
        patch(
            "brasa.engine.dependency_graph.list_templates",
            return_value=list(name_map.keys()),
        ),
        patch(
            "brasa.engine.dependency_graph.retrieve_template",
            side_effect=lambda n: name_map[n],
        ),
    ):
        return TemplateDependencyGraph()


def _make_success_report(template_name: str, operation: str = "process") -> TaskReport:
    """Create a TaskReport with a single successful result."""
    report = TaskReport(
        operation=operation,
        template_name=template_name,
        verbosity=Verbosity.QUIET,
    )
    report.start(total=1)
    result = TaskResult(
        status=TaskStatus.PASSED,
        operation=operation,
        template_name=template_name,
        args={},
        duration_seconds=0.1,
    )
    report.add_result(result)
    report.finish()
    return report


def _make_error_report(template_name: str, operation: str = "process") -> TaskReport:
    """Create a TaskReport with a single error result."""
    report = TaskReport(
        operation=operation,
        template_name=template_name,
        verbosity=Verbosity.QUIET,
    )
    report.start(total=1)
    result = TaskResult(
        status=TaskStatus.ERROR,
        operation=operation,
        template_name=template_name,
        args={},
        duration_seconds=0.1,
        error_type="RuntimeError",
        error_message="Test error",
    )
    report.add_result(result)
    report.finish()
    return report


# ===================================================================
# TEST-015: Orchestrator dry run
# ===================================================================


class TestOrchestratorDryRun:
    """Verify dry run returns plan without executing anything."""

    def test_dry_run_no_execution(self):
        """dry_run=True should not call process_marketdata or process_etl."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=True
            ),
            patch.object(graph, "_check_etl_template_staleness", return_value=True),
            patch("brasa.engine.api.process_marketdata") as mock_pm,
            patch("brasa.engine.api.process_etl") as mock_pe,
        ):
            report = orchestrator.execute(
                "my-etl", dry_run=True, verbosity=Verbosity.QUIET
            )

        mock_pm.assert_not_called()
        mock_pe.assert_not_called()
        assert report.dry_run is True
        assert report.success is True
        assert report.steps_executed == 0
        assert len(report.step_reports) == 0

    def test_dry_run_has_plan(self):
        """dry_run should still populate the execution plan."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=True
            ),
            patch.object(graph, "_check_etl_template_staleness", return_value=True),
        ):
            report = orchestrator.execute(
                "my-etl", dry_run=True, verbosity=Verbosity.QUIET
            )

        assert report.plan is not None
        assert len(report.plan.steps) == 2
        assert report.plan.steps[0].template_id == "dl-src"
        assert report.plan.steps[1].template_id == "my-etl"


# ===================================================================
# TEST-016: Orchestrator execute — correct order
# ===================================================================


class TestOrchestratorExecute:
    """Verify process_marketdata and process_etl are called in order."""

    def test_calls_in_correct_order(self):
        """For dl-src → my-etl, should call process_marketdata first, then process_etl."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)

        call_order = []

        def mock_process_marketdata(template_name, **kwargs):
            call_order.append(("process_marketdata", template_name))
            return _make_success_report(template_name, "process")

        def mock_process_etl(template_name, **kwargs):
            call_order.append(("process_etl", template_name))
            return _make_success_report(template_name, "etl")

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=True
            ),
            patch.object(graph, "_check_etl_template_staleness", return_value=True),
            patch(
                "brasa.engine.api.process_marketdata",
                side_effect=mock_process_marketdata,
            ),
            patch(
                "brasa.engine.api.process_etl",
                side_effect=mock_process_etl,
            ),
        ):
            report = orchestrator.execute("my-etl", verbosity=Verbosity.QUIET)

        assert call_order == [
            ("process_marketdata", "dl-src"),
            ("process_etl", "my-etl"),
        ]
        assert report.success is True
        assert report.steps_executed == 2

    def test_chain_three_levels(self):
        """dl-src → etl-mid → etl-end: all three should execute in order."""
        src = _make_download_template("dl-src")
        mid = _make_etl_template("etl-mid", input_datasets=["dl-src"])
        end = _make_etl_template("etl-end", input_datasets=["staging.etl-mid"])
        graph = _build_graph_from_templates([src, mid, end])

        orchestrator = PipelineOrchestrator(graph=graph)
        call_order = []

        def mock_process_marketdata(template_name, **kwargs):
            call_order.append(("process_marketdata", template_name))
            return _make_success_report(template_name, "process")

        def mock_process_etl(template_name, **kwargs):
            call_order.append(("process_etl", template_name))
            return _make_success_report(template_name, "etl")

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=True
            ),
            patch.object(graph, "_check_etl_template_staleness", return_value=True),
            patch(
                "brasa.engine.api.process_marketdata",
                side_effect=mock_process_marketdata,
            ),
            patch(
                "brasa.engine.api.process_etl",
                side_effect=mock_process_etl,
            ),
        ):
            report = orchestrator.execute(
                "etl-end", force=True, verbosity=Verbosity.QUIET
            )

        assert call_order == [
            ("process_marketdata", "dl-src"),
            ("process_etl", "etl-mid"),
            ("process_etl", "etl-end"),
        ]
        assert report.success is True
        assert report.steps_executed == 3
        assert report.steps_skipped == 0

    def test_skips_fresh_upstreams(self):
        """Fresh upstreams should be skipped, only stale ones executed."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)

        def mock_process_etl(template_name, **kwargs):
            return _make_success_report(template_name, "etl")

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=False
            ),
            patch.object(graph, "_check_etl_template_staleness", return_value=True),
            patch("brasa.engine.api.process_marketdata") as mock_pm,
            patch(
                "brasa.engine.api.process_etl",
                side_effect=mock_process_etl,
            ),
        ):
            report = orchestrator.execute("my-etl", verbosity=Verbosity.QUIET)

        mock_pm.assert_not_called()
        assert report.steps_executed == 1
        assert report.steps_skipped == 1
        assert report.success is True

    def test_stops_on_error(self):
        """If an upstream step fails, execution should stop."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)

        def mock_process_marketdata(template_name, **kwargs):
            return _make_error_report(template_name, "process")

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=True
            ),
            patch.object(graph, "_check_etl_template_staleness", return_value=True),
            patch(
                "brasa.engine.api.process_marketdata",
                side_effect=mock_process_marketdata,
            ),
            patch("brasa.engine.api.process_etl") as mock_pe,
        ):
            report = orchestrator.execute("my-etl", verbosity=Verbosity.QUIET)

        # process_etl should NOT be called because upstream failed
        mock_pe.assert_not_called()
        assert report.success is False
        assert report.steps_executed == 1


# ===================================================================
# TEST: Force mode via orchestrator
# ===================================================================


class TestOrchestratorForceMode:
    """Verify force=True marks all ancestors for execution."""

    def test_force_executes_all(self):
        """force=True should execute all steps even if fresh."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)
        call_order = []

        def mock_process_marketdata(template_name, **kwargs):
            call_order.append(("process_marketdata", template_name))
            return _make_success_report(template_name, "process")

        def mock_process_etl(template_name, **kwargs):
            call_order.append(("process_etl", template_name))
            return _make_success_report(template_name, "etl")

        with (
            patch(
                "brasa.engine.api.process_marketdata",
                side_effect=mock_process_marketdata,
            ),
            patch(
                "brasa.engine.api.process_etl",
                side_effect=mock_process_etl,
            ),
        ):
            report = orchestrator.execute(
                "my-etl", force=True, verbosity=Verbosity.QUIET
            )

        assert len(call_order) == 2
        assert call_order[0] == ("process_marketdata", "dl-src")
        assert call_order[1] == ("process_etl", "my-etl")
        assert report.steps_executed == 2
        assert report.steps_skipped == 0


# ===================================================================
# TEST-017: Backward compatibility — process_etl without resolve_dependencies
# ===================================================================


class TestBackwardCompatibility:
    """Verify process_etl works exactly as before without resolve_dependencies."""

    def test_default_no_orchestration(self):
        """process_etl with default args should not use orchestrator."""
        with (
            patch("brasa.engine.api.retrieve_template") as mock_retrieve,
            patch("brasa.engine.api.capture_warnings") as mock_warnings,
        ):
            mock_template = MagicMock()
            mock_template.id = "test-etl"
            mock_template.etl.is_pipeline = True
            mock_retrieve.return_value = mock_template
            mock_warnings.return_value.__enter__ = MagicMock(return_value=[])
            mock_warnings.return_value.__exit__ = MagicMock(return_value=False)

            from brasa.engine.api import process_etl

            report = process_etl("test-etl", verbosity=Verbosity.QUIET)

        assert isinstance(report, TaskReport)
        assert report.operation == "etl"
        assert report.template_name == "test-etl"


# ===================================================================
# TEST: OrchestratorReport properties
# ===================================================================


class TestOrchestratorReport:
    """Verify OrchestratorReport properties and summary."""

    def test_empty_report(self):
        plan = ExecutionPlan(target_template="tgt")
        report = OrchestratorReport(
            target_template="tgt",
            plan=plan,
        )
        assert report.steps_executed == 0
        assert report.steps_skipped == 0
        assert report.total_duration == 0.0
        assert report.all_reports == []
        assert report.success is True

    def test_summary_dry_run(self):
        plan = ExecutionPlan(
            target_template="my-etl",
            steps=[
                ExecutionStep("dl-src", "process", "stale", "download"),
                ExecutionStep("my-etl", "etl", "outdated", "etl"),
            ],
        )
        report = OrchestratorReport(
            target_template="my-etl",
            plan=plan,
            dry_run=True,
        )
        summary = report.summary()
        assert "DRY RUN" in summary
        assert "my-etl" in summary

    def test_summary_with_reports(self):
        plan = ExecutionPlan(
            target_template="my-etl",
            steps=[
                ExecutionStep("dl-src", "process", "stale", "download"),
                ExecutionStep("my-etl", "etl", "outdated", "etl"),
            ],
        )
        report = OrchestratorReport(
            target_template="my-etl",
            plan=plan,
        )
        report.step_reports["dl-src"] = _make_success_report("dl-src", "process")
        report.step_reports["my-etl"] = _make_success_report("my-etl", "etl")

        assert report.steps_executed == 2
        assert report.success is True
        assert len(report.all_reports) == 2

        summary = report.summary()
        assert "EXECUTE" in summary
        assert "2 executed" in summary

    def test_success_false_on_error(self):
        plan = ExecutionPlan(
            target_template="my-etl",
            steps=[
                ExecutionStep("dl-src", "process", "stale", "download"),
            ],
        )
        report = OrchestratorReport(
            target_template="my-etl",
            plan=plan,
        )
        report.step_reports["dl-src"] = _make_error_report("dl-src", "process")

        assert report.success is False

    def test_all_reports_preserves_order(self):
        """all_reports should follow the plan's step order."""
        plan = ExecutionPlan(
            target_template="c",
            steps=[
                ExecutionStep("a", "process", "stale", "download"),
                ExecutionStep("b", "etl", "outdated", "etl"),
                ExecutionStep("c", "etl", "outdated", "etl"),
            ],
        )
        report = OrchestratorReport(target_template="c", plan=plan)
        report.step_reports["a"] = _make_success_report("a", "process")
        report.step_reports["c"] = _make_success_report("c", "etl")
        report.step_reports["b"] = _make_success_report("b", "etl")

        all_reports = report.all_reports
        assert len(all_reports) == 3
        assert all_reports[0].template_name == "a"
        assert all_reports[1].template_name == "b"
        assert all_reports[2].template_name == "c"


# ===================================================================
# TEST: PipelineOrchestrator with lazy graph
# ===================================================================


class TestOrchestratorLazyGraph:
    """Verify the orchestrator builds the graph lazily."""

    def test_graph_created_on_access(self):
        """Graph should be created when first accessed."""
        orchestrator = PipelineOrchestrator()
        assert orchestrator._graph is None

        with (
            patch(
                "brasa.engine.dependency_graph.list_templates",
                return_value=[],
            ),
            patch(
                "brasa.engine.dependency_graph.retrieve_template",
                side_effect=lambda n: None,
            ),
        ):
            graph = orchestrator.graph

        assert graph is not None
        assert orchestrator._graph is graph

    def test_provided_graph_used(self):
        """If a graph is provided, it should be used directly."""
        src = _make_download_template("dl-src")
        graph = _build_graph_from_templates([src])

        orchestrator = PipelineOrchestrator(graph=graph)
        assert orchestrator.graph is graph


# ===================================================================
# TEST: process_etl with resolve_dependencies=True
# ===================================================================


class TestProcessEtlWithResolveDependencies:
    """Verify process_etl delegates to orchestrator when requested."""

    def test_resolve_dependencies_calls_orchestrator(self):
        """resolve_dependencies=True should use PipelineOrchestrator."""
        with patch(
            "brasa.engine.orchestrator.PipelineOrchestrator",
        ) as MockOrchestrator:
            # Set up the mock orchestrator
            mock_orch_instance = MagicMock()
            mock_orch_report = OrchestratorReport(
                target_template="my-etl",
                plan=ExecutionPlan(target_template="my-etl"),
            )
            target_report = _make_success_report("my-etl", "etl")
            mock_orch_report.step_reports["my-etl"] = target_report
            mock_orch_instance.execute.return_value = mock_orch_report
            MockOrchestrator.return_value = mock_orch_instance

            from brasa.engine.api import process_etl

            report = process_etl(
                "my-etl",
                resolve_dependencies=True,
                verbosity=Verbosity.QUIET,
            )

        MockOrchestrator.assert_called_once()
        mock_orch_instance.execute.assert_called_once_with(
            "my-etl", force=False, verbosity=Verbosity.QUIET
        )
        assert report is target_report

    def test_resolve_dependencies_with_force(self):
        """force=True should be passed through to orchestrator."""
        with patch(
            "brasa.engine.orchestrator.PipelineOrchestrator",
        ) as MockOrchestrator:
            mock_orch_instance = MagicMock()
            mock_orch_report = OrchestratorReport(
                target_template="my-etl",
                plan=ExecutionPlan(target_template="my-etl"),
            )
            target_report = _make_success_report("my-etl", "etl")
            mock_orch_report.step_reports["my-etl"] = target_report
            mock_orch_instance.execute.return_value = mock_orch_report
            MockOrchestrator.return_value = mock_orch_instance

            from brasa.engine.api import process_etl

            process_etl(
                "my-etl",
                resolve_dependencies=True,
                force=True,
                verbosity=Verbosity.QUIET,
            )

        mock_orch_instance.execute.assert_called_once_with(
            "my-etl", force=True, verbosity=Verbosity.QUIET
        )


# ===================================================================
# TEST: ETL staleness re-evaluation after upstream execution
# ===================================================================


class TestETLStalenessReEvaluation:
    """Regression test: ETL planned as 'skip' should be promoted to 'etl'
    if an upstream dependency was actually executed in the current run."""

    def test_etl_promoted_when_upstream_executed(self):
        """ETL step planned as 'skip' is re-evaluated and promoted to 'etl'
        if a direct upstream was executed in the same run."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)

        call_order = []

        def mock_process_marketdata(template_name, **kwargs):
            call_order.append(("process_marketdata", template_name))
            return _make_success_report(template_name, "process")

        def mock_process_etl(template_name, **kwargs):
            call_order.append(("process_etl", template_name))
            return _make_success_report(template_name, "etl")

        # At plan-build time, ETL appears fresh (staleness=False).
        # After upstream executes, re-evaluation returns True.
        staleness_call_count = 0

        def staleness_side_effect(template_id):
            nonlocal staleness_call_count
            staleness_call_count += 1
            # First call: plan-build time — ETL looks fresh;
            # second call: re-evaluation after upstream ran — ETL is stale
            return staleness_call_count != 1

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=True
            ),
            patch.object(
                graph,
                "_check_etl_template_staleness",
                side_effect=staleness_side_effect,
            ),
            patch(
                "brasa.engine.api.process_marketdata",
                side_effect=mock_process_marketdata,
            ),
            patch(
                "brasa.engine.api.process_etl",
                side_effect=mock_process_etl,
            ),
        ):
            report = orchestrator.execute("my-etl", verbosity=Verbosity.QUIET)

        # Both steps should have executed
        assert call_order == [
            ("process_marketdata", "dl-src"),
            ("process_etl", "my-etl"),
        ]
        assert report.success is True
        assert report.steps_executed == 2

    def test_etl_not_promoted_when_staleness_still_false(self):
        """ETL step remains skipped if re-evaluation still shows it fresh."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)

        def mock_process_marketdata(template_name, **kwargs):
            return _make_success_report(template_name, "process")

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=True
            ),
            # ETL is always fresh — even after upstream runs
            patch.object(graph, "_check_etl_template_staleness", return_value=False),
            patch(
                "brasa.engine.api.process_marketdata",
                side_effect=mock_process_marketdata,
            ),
            patch("brasa.engine.api.process_etl") as mock_pe,
        ):
            report = orchestrator.execute("my-etl", verbosity=Verbosity.QUIET)

        mock_pe.assert_not_called()
        assert report.steps_executed == 1
        assert report.steps_skipped == 1

    def test_etl_not_promoted_when_no_upstream_executed(self):
        """ETL step is not re-evaluated if no upstream was executed."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        orchestrator = PipelineOrchestrator(graph=graph)
        staleness_calls: list[str] = []

        def staleness_side_effect(template_id):
            staleness_calls.append(template_id)
            return False

        with (
            # Both upstream and ETL are fresh at plan-build time
            patch.object(
                graph, "_check_download_template_staleness", return_value=False
            ),
            patch.object(
                graph,
                "_check_etl_template_staleness",
                side_effect=staleness_side_effect,
            ),
            patch("brasa.engine.api.process_marketdata") as mock_pm,
            patch("brasa.engine.api.process_etl") as mock_pe,
        ):
            report = orchestrator.execute("my-etl", verbosity=Verbosity.QUIET)

        mock_pm.assert_not_called()
        mock_pe.assert_not_called()
        # Only one staleness call at plan-build time; no re-evaluation
        assert staleness_calls.count("my-etl") == 1
        assert report.steps_executed == 0
        assert report.steps_skipped == 2

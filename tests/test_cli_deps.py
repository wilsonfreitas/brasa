"""Tests for Phase 5 CLI commands and public API functions.

Tests cover:
- TASK-024: get_dependency_graph() and get_execution_plan() API functions
- TASK-026: deps CLI output format
- TASK-026: plan CLI output format
- TASK-027: graph DOT output format
- TASK-027: graph DOT output with --template filter
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from brasa.engine.dependency_graph import (
    ExecutionPlan,
    ExecutionStep,
    TemplateDependencyGraph,
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


# ===================================================================
# Tests for public API functions (TASK-024)
# ===================================================================


class TestGetDependencyGraph:
    """Test get_dependency_graph() public API function."""

    def test_returns_graph_instance(self):
        """get_dependency_graph() should return a TemplateDependencyGraph."""
        from brasa.engine.api import get_dependency_graph

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
            graph = get_dependency_graph()

        assert isinstance(graph, TemplateDependencyGraph)

    def test_graph_contains_templates(self):
        """get_dependency_graph() should scan and include templates."""
        src = _make_download_template("dl-src")

        from brasa.engine.api import get_dependency_graph

        with (
            patch(
                "brasa.engine.dependency_graph.list_templates",
                return_value=["dl-src"],
            ),
            patch(
                "brasa.engine.dependency_graph.retrieve_template",
                return_value=src,
            ),
        ):
            graph = get_dependency_graph()

        assert "dl-src" in graph


class TestGetExecutionPlan:
    """Test get_execution_plan() public API function."""

    def test_returns_execution_plan(self):
        """get_execution_plan() should return an ExecutionPlan."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])

        from brasa.engine.api import get_execution_plan

        with (
            patch(
                "brasa.engine.dependency_graph.list_templates",
                return_value=["dl-src", "my-etl"],
            ),
            patch(
                "brasa.engine.dependency_graph.retrieve_template",
                side_effect=lambda n: {"dl-src": src, "my-etl": etl}[n],
            ),
            patch(
                "brasa.engine.dependency_graph.CacheManager",
            ),
        ):
            plan = get_execution_plan("my-etl")

        assert isinstance(plan, ExecutionPlan)
        assert plan.target_template == "my-etl"
        assert len(plan.steps) == 2

    def test_force_flag_propagated(self):
        """get_execution_plan(force=True) should mark all for execution."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])

        from brasa.engine.api import get_execution_plan

        with (
            patch(
                "brasa.engine.dependency_graph.list_templates",
                return_value=["dl-src", "my-etl"],
            ),
            patch(
                "brasa.engine.dependency_graph.retrieve_template",
                side_effect=lambda n: {"dl-src": src, "my-etl": etl}[n],
            ),
            patch(
                "brasa.engine.dependency_graph.CacheManager",
            ),
        ):
            plan = get_execution_plan("my-etl", force=True)

        # All steps should be non-skip when forced
        for step in plan.steps:
            assert step.action != "skip"

    def test_unknown_template_raises(self):
        """get_execution_plan() with unknown template should raise KeyError."""
        from brasa.engine.api import get_execution_plan

        with (
            patch(
                "brasa.engine.dependency_graph.list_templates",
                return_value=[],
            ),
            patch(
                "brasa.engine.dependency_graph.retrieve_template",
                side_effect=lambda n: None,
            ),
            pytest.raises(KeyError),
        ):
            get_execution_plan("nonexistent")


# ===================================================================
# Tests for _generate_dot helper (TASK-027)
# ===================================================================


class TestGenerateDot:
    """Test _generate_dot CLI helper."""

    def test_full_graph_dot(self):
        """DOT output for full graph should have correct structure."""
        from brasa.cli import _generate_dot

        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        dot = _generate_dot(graph)

        assert dot.startswith("digraph dependencies {")
        assert dot.endswith("}")
        assert '"dl-src"' in dot
        assert '"my-etl"' in dot
        assert '"dl-src" -> "my-etl"' in dot
        # download templates get lightblue
        assert "lightblue" in dot
        # etl templates get lightyellow
        assert "lightyellow" in dot

    def test_subgraph_dot(self):
        """DOT output with --template should only include ancestors + target."""
        from brasa.cli import _generate_dot

        src1 = _make_download_template("dl-src1")
        src2 = _make_download_template("dl-src2")
        etl1 = _make_etl_template("etl1", input_datasets=["dl-src1"])
        etl2 = _make_etl_template("etl2", input_datasets=["dl-src2"])
        graph = _build_graph_from_templates([src1, src2, etl1, etl2])

        dot = _generate_dot(graph, template_id="etl1")

        assert '"dl-src1"' in dot
        assert '"etl1"' in dot
        # etl2 and dl-src2 should NOT appear in the subgraph for etl1
        assert '"dl-src2"' not in dot
        assert '"etl2"' not in dot

    def test_dot_edges_only_within_subgraph(self):
        """Edges should only connect nodes within the subgraph."""
        from brasa.cli import _generate_dot

        src = _make_download_template("dl-src")
        mid = _make_etl_template("etl-mid", input_datasets=["dl-src"])
        end = _make_etl_template("etl-end", input_datasets=["staging.etl-mid"])
        graph = _build_graph_from_templates([src, mid, end])

        dot = _generate_dot(graph, template_id="etl-end")

        assert '"dl-src" -> "etl-mid"' in dot
        assert '"etl-mid" -> "etl-end"' in dot

    def test_no_template_filter_includes_all(self):
        """Without template filter, all graph nodes should appear."""
        from brasa.cli import _generate_dot

        src1 = _make_download_template("a")
        src2 = _make_download_template("b")
        etl = _make_etl_template("c", input_datasets=["a", "b"])
        graph = _build_graph_from_templates([src1, src2, etl])

        dot = _generate_dot(graph)

        assert '"a"' in dot
        assert '"b"' in dot
        assert '"c"' in dot


# ===================================================================
# Tests for deps command output format (TASK-026)
# ===================================================================


class TestDepsOutput:
    """Test the deps command output structure.

    Since the CLI runs in __main__, we test the logic by
    calling the dependency graph methods directly and verifying
    the data that would be printed.
    """

    def test_deps_shows_upstream(self):
        """deps should report direct upstream templates."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        upstream = graph.get_upstream("my-etl")
        assert upstream == ["dl-src"]

    def test_deps_shows_downstream(self):
        """deps should report direct downstream templates."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        graph = _build_graph_from_templates([src, etl])

        downstream = graph.get_downstream("dl-src")
        assert downstream == ["my-etl"]

    def test_deps_shows_ancestors(self):
        """deps should report all transitive ancestors."""
        src = _make_download_template("dl-src")
        mid = _make_etl_template("etl-mid", input_datasets=["dl-src"])
        end = _make_etl_template("etl-end", input_datasets=["staging.etl-mid"])
        graph = _build_graph_from_templates([src, mid, end])

        ancestors = graph.get_ancestors("etl-end")
        assert ancestors == {"dl-src", "etl-mid"}

    def test_deps_shows_outputs(self):
        """deps should report template outputs."""
        src = _make_download_template("dl-src")
        graph = _build_graph_from_templates([src])

        outputs = graph.get_outputs("dl-src")
        assert len(outputs) == 1
        assert outputs[0].dataset_id == "input/dl-src"

    def test_deps_source_template_no_upstream(self):
        """Source templates should have no upstream."""
        src = _make_download_template("dl-src")
        graph = _build_graph_from_templates([src])

        upstream = graph.get_upstream("dl-src")
        assert upstream == []


# ===================================================================
# Tests for plan command output format (TASK-026)
# ===================================================================


class TestPlanOutput:
    """Test the plan command output format."""

    def test_plan_str_format(self):
        """ExecutionPlan.__str__ should produce structured output."""
        plan = ExecutionPlan(
            target_template="my-etl",
            steps=[
                ExecutionStep("dl-src", "process", "unprocessed downloads", "download"),
                ExecutionStep("my-etl", "etl", "output missing", "etl"),
            ],
        )
        output = str(plan)

        assert "Execution plan for 'my-etl'" in output
        assert "[PROCESS]" in output
        assert "[ETL]" in output
        assert "dl-src" in output
        assert "my-etl" in output
        assert "2 steps" in output
        assert "2 to execute" in output
        assert "0 to skip" in output

    def test_plan_with_skips(self):
        """Plan with mixed skip/execute steps should display correctly."""
        plan = ExecutionPlan(
            target_template="my-etl",
            steps=[
                ExecutionStep("dl-src", "skip", "already processed", "download"),
                ExecutionStep("my-etl", "etl", "output missing", "etl"),
            ],
        )
        output = str(plan)

        assert "[SKIP]" in output
        assert "[ETL]" in output
        assert "1 to execute" in output
        assert "1 to skip" in output


# ===================================================================
# Tests for exports (TASK-025)
# ===================================================================


class TestPublicExports:
    """Verify that new symbols are exported from brasa and brasa.engine."""

    def test_brasa_exports_graph_classes(self):
        """brasa.__all__ should include dependency graph items."""
        import brasa

        assert "TemplateDependencyGraph" in brasa.__all__
        assert "ExecutionPlan" in brasa.__all__
        assert "ExecutionStep" in brasa.__all__
        assert "PipelineOrchestrator" in brasa.__all__
        assert "OrchestratorReport" in brasa.__all__
        assert "get_dependency_graph" in brasa.__all__
        assert "get_execution_plan" in brasa.__all__

    def test_brasa_engine_exports_graph_classes(self):
        """brasa.engine.__all__ should include dependency graph items."""
        import brasa.engine

        assert "TemplateDependencyGraph" in brasa.engine.__all__
        assert "ExecutionPlan" in brasa.engine.__all__
        assert "ExecutionStep" in brasa.engine.__all__
        assert "DatasetOutput" in brasa.engine.__all__
        assert "CyclicDependencyError" in brasa.engine.__all__
        assert "PipelineOrchestrator" in brasa.engine.__all__
        assert "OrchestratorReport" in brasa.engine.__all__
        assert "get_dependency_graph" in brasa.engine.__all__
        assert "get_execution_plan" in brasa.engine.__all__

    def test_importable_from_brasa(self):
        """Key symbols should be directly importable from brasa."""
        from brasa import (
            ExecutionPlan,
            ExecutionStep,
            OrchestratorReport,
            PipelineOrchestrator,
            TemplateDependencyGraph,
            get_dependency_graph,
            get_execution_plan,
        )

        assert get_dependency_graph is not None
        assert get_execution_plan is not None
        assert TemplateDependencyGraph is not None
        assert ExecutionPlan is not None
        assert ExecutionStep is not None
        assert PipelineOrchestrator is not None
        assert OrchestratorReport is not None

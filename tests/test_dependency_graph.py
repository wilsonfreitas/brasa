"""Unit tests for the template dependency graph.

Tests cover:
- Output discovery for single-output, multi-output, and ETL templates
- Dependency discovery for load, concat_datasets, sql_query pipeline steps
- Exclusion of legacy function-based templates
- Reverse index construction
- Edge building and template edge resolution
- Normalisation of dataset references
- Public query helpers
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from brasa.engine.dependency_graph import (
    CyclicDependencyError,
    DatasetOutput,
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
# Helpers to build lightweight mock templates
# ---------------------------------------------------------------------------


def _make_writer(
    layer: str = "input",
    dataset: str = "",
    template_id: str = "",
) -> MarketDataWriter:
    """Create a MarketDataWriter without touching YAML."""
    cfg: dict = {"layer": layer, "partitioning": []}
    if dataset:
        cfg["dataset"] = dataset
    w = MarketDataWriter(cfg, template_id=template_id)
    return w


def _make_download_template(
    template_id: str,
    *,
    datasets: dict | None = None,
    writer_layer: str = "input",
    has_pipeline: bool = True,
    dependencies: list | None = None,
) -> MarketDataTemplate:
    """Build a minimal download-type template mock.

    Args:
        template_id: The template id.
        datasets: If provided, creates a multi-output template.
        writer_layer: Writer layer string.
        has_pipeline: Whether the reader uses a pipeline.
        dependencies: Raw dependencies block (list of dicts) as stored from YAML.

    Returns:
        A mock MarketDataTemplate.
    """
    tmpl = MagicMock(spec=MarketDataTemplate)
    tmpl.id = template_id
    tmpl.has_reader = True
    tmpl.is_etl = False

    # Writer
    tmpl.writer = _make_writer(
        layer=writer_layer,
        dataset=template_id,
        template_id=template_id,
    )

    # Reader with pipeline flag
    reader = MagicMock(spec=MarketDataReader)
    reader.has_pipeline = has_pipeline
    tmpl.reader = reader

    # Datasets
    if datasets is not None:
        from brasa.engine.template import DatasetConfig
        from brasa.fieldsets import Fieldset

        tmpl.datasets = {
            name: DatasetConfig(name=name, tag=tag, fields=Fieldset())
            for name, tag in datasets.items()
        }
    else:
        tmpl.datasets = None

    tmpl.dependencies = dependencies

    return tmpl


def _make_etl_template(
    template_id: str,
    input_datasets: list[str],
    *,
    writer_layer: str = "staging",
    writer_dataset: str = "",
    is_pipeline: bool = True,
) -> MarketDataTemplate:
    """Build a minimal ETL-type template mock.

    Args:
        template_id: The template id.
        input_datasets: Dataset refs this ETL depends on.
        writer_layer: Writer layer string.
        writer_dataset: Explicit dataset name (defaults to template_id).
        is_pipeline: Whether this uses pipeline-based ETL.

    Returns:
        A mock MarketDataTemplate.
    """
    tmpl = MagicMock(spec=MarketDataTemplate)
    tmpl.id = template_id
    tmpl.has_reader = False
    tmpl.is_etl = True
    tmpl.datasets = None

    # Writer
    tmpl.writer = _make_writer(
        layer=writer_layer,
        dataset=writer_dataset or template_id,
        template_id=template_id,
    )

    # ETL mock
    etl = MagicMock(spec=MarketDataETL)
    etl.is_pipeline = is_pipeline
    etl.get_input_datasets.return_value = list(input_datasets)
    tmpl.etl = etl

    return tmpl


def _make_legacy_etl_template(template_id: str) -> MarketDataTemplate:
    """Build a legacy function-based ETL template mock."""
    tmpl = MagicMock(spec=MarketDataTemplate)
    tmpl.id = template_id
    tmpl.has_reader = False
    tmpl.is_etl = True
    tmpl.datasets = None
    tmpl.writer = _make_writer(
        layer="staging",
        dataset=template_id,
        template_id=template_id,
    )

    etl = MagicMock(spec=MarketDataETL)
    etl.is_pipeline = False
    tmpl.etl = etl

    return tmpl


def _make_legacy_reader_template(template_id: str) -> MarketDataTemplate:
    """Build a legacy function-based reader template mock."""
    tmpl = MagicMock(spec=MarketDataTemplate)
    tmpl.id = template_id
    tmpl.has_reader = True
    tmpl.is_etl = False
    tmpl.datasets = None
    tmpl.writer = _make_writer(
        layer="input",
        dataset=template_id,
        template_id=template_id,
    )

    reader = MagicMock(spec=MarketDataReader)
    reader.has_pipeline = False  # legacy — no pipeline
    tmpl.reader = reader

    return tmpl


def _build_graph_from_templates(
    templates: list[MarketDataTemplate],
) -> TemplateDependencyGraph:
    """Build a TemplateDependencyGraph from a list of mock templates.

    Patches ``list_templates`` and ``retrieve_template`` so that the
    graph constructor only sees the provided templates.
    """
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
# TEST-001: Output discovery — single dataset download template
# ===================================================================


class TestDiscoverOutputsSingleDataset:
    """Verify single-output download templates produce one DatasetOutput."""

    def test_single_output(self):
        tmpl = _make_download_template("b3-cotahist-daily")
        outputs = TemplateDependencyGraph._discover_outputs(tmpl)

        assert len(outputs) == 1
        out = outputs[0]
        assert out.dataset_id == "input/b3-cotahist-daily"
        assert out.layer == "input"
        assert out.dataset_name == "b3-cotahist-daily"
        assert out.template_id == "b3-cotahist-daily"

    def test_single_output_custom_layer(self):
        tmpl = _make_download_template("my-template", writer_layer="staging")
        outputs = TemplateDependencyGraph._discover_outputs(tmpl)
        assert outputs[0].dataset_id == "staging/my-template"
        assert outputs[0].layer == "staging"


# ===================================================================
# TEST-002: Output discovery — multi-dataset download template
# ===================================================================


class TestDiscoverOutputsMultiDataset:
    """Verify multi-output download templates produce multiple DatasetOutputs."""

    def test_multi_output(self):
        tmpl = _make_download_template(
            "b3-bvbg028",
            datasets={
                "equities": "EqtyInf",
                "options_on_equities": "OptnOnEqtsInf",
                "future_contracts": "FutrCtrctsInf",
            },
        )
        outputs = TemplateDependencyGraph._discover_outputs(tmpl)

        assert len(outputs) == 3
        ids = {o.dataset_id for o in outputs}
        assert "input/b3-bvbg028-equities" in ids
        assert "input/b3-bvbg028-options_on_equities" in ids
        assert "input/b3-bvbg028-future_contracts" in ids

        for o in outputs:
            assert o.template_id == "b3-bvbg028"
            assert o.layer == "input"


# ===================================================================
# TEST-003: Dependency discovery — load step
# ===================================================================


class TestDiscoverDependenciesLoadStep:
    """Verify ETL with ``load`` step reports correct dependency."""

    def test_load_step_dependency(self):
        tmpl = _make_etl_template(
            "b3-futures",
            input_datasets=["b3-futures-settlement-prices"],
        )
        deps = TemplateDependencyGraph._discover_dependencies(tmpl)
        assert deps == ["b3-futures-settlement-prices"]


# ===================================================================
# TEST-004: Dependency discovery — concat_datasets step
# ===================================================================


class TestDiscoverDependenciesConcatStep:
    """Verify ETL with ``concat_datasets`` step reports all inputs."""

    def test_concat_step_dependencies(self):
        tmpl = _make_etl_template(
            "b3-cotahist",
            input_datasets=["b3-cotahist-yearly", "b3-cotahist-daily"],
        )
        deps = TemplateDependencyGraph._discover_dependencies(tmpl)
        assert set(deps) == {"b3-cotahist-yearly", "b3-cotahist-daily"}


# ===================================================================
# TEST-005: Dependency discovery — sql_query step
# ===================================================================


class TestDiscoverDependenciesSqlQueryStep:
    """Verify ETL with ``sql_query`` step reports dataset dependencies."""

    def test_sql_query_step_dependencies(self):
        tmpl = _make_etl_template(
            "b3-equities-register",
            input_datasets=["input.b3-bvbg028-equities"],
        )
        deps = TemplateDependencyGraph._discover_dependencies(tmpl)
        assert deps == ["input.b3-bvbg028-equities"]


# ===================================================================
# TEST-006: Legacy function-based templates are excluded
# ===================================================================


class TestLegacyTemplatesExcluded:
    """Verify legacy function-based templates are not in the graph."""

    def test_legacy_etl_excluded(self):
        pipeline_tmpl = _make_download_template("good-template")
        legacy_tmpl = _make_legacy_etl_template("bad-etl-template")
        g = _build_graph_from_templates([pipeline_tmpl, legacy_tmpl])

        assert "good-template" in g
        assert "bad-etl-template" not in g

    def test_legacy_reader_excluded(self):
        pipeline_tmpl = _make_download_template("good-template")
        legacy_tmpl = _make_legacy_reader_template("bad-reader-template")
        g = _build_graph_from_templates([pipeline_tmpl, legacy_tmpl])

        assert "good-template" in g
        assert "bad-reader-template" not in g


# ===================================================================
# TEST-007: Legacy reader templates excluded
# ===================================================================


class TestLegacyReaderTemplatesExcluded:
    """Verify legacy reader templates with reader.function are excluded."""

    def test_reader_function_excluded(self):
        t1 = _make_download_template("pipeline-reader")
        t2 = _make_legacy_reader_template("function-reader")
        g = _build_graph_from_templates([t1, t2])

        assert "pipeline-reader" in g
        assert "function-reader" not in g
        assert len(g) == 1


# ===================================================================
# TEST-008: Reverse index construction
# ===================================================================


class TestReverseIndexConstruction:
    """Verify reverse index maps dataset ids to producing templates."""

    def test_basic_reverse_index(self):
        t1 = _make_download_template("tmpl-a")
        t2 = _make_download_template(
            "tmpl-b",
            datasets={"ds1": "tag1", "ds2": "tag2"},
        )
        t3 = _make_etl_template(
            "tmpl-c",
            input_datasets=["input.tmpl-b-ds1"],
        )

        g = _build_graph_from_templates([t1, t2, t3])

        assert g.reverse_index["input/tmpl-a"] == "tmpl-a"
        assert g.reverse_index["input/tmpl-b-ds1"] == "tmpl-b"
        assert g.reverse_index["input/tmpl-b-ds2"] == "tmpl-b"
        assert g.reverse_index["staging/tmpl-c"] == "tmpl-c"

    def test_duplicate_producer_raises(self):
        """Two templates producing the same dataset should raise."""
        t1 = _make_download_template("same-name")
        t2 = _make_download_template("same-name-dup")
        # Force t2 to produce same dataset id as t1
        t2.writer = _make_writer(
            layer="input", dataset="same-name", template_id="same-name-dup"
        )
        t2.datasets = None

        with pytest.raises(ValueError, match="Duplicate dataset producer"):
            _build_graph_from_templates([t1, t2])


# ===================================================================
# Normalise dataset references
# ===================================================================


class TestNormalizeDatasetRef:
    """Verify dataset reference parsing."""

    def test_with_layer_prefix(self):
        layer, name = TemplateDependencyGraph._normalize_dataset_ref(
            "input.b3-bvbg028-equities"
        )
        assert layer == "input"
        assert name == "b3-bvbg028-equities"

    def test_staging_prefix(self):
        layer, name = TemplateDependencyGraph._normalize_dataset_ref(
            "staging.my-dataset"
        )
        assert layer == "staging"
        assert name == "my-dataset"

    def test_bare_name_defaults_to_input(self):
        layer, name = TemplateDependencyGraph._normalize_dataset_ref(
            "b3-futures-settlement-prices"
        )
        assert layer == "input"
        assert name == "b3-futures-settlement-prices"


# ===================================================================
# Edge building
# ===================================================================


class TestBuildTemplateEdges:
    """Verify that edges connect ETL templates to their upstream producers."""

    def test_simple_chain(self):
        """download → etl should have one edge."""
        t1 = _make_download_template("src-data")
        t2 = _make_etl_template("derived", input_datasets=["src-data"])
        g = _build_graph_from_templates([t1, t2])

        assert g.edges["derived"] == ["src-data"]
        assert g.edges["src-data"] == []

    def test_multi_output_edge(self):
        """Multi-output download → etl via specific dataset."""
        t1 = _make_download_template(
            "multi-src",
            datasets={"ds_a": "A", "ds_b": "B"},
        )
        t2 = _make_etl_template(
            "consumer",
            input_datasets=["input.multi-src-ds_a"],
        )
        g = _build_graph_from_templates([t1, t2])

        assert g.edges["consumer"] == ["multi-src"]

    def test_diamond_dependency(self):
        """Two ETL templates converging on a common ancestor."""
        src = _make_download_template("base")
        etl_a = _make_etl_template("branch-a", input_datasets=["base"])
        etl_b = _make_etl_template("branch-b", input_datasets=["base"])
        merge = _make_etl_template(
            "merge",
            input_datasets=[
                "staging.branch-a",
                "staging.branch-b",
            ],
        )
        g = _build_graph_from_templates([src, etl_a, etl_b, merge])

        assert set(g.edges["merge"]) == {"branch-a", "branch-b"}
        assert g.edges["branch-a"] == ["base"]
        assert g.edges["branch-b"] == ["base"]

    def test_unresolved_dependency_logged(self, caplog):
        """Dependencies on unknown datasets are logged."""
        t1 = _make_etl_template(
            "orphan",
            input_datasets=["nonexistent-dataset"],
        )
        import logging

        with caplog.at_level(logging.WARNING):
            g = _build_graph_from_templates([t1])

        assert g.edges["orphan"] == []
        assert "no known producer" in caplog.text

    def test_no_self_edges(self):
        """A template referencing its own output should not create a self-edge."""
        t1 = _make_etl_template(
            "self-ref",
            input_datasets=["staging.self-ref"],
        )
        g = _build_graph_from_templates([t1])
        assert g.edges["self-ref"] == []


# ===================================================================
# Public query helpers
# ===================================================================


class TestPublicQueryHelpers:
    """Verify public query methods on the graph."""

    @pytest.fixture()
    def graph(self) -> TemplateDependencyGraph:
        src = _make_download_template("dl-tmpl")
        etl1 = _make_etl_template("etl-1", input_datasets=["dl-tmpl"])
        etl2 = _make_etl_template("etl-2", input_datasets=["staging.etl-1"])
        return _build_graph_from_templates([src, etl1, etl2])

    def test_get_upstream(self, graph: TemplateDependencyGraph):
        assert graph.get_upstream("dl-tmpl") == []
        assert graph.get_upstream("etl-1") == ["dl-tmpl"]
        assert graph.get_upstream("etl-2") == ["etl-1"]

    def test_get_upstream_unknown_raises(self, graph: TemplateDependencyGraph):
        with pytest.raises(KeyError, match="not in the dependency graph"):
            graph.get_upstream("not-a-template")

    def test_get_downstream(self, graph: TemplateDependencyGraph):
        assert graph.get_downstream("dl-tmpl") == ["etl-1"]
        assert graph.get_downstream("etl-1") == ["etl-2"]
        assert graph.get_downstream("etl-2") == []

    def test_get_template_type(self, graph: TemplateDependencyGraph):
        assert graph.get_template_type("dl-tmpl") == "download"
        assert graph.get_template_type("etl-1") == "etl"

    def test_get_outputs(self, graph: TemplateDependencyGraph):
        outputs = graph.get_outputs("dl-tmpl")
        assert len(outputs) == 1
        assert outputs[0].dataset_id == "input/dl-tmpl"

    def test_get_producer(self, graph: TemplateDependencyGraph):
        assert graph.get_producer("input/dl-tmpl") == "dl-tmpl"
        assert graph.get_producer("staging/etl-1") == "etl-1"
        assert graph.get_producer("nonexistent") is None

    def test_template_ids(self, graph: TemplateDependencyGraph):
        assert graph.template_ids == ["dl-tmpl", "etl-1", "etl-2"]

    def test_len(self, graph: TemplateDependencyGraph):
        assert len(graph) == 3

    def test_contains(self, graph: TemplateDependencyGraph):
        assert "dl-tmpl" in graph
        assert "nope" not in graph

    def test_repr(self, graph: TemplateDependencyGraph):
        r = repr(graph)
        assert "templates=3" in r
        assert "TemplateDependencyGraph" in r


# ===================================================================
# Integration-style test against real templates
# ===================================================================


class TestIntegrationWithRealTemplates:
    """Run the dependency graph against the actual template files.

    These tests verify that the graph builds without errors and that
    known dependency chains are correctly discovered.
    """

    @pytest.fixture(scope="class")
    def graph(self) -> TemplateDependencyGraph:
        return TemplateDependencyGraph()

    def test_graph_builds(self, graph: TemplateDependencyGraph):
        """Graph should build with some templates."""
        assert len(graph) > 0

    def test_b3_bvbg028_produces_multi_outputs(self, graph: TemplateDependencyGraph):
        """b3-bvbg028 is a multi-output download template."""
        if "b3-bvbg028" not in graph:
            pytest.skip("b3-bvbg028 template not available")
        outputs = graph.get_outputs("b3-bvbg028")
        ds_ids = {o.dataset_id for o in outputs}
        assert "input/b3-bvbg028-equities" in ds_ids
        assert "input/b3-bvbg028-future_contracts" in ds_ids

    def test_b3_equities_register_depends_on_bvbg028(
        self, graph: TemplateDependencyGraph
    ):
        """b3-equities-register should depend on b3-bvbg028."""
        if "b3-equities-register" not in graph:
            pytest.skip("b3-equities-register template not available")
        upstreams = graph.get_upstream("b3-equities-register")
        assert "b3-bvbg028" in upstreams

    def test_b3_equities_spot_market_chain(self, graph: TemplateDependencyGraph):
        """b3-equities-spot-market depends on b3-equities-register."""
        if "b3-equities-spot-market" not in graph:
            pytest.skip("b3-equities-spot-market template not available")
        upstreams = graph.get_upstream("b3-equities-spot-market")
        assert "b3-equities-register" in upstreams

    def test_b3_cotahist_depends_on_yearly_and_daily(
        self, graph: TemplateDependencyGraph
    ):
        """b3-cotahist uses concat_datasets with yearly + daily."""
        if "b3-cotahist" not in graph:
            pytest.skip("b3-cotahist template not available")
        upstreams = graph.get_upstream("b3-cotahist")
        assert "b3-cotahist-yearly" in upstreams
        assert "b3-cotahist-daily" in upstreams

    def test_download_templates_without_deps_are_source_nodes(
        self, graph: TemplateDependencyGraph
    ):
        """Download templates without a ``dependencies:`` block should be source nodes."""
        for tid in graph.template_ids:
            if graph.get_template_type(tid) != "download":
                continue
            tmpl = graph.templates[tid]
            declared_deps = getattr(tmpl, "dependencies", None)
            if declared_deps:
                continue  # legitimately has upstream deps
            assert graph.get_upstream(tid) == [], (
                f"Download template '{tid}' should be a source node"
            )

    def test_reverse_index_covers_all_outputs(self, graph: TemplateDependencyGraph):
        """Every output should be in the reverse index."""
        for tid, ds_list in graph.outputs.items():
            for ds_out in ds_list:
                assert ds_out.dataset_id in graph.reverse_index
                assert graph.reverse_index[ds_out.dataset_id] == tid


# ===================================================================
# ConcatDatasetsStep.get_input_datasets override
# ===================================================================


class TestConcatDatasetsStepGetInputDatasets:
    """Verify the ConcatDatasetsStep correctly reports its inputs."""

    def test_get_input_datasets(self):
        from brasa.engine.pipeline.steps.etl_steps import ConcatDatasetsStep

        step = ConcatDatasetsStep(
            params={
                "inputs": ["ds-a", "ds-b", "ds-c"],
                "layer": "input",
            }
        )
        result = step.get_input_datasets()
        assert result == ["ds-a", "ds-b", "ds-c"]

    def test_get_input_datasets_empty(self):
        from brasa.engine.pipeline.steps.etl_steps import ConcatDatasetsStep

        step = ConcatDatasetsStep(params={"layer": "input"})
        assert step.get_input_datasets() == []


# ===================================================================
# DatasetOutput dataclass
# ===================================================================


class TestDatasetOutput:
    """Verify DatasetOutput is frozen and has correct fields."""

    def test_frozen(self):
        ds = DatasetOutput(
            dataset_id="input/test",
            layer="input",
            dataset_name="test",
            template_id="tmpl",
        )
        with pytest.raises(AttributeError):
            ds.layer = "staging"  # type: ignore[misc]

    def test_equality(self):
        a = DatasetOutput("input/x", "input", "x", "t1")
        b = DatasetOutput("input/x", "input", "x", "t1")
        assert a == b

    def test_hash(self):
        a = DatasetOutput("input/x", "input", "x", "t1")
        assert hash(a) == hash(DatasetOutput("input/x", "input", "x", "t1"))


# ===================================================================
# _is_pipeline_template static method
# ===================================================================


class TestIsPipelineTemplate:
    """Verify the classification of pipeline vs legacy templates."""

    def test_pipeline_reader(self):
        tmpl = _make_download_template("t", has_pipeline=True)
        assert TemplateDependencyGraph._is_pipeline_template(tmpl) is True

    def test_legacy_reader(self):
        tmpl = _make_legacy_reader_template("t")
        assert TemplateDependencyGraph._is_pipeline_template(tmpl) is False

    def test_pipeline_etl(self):
        tmpl = _make_etl_template("t", input_datasets=[], is_pipeline=True)
        assert TemplateDependencyGraph._is_pipeline_template(tmpl) is True

    def test_legacy_etl(self):
        tmpl = _make_legacy_etl_template("t")
        assert TemplateDependencyGraph._is_pipeline_template(tmpl) is False


# ===================================================================
# Error handling during template loading
# ===================================================================


class TestTemplateLoadErrors:
    """Verify broken templates are skipped gracefully."""

    def test_broken_template_skipped(self, caplog):
        """If retrieve_template raises, the template is skipped."""
        import logging

        def _side_effect(name: str):
            if name == "bad":
                raise ValueError("Broken template")
            return _make_download_template(name)

        with (
            patch(
                "brasa.engine.dependency_graph.list_templates",
                return_value=["good", "bad"],
            ),
            patch(
                "brasa.engine.dependency_graph.retrieve_template",
                side_effect=_side_effect,
            ),
            caplog.at_level(logging.WARNING),
        ):
            g = TemplateDependencyGraph()

        assert "good" in g
        assert "bad" not in g
        assert "failed to load" in caplog.text


# ===================================================================
# Phase 2: Topological Sort & Cycle Detection
# ===================================================================


# ===================================================================
# TEST-009: Topological sort — simple chain
# ===================================================================


class TestTopologicalSortSimpleChain:
    """Verify correct ordering for a linear chain.

    Graph: dl-src → etl-mid → etl-end
    Expected order: [dl-src, etl-mid, etl-end]
    """

    @pytest.fixture()
    def graph(self) -> TemplateDependencyGraph:
        src = _make_download_template("dl-src")
        mid = _make_etl_template("etl-mid", input_datasets=["dl-src"])
        end = _make_etl_template("etl-end", input_datasets=["staging.etl-mid"])
        return _build_graph_from_templates([src, mid, end])

    def test_full_chain(self, graph: TemplateDependencyGraph):
        order = graph.topological_sort("etl-end")
        assert order == ["dl-src", "etl-mid", "etl-end"]

    def test_mid_node(self, graph: TemplateDependencyGraph):
        order = graph.topological_sort("etl-mid")
        assert order == ["dl-src", "etl-mid"]

    def test_source_node(self, graph: TemplateDependencyGraph):
        order = graph.topological_sort("dl-src")
        assert order == ["dl-src"]

    def test_unknown_template_raises(self, graph: TemplateDependencyGraph):
        with pytest.raises(KeyError, match="not in the dependency graph"):
            graph.topological_sort("nonexistent")


# ===================================================================
# TEST-010: Topological sort — diamond dependency
# ===================================================================


class TestTopologicalSortDiamond:
    """Verify correct ordering for a diamond-shaped DAG.

    Graph:
        base → branch-a ↘
                          merge
        base → branch-b ↗

    Expected: base comes first, then branch-a/branch-b (in any order
    but deterministic via sorted), then merge last.
    """

    @pytest.fixture()
    def graph(self) -> TemplateDependencyGraph:
        src = _make_download_template("base")
        etl_a = _make_etl_template("branch-a", input_datasets=["base"])
        etl_b = _make_etl_template("branch-b", input_datasets=["base"])
        merge = _make_etl_template(
            "merge",
            input_datasets=["staging.branch-a", "staging.branch-b"],
        )
        return _build_graph_from_templates([src, etl_a, etl_b, merge])

    def test_diamond_order(self, graph: TemplateDependencyGraph):
        order = graph.topological_sort("merge")
        assert order[0] == "base"
        assert order[-1] == "merge"
        assert set(order[1:-1]) == {"branch-a", "branch-b"}

    def test_diamond_all_present(self, graph: TemplateDependencyGraph):
        order = graph.topological_sort("merge")
        assert len(order) == 4
        assert set(order) == {"base", "branch-a", "branch-b", "merge"}

    def test_branch_only(self, graph: TemplateDependencyGraph):
        """Sort for branch-a should not include branch-b or merge."""
        order = graph.topological_sort("branch-a")
        assert order == ["base", "branch-a"]


# ===================================================================
# TEST-011: Cycle detection
# ===================================================================


class TestCycleDetection:
    """Verify detect_cycles() finds artificial cycles."""

    def test_detects_simple_cycle(self):
        """A → B → A should be detected as a cycle."""
        t_a = _make_etl_template("t-a", input_datasets=["staging.t-b"])
        t_b = _make_etl_template("t-b", input_datasets=["staging.t-a"])

        with pytest.raises(CyclicDependencyError, match="Circular dependencies"):
            _build_graph_from_templates([t_a, t_b])

    def test_detects_three_node_cycle(self):
        """A → B → C → A should be detected."""
        t_a = _make_etl_template("t-a", input_datasets=["staging.t-c"])
        t_b = _make_etl_template("t-b", input_datasets=["staging.t-a"])
        t_c = _make_etl_template("t-c", input_datasets=["staging.t-b"])

        with pytest.raises(CyclicDependencyError, match="Circular dependencies"):
            _build_graph_from_templates([t_a, t_b, t_c])

    def test_no_cycles_in_dag(self):
        """A clean DAG should return empty list from detect_cycles."""
        src = _make_download_template("src")
        etl = _make_etl_template("etl", input_datasets=["src"])
        g = _build_graph_from_templates([src, etl])

        assert g.detect_cycles() == []

    def test_topological_sort_raises_on_injected_cycle(self):
        """Manually injecting a cycle into edges causes topological_sort to fail."""
        src = _make_download_template("x-src")
        mid = _make_etl_template("x-mid", input_datasets=["x-src"])
        end = _make_etl_template("x-end", input_datasets=["staging.x-mid"])
        g = _build_graph_from_templates([src, mid, end])

        # Manually inject a cycle: x-src depends on x-end
        g.edges["x-src"] = ["x-end"]

        with pytest.raises(CyclicDependencyError, match="Cyclic dependency"):
            g.topological_sort("x-end")


# ===================================================================
# TEST: get_ancestors
# ===================================================================


class TestGetAncestors:
    """Verify get_ancestors returns all transitive upstream templates."""

    @pytest.fixture()
    def graph(self) -> TemplateDependencyGraph:
        src = _make_download_template("dl-root")
        mid = _make_etl_template("etl-mid", input_datasets=["dl-root"])
        leaf = _make_etl_template("etl-leaf", input_datasets=["staging.etl-mid"])
        return _build_graph_from_templates([src, mid, leaf])

    def test_leaf_ancestors(self, graph: TemplateDependencyGraph):
        assert graph.get_ancestors("etl-leaf") == {"dl-root", "etl-mid"}

    def test_mid_ancestors(self, graph: TemplateDependencyGraph):
        assert graph.get_ancestors("etl-mid") == {"dl-root"}

    def test_root_ancestors(self, graph: TemplateDependencyGraph):
        assert graph.get_ancestors("dl-root") == set()

    def test_unknown_raises(self, graph: TemplateDependencyGraph):
        with pytest.raises(KeyError, match="not in the dependency graph"):
            graph.get_ancestors("ghost")


class TestGetAncestorsDiamond:
    """Verify get_ancestors with diamond DAG."""

    def test_diamond_ancestors(self):
        src = _make_download_template("root")
        a = _make_etl_template("a", input_datasets=["root"])
        b = _make_etl_template("b", input_datasets=["root"])
        merge = _make_etl_template("merge", input_datasets=["staging.a", "staging.b"])
        g = _build_graph_from_templates([src, a, b, merge])

        assert g.get_ancestors("merge") == {"root", "a", "b"}
        assert g.get_ancestors("a") == {"root"}
        assert g.get_ancestors("b") == {"root"}


# ===================================================================
# TEST: get_descendants
# ===================================================================


class TestGetDescendants:
    """Verify get_descendants returns all transitive downstream templates."""

    @pytest.fixture()
    def graph(self) -> TemplateDependencyGraph:
        src = _make_download_template("dl-root")
        mid = _make_etl_template("etl-mid", input_datasets=["dl-root"])
        leaf = _make_etl_template("etl-leaf", input_datasets=["staging.etl-mid"])
        return _build_graph_from_templates([src, mid, leaf])

    def test_root_descendants(self, graph: TemplateDependencyGraph):
        assert graph.get_descendants("dl-root") == {"etl-mid", "etl-leaf"}

    def test_mid_descendants(self, graph: TemplateDependencyGraph):
        assert graph.get_descendants("etl-mid") == {"etl-leaf"}

    def test_leaf_descendants(self, graph: TemplateDependencyGraph):
        assert graph.get_descendants("etl-leaf") == set()

    def test_unknown_raises(self, graph: TemplateDependencyGraph):
        with pytest.raises(KeyError, match="not in the dependency graph"):
            graph.get_descendants("ghost")


class TestGetDescendantsDiamond:
    """Verify get_descendants with diamond DAG."""

    def test_diamond_descendants(self):
        src = _make_download_template("root")
        a = _make_etl_template("a", input_datasets=["root"])
        b = _make_etl_template("b", input_datasets=["root"])
        merge = _make_etl_template("merge", input_datasets=["staging.a", "staging.b"])
        g = _build_graph_from_templates([src, a, b, merge])

        assert g.get_descendants("root") == {"a", "b", "merge"}
        assert g.get_descendants("a") == {"merge"}
        assert g.get_descendants("b") == {"merge"}
        assert g.get_descendants("merge") == set()


# ===================================================================
# Integration: Phase 2 with real templates
# ===================================================================


class TestIntegrationPhase2WithRealTemplates:
    """Run topological sort and ancestor/descendant queries against real templates."""

    @pytest.fixture(scope="class")
    def graph(self) -> TemplateDependencyGraph:
        return TemplateDependencyGraph()

    def test_no_cycles(self, graph: TemplateDependencyGraph):
        """Real templates should have no cycles."""
        assert graph.detect_cycles() == []

    def test_topological_sort_b3_equities_register(
        self, graph: TemplateDependencyGraph
    ):
        """Topological sort for b3-equities-register should start with b3-bvbg028."""
        if "b3-equities-register" not in graph:
            pytest.skip("b3-equities-register template not available")
        order = graph.topological_sort("b3-equities-register")
        assert order[0] == "b3-bvbg028"
        assert order[-1] == "b3-equities-register"

    def test_topological_sort_b3_equities_spot_market(
        self, graph: TemplateDependencyGraph
    ):
        """Full chain: b3-bvbg028 → b3-equities-register → b3-equities-spot-market."""
        if "b3-equities-spot-market" not in graph:
            pytest.skip("b3-equities-spot-market template not available")
        order = graph.topological_sort("b3-equities-spot-market")
        assert order[0] == "b3-bvbg028"
        assert order[-1] == "b3-equities-spot-market"
        # b3-equities-register must come before b3-equities-spot-market
        idx_reg = order.index("b3-equities-register")
        idx_spot = order.index("b3-equities-spot-market")
        assert idx_reg < idx_spot

    def test_topological_sort_b3_cotahist(self, graph: TemplateDependencyGraph):
        """b3-cotahist depends on b3-cotahist-yearly and b3-cotahist-daily."""
        if "b3-cotahist" not in graph:
            pytest.skip("b3-cotahist template not available")
        order = graph.topological_sort("b3-cotahist")
        assert order[-1] == "b3-cotahist"
        assert "b3-cotahist-yearly" in order
        assert "b3-cotahist-daily" in order

    def test_ancestors_b3_equities_spot_market(self, graph: TemplateDependencyGraph):
        """b3-equities-spot-market should have b3-bvbg028 and b3-equities-register as ancestors."""
        if "b3-equities-spot-market" not in graph:
            pytest.skip("b3-equities-spot-market template not available")
        ancestors = graph.get_ancestors("b3-equities-spot-market")
        assert "b3-bvbg028" in ancestors
        assert "b3-equities-register" in ancestors

    def test_descendants_b3_bvbg028(self, graph: TemplateDependencyGraph):
        """b3-bvbg028 should have b3-equities-register as a descendant."""
        if "b3-bvbg028" not in graph:
            pytest.skip("b3-bvbg028 template not available")
        descendants = graph.get_descendants("b3-bvbg028")
        assert "b3-equities-register" in descendants

    def test_source_node_topological_sort(self, graph: TemplateDependencyGraph):
        """Source nodes should return a single-element list."""
        download_templates = [
            tid
            for tid in graph.template_ids
            if graph.get_template_type(tid) == "download"
        ]
        if not download_templates:
            pytest.skip("No download templates available")
        tid = download_templates[0]
        order = graph.topological_sort(tid)
        assert order == [tid]


# ===================================================================
# Phase 3: Staleness Detection
# ===================================================================


# ===================================================================
# TEST: ExecutionStep dataclass
# ===================================================================


class TestExecutionStep:
    """Verify ExecutionStep dataclass behaviour."""

    def test_frozen(self):
        step = ExecutionStep(
            template_id="t1",
            action="process",
            reason="stale",
            template_type="download",
        )
        with pytest.raises(AttributeError):
            step.action = "skip"  # type: ignore[misc]

    def test_fields(self):
        step = ExecutionStep(
            template_id="t1",
            action="etl",
            reason="output missing",
            template_type="etl",
        )
        assert step.template_id == "t1"
        assert step.action == "etl"
        assert step.reason == "output missing"
        assert step.template_type == "etl"


# ===================================================================
# TEST: ExecutionPlan dataclass
# ===================================================================


class TestExecutionPlan:
    """Verify ExecutionPlan dataclass and properties."""

    def test_empty_plan(self):
        plan = ExecutionPlan(target_template="tgt")
        assert plan.steps == []
        assert plan.steps_to_execute == []
        assert plan.steps_to_skip == []

    def test_steps_to_execute_and_skip(self):
        plan = ExecutionPlan(
            target_template="tgt",
            steps=[
                ExecutionStep("a", "process", "stale", "download"),
                ExecutionStep("b", "skip", "fresh", "download"),
                ExecutionStep("c", "etl", "outdated", "etl"),
                ExecutionStep("tgt", "skip", "up to date", "etl"),
            ],
        )
        execute = plan.steps_to_execute
        skip = plan.steps_to_skip
        assert len(execute) == 2
        assert execute[0].template_id == "a"
        assert execute[1].template_id == "c"
        assert len(skip) == 2
        assert skip[0].template_id == "b"

    def test_str_output(self):
        plan = ExecutionPlan(
            target_template="my-etl",
            steps=[
                ExecutionStep("dl-src", "process", "unprocessed", "download"),
                ExecutionStep("my-etl", "etl", "output missing", "etl"),
            ],
        )
        text = str(plan)
        assert "my-etl" in text
        assert "PROCESS" in text
        assert "ETL" in text
        assert "2 to execute" in text
        assert "0 to skip" in text

    def test_str_with_skips(self):
        plan = ExecutionPlan(
            target_template="tgt",
            steps=[
                ExecutionStep("a", "skip", "fresh", "download"),
                ExecutionStep("tgt", "etl", "outdated", "etl"),
            ],
        )
        text = str(plan)
        assert "SKIP" in text
        assert "1 to execute" in text
        assert "1 to skip" in text


# ===================================================================
# TEST-012: Staleness detection for download templates (mocked cache)
# ===================================================================


class TestCheckDownloadTemplateStaleness:
    """Verify _check_download_template_staleness with mocked CacheManager."""

    def _make_graph(self):
        """Build a simple graph with one download template."""
        src = _make_download_template("dl-src")
        return _build_graph_from_templates([src])

    def test_no_cache_entries_means_not_stale(self):
        """No cache rows → nothing downloaded → not stale."""
        g = self._make_graph()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.meta_db_connection = mock_conn
            MockCacheManager.return_value = mock_cache

            assert g._check_download_template_staleness("dl-src") is False

    def test_empty_processed_files_means_stale(self):
        """Cache row with empty processed_files JSON → stale."""
        g = self._make_graph()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("{}",)]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.meta_db_connection = mock_conn
            MockCacheManager.return_value = mock_cache

            assert g._check_download_template_staleness("dl-src") is True

    def test_null_processed_files_means_stale(self):
        """Cache row with null/empty string → stale."""
        g = self._make_graph()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [("",)]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.meta_db_connection = mock_conn
            MockCacheManager.return_value = mock_cache

            assert g._check_download_template_staleness("dl-src") is True

    def test_populated_processed_files_means_not_stale(self):
        """Cache row with populated processed_files → not stale."""
        import json

        g = self._make_graph()
        processed = json.dumps({"data": "/path/to/file.parquet"})

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [(processed,)]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.meta_db_connection = mock_conn
            MockCacheManager.return_value = mock_cache

            assert g._check_download_template_staleness("dl-src") is False

    def test_mixed_rows_one_unprocessed_means_stale(self):
        """Multiple cache rows, one unprocessed → stale."""
        import json

        g = self._make_graph()
        processed = json.dumps({"data": "/path/to/file.parquet"})

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # First row is processed, second is not
        mock_cursor.fetchall.return_value = [(processed,), ("{}",)]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.meta_db_connection = mock_conn
            MockCacheManager.return_value = mock_cache

            assert g._check_download_template_staleness("dl-src") is True


# ===================================================================
# TEST: Staleness detection for ETL templates (mocked filesystem)
# ===================================================================


class TestCheckEtlTemplateStaleness:
    """Verify _check_etl_template_staleness with mocked filesystem."""

    def _make_graph(self):
        """Build a graph: dl-src → etl-mid."""
        src = _make_download_template("dl-src")
        mid = _make_etl_template("etl-mid", input_datasets=["dl-src"])
        return _build_graph_from_templates([src, mid])

    def test_no_output_dir_means_stale(self, tmp_path):
        """Missing output directory → stale."""
        g = self._make_graph()

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            # Point to a nonexistent directory
            mock_cache.db_path.return_value = str(tmp_path / "nonexistent")
            MockCacheManager.return_value = mock_cache

            assert g._check_etl_template_staleness("etl-mid") is True

    def test_empty_output_dir_means_stale(self, tmp_path):
        """Output directory exists but has no parquet files → stale."""
        g = self._make_graph()
        output_dir = tmp_path / "staging" / "etl-mid"
        output_dir.mkdir(parents=True)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.db_path.return_value = str(output_dir)
            MockCacheManager.return_value = mock_cache

            assert g._check_etl_template_staleness("etl-mid") is True

    def test_output_newer_than_upstream_means_fresh(self, tmp_path):
        """Output parquet newer than upstream → not stale."""
        import time

        g = self._make_graph()

        # Create upstream parquet
        upstream_dir = tmp_path / "input" / "dl-src"
        upstream_dir.mkdir(parents=True)
        upstream_pq = upstream_dir / "part-001.parquet"
        upstream_pq.write_text("upstream")

        time.sleep(0.05)

        # Create output parquet (newer)
        output_dir = tmp_path / "staging" / "etl-mid"
        output_dir.mkdir(parents=True)
        output_pq = output_dir / "part-001.parquet"
        output_pq.write_text("output")

        def mock_db_path(name):
            return str(tmp_path / name)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.db_path.side_effect = mock_db_path
            MockCacheManager.return_value = mock_cache

            assert g._check_etl_template_staleness("etl-mid") is False

    def test_upstream_newer_than_output_means_stale(self, tmp_path):
        """Upstream parquet newer than output → stale."""
        import time

        g = self._make_graph()

        # Create output parquet first (older)
        output_dir = tmp_path / "staging" / "etl-mid"
        output_dir.mkdir(parents=True)
        output_pq = output_dir / "part-001.parquet"
        output_pq.write_text("output")

        time.sleep(0.05)

        # Create upstream parquet (newer)
        upstream_dir = tmp_path / "input" / "dl-src"
        upstream_dir.mkdir(parents=True)
        upstream_pq = upstream_dir / "part-001.parquet"
        upstream_pq.write_text("upstream")

        def mock_db_path(name):
            return str(tmp_path / name)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.db_path.side_effect = mock_db_path
            MockCacheManager.return_value = mock_cache

            assert g._check_etl_template_staleness("etl-mid") is True

    def test_old_partitions_do_not_cause_false_staleness(self, tmp_path):
        """Newest output newer than upstream → fresh, even with old partitions."""
        import time

        g = self._make_graph()

        # Create upstream parquet
        upstream_dir = tmp_path / "input" / "dl-src"
        upstream_dir.mkdir(parents=True)
        upstream_pq = upstream_dir / "part-001.parquet"
        upstream_pq.write_text("upstream")

        time.sleep(0.05)

        # Create output partitions: one old, one new (newer than upstream)
        output_dir = tmp_path / "staging" / "etl-mid"
        output_dir.mkdir(parents=True)
        new_pq = output_dir / "part-new.parquet"
        new_pq.write_text("new output")

        old_pq = output_dir / "part-old.parquet"
        old_pq.write_text("old output")
        # Backdate the old partition to before the upstream
        old_time = upstream_pq.stat().st_mtime - 1000
        import os

        os.utime(str(old_pq), (old_time, old_time))

        def mock_db_path(name):
            return str(tmp_path / name)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.db_path.side_effect = mock_db_path
            MockCacheManager.return_value = mock_cache

            # Should be fresh: newest output > newest upstream
            assert g._check_etl_template_staleness("etl-mid") is False

    def test_no_upstream_parquets_means_fresh(self, tmp_path):
        """Output exists but upstream has no parquets → fresh."""
        g = self._make_graph()

        # Create output parquet
        output_dir = tmp_path / "staging" / "etl-mid"
        output_dir.mkdir(parents=True)
        (output_dir / "part-001.parquet").write_text("output")

        # Upstream dir exists but is empty
        upstream_dir = tmp_path / "input" / "dl-src"
        upstream_dir.mkdir(parents=True)

        def mock_db_path(name):
            return str(tmp_path / name)

        with patch("brasa.engine.dependency_graph.CacheManager") as MockCacheManager:
            mock_cache = MagicMock()
            mock_cache.db_path.side_effect = mock_db_path
            MockCacheManager.return_value = mock_cache

            assert g._check_etl_template_staleness("etl-mid") is False


# ===================================================================
# TEST-013: Execution plan — stale upstream
# ===================================================================


class TestExecutionPlanWithStaleUpstream:
    """Verify execution plan marks stale upstreams for processing."""

    def test_stale_download_triggers_process(self):
        """Download with unprocessed files → action='process'."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        g = _build_graph_from_templates([src, etl])

        with (
            patch.object(g, "_check_download_template_staleness", return_value=True),
            patch.object(g, "_check_etl_template_staleness", return_value=True),
        ):
            plan = g.get_execution_plan("my-etl")

        assert len(plan.steps) == 2
        # First step: download template is stale → process
        assert plan.steps[0].template_id == "dl-src"
        assert plan.steps[0].action == "process"
        assert plan.steps[0].template_type == "download"
        # Second step: etl template
        assert plan.steps[1].template_id == "my-etl"
        assert plan.steps[1].action == "etl"
        assert plan.steps[1].template_type == "etl"


# ===================================================================
# TEST-014: Execution plan — all fresh
# ===================================================================


class TestExecutionPlanAllFresh:
    """Verify execution plan with all upstreams fresh."""

    def test_all_fresh_only_target_etl(self):
        """All upstreams processed and output up to date → all skip."""
        src = _make_download_template("dl-src")
        etl = _make_etl_template("my-etl", input_datasets=["dl-src"])
        g = _build_graph_from_templates([src, etl])

        with (
            patch.object(g, "_check_download_template_staleness", return_value=False),
            patch.object(g, "_check_etl_template_staleness", return_value=False),
        ):
            plan = g.get_execution_plan("my-etl")

        assert len(plan.steps) == 2
        assert plan.steps[0].action == "skip"
        assert plan.steps[1].action == "skip"
        assert len(plan.steps_to_execute) == 0
        assert len(plan.steps_to_skip) == 2


# ===================================================================
# TEST-015: Execution plan — force mode
# ===================================================================


class TestExecutionPlanForceMode:
    """Verify force=True marks all ancestors for execution."""

    def test_force_all_executed(self):
        """force=True → all templates in chain marked for execution."""
        src = _make_download_template("dl-src")
        mid = _make_etl_template("etl-mid", input_datasets=["dl-src"])
        end = _make_etl_template("etl-end", input_datasets=["staging.etl-mid"])
        g = _build_graph_from_templates([src, mid, end])

        # Even though we don't mock staleness, force should override
        plan = g.get_execution_plan("etl-end", force=True)

        assert len(plan.steps) == 3
        assert all(s.action != "skip" for s in plan.steps)

        # Check ordering and correct action types
        assert plan.steps[0].template_id == "dl-src"
        assert plan.steps[0].action == "process"
        assert plan.steps[0].reason == "forced execution"

        assert plan.steps[1].template_id == "etl-mid"
        assert plan.steps[1].action == "etl"

        assert plan.steps[2].template_id == "etl-end"
        assert plan.steps[2].action == "etl"

    def test_force_single_download(self):
        """force=True on a source node → process."""
        src = _make_download_template("dl-src")
        g = _build_graph_from_templates([src])

        plan = g.get_execution_plan("dl-src", force=True)

        assert len(plan.steps) == 1
        assert plan.steps[0].action == "process"
        assert plan.steps[0].template_type == "download"


# ===================================================================
# TEST: Execution plan — mixed stale/fresh ancestors
# ===================================================================


class TestExecutionPlanMixed:
    """Verify execution plan with some stale and some fresh ancestors."""

    def test_mixed_staleness(self):
        """Diamond DAG with one stale branch and one fresh."""
        base = _make_download_template("base")
        branch_a = _make_etl_template("branch-a", input_datasets=["base"])
        branch_b = _make_etl_template("branch-b", input_datasets=["base"])
        merge = _make_etl_template(
            "merge",
            input_datasets=["staging.branch-a", "staging.branch-b"],
        )
        g = _build_graph_from_templates([base, branch_a, branch_b, merge])

        def mock_download_staleness(tid):
            return False  # base is fresh

        def mock_etl_staleness(tid):
            # branch-a is stale, branch-b is fresh
            return tid in ("branch-a", "merge")

        with (
            patch.object(
                g,
                "_check_download_template_staleness",
                side_effect=mock_download_staleness,
            ),
            patch.object(
                g,
                "_check_etl_template_staleness",
                side_effect=mock_etl_staleness,
            ),
        ):
            plan = g.get_execution_plan("merge")

        step_map = {s.template_id: s for s in plan.steps}

        assert step_map["base"].action == "skip"
        assert step_map["branch-a"].action == "etl"
        assert step_map["branch-b"].action == "skip"
        assert step_map["merge"].action == "etl"

        assert len(plan.steps_to_execute) == 2
        assert len(plan.steps_to_skip) == 2


# ===================================================================
# TEST: Execution plan — unknown template raises
# ===================================================================


class TestExecutionPlanErrors:
    """Verify error cases for get_execution_plan."""

    def test_unknown_template_raises(self):
        src = _make_download_template("dl-src")
        g = _build_graph_from_templates([src])

        with pytest.raises(KeyError, match="not in the dependency graph"):
            g.get_execution_plan("nonexistent")


# ===================================================================
# Download template dependencies: block in dependency graph
# ===================================================================


class TestDiscoverDependenciesDownloadTemplate:
    """Verify _discover_dependencies parses the ``dependencies:`` block of download templates."""

    def test_no_deps_attr_returns_empty(self):
        """Download template with no ``dependencies`` attribute → []."""
        tmpl = _make_download_template("dl-no-deps", dependencies=None)
        assert TemplateDependencyGraph._discover_dependencies(tmpl) == []

    def test_with_one_dep_entry(self):
        """Download template with one dependency entry returns the dataset refs."""
        raw_deps = [
            {
                "index": {
                    "required": True,
                    "from": {"datasets": ["staging.b3-indexes-composition"]},
                }
            }
        ]
        tmpl = _make_download_template("dl-with-deps", dependencies=raw_deps)
        result = TemplateDependencyGraph._discover_dependencies(tmpl)
        assert result == ["staging.b3-indexes-composition"]

    def test_with_multiple_dep_entries_and_datasets(self):
        """Multiple dep entries / multiple datasets per entry → all refs returned."""
        raw_deps = [
            {
                "param_a": {
                    "required": True,
                    "from": {"datasets": ["staging.upstream-a", "input.upstream-b"]},
                }
            },
            {
                "param_b": {
                    "required": False,
                    "from": {"datasets": ["staging.upstream-c"]},
                }
            },
        ]
        tmpl = _make_download_template("dl-multi-deps", dependencies=raw_deps)
        result = TemplateDependencyGraph._discover_dependencies(tmpl)
        assert set(result) == {
            "staging.upstream-a",
            "input.upstream-b",
            "staging.upstream-c",
        }


class TestDownloadTemplateDepsInGraph:
    """Verify graph edges, ancestors, and downstream are correct for download templates with deps."""

    def _build(self) -> TemplateDependencyGraph:
        """Build: etl-upstream → (staging) → dl-with-deps."""
        upstream_etl = _make_etl_template(
            "etl-upstream",
            input_datasets=[],
            writer_layer="staging",
            writer_dataset="etl-upstream",
        )
        raw_deps = [
            {
                "index": {
                    "required": True,
                    "from": {"datasets": ["staging.etl-upstream"]},
                }
            }
        ]
        dl = _make_download_template("dl-with-deps", dependencies=raw_deps)
        return _build_graph_from_templates([upstream_etl, dl])

    def test_edges_include_download_template_deps(self):
        """Edge from dl-with-deps to etl-upstream appears in graph.edges."""
        g = self._build()
        assert "etl-upstream" in g.edges["dl-with-deps"]

    def test_get_ancestors_download_template(self):
        """get_ancestors() returns the upstream ETL for a download template with deps."""
        g = self._build()
        assert g.get_ancestors("dl-with-deps") == {"etl-upstream"}

    def test_get_downstream_etl_visible_from_download_dep(self):
        """get_downstream() on the upstream ETL returns the download template."""
        g = self._build()
        assert "dl-with-deps" in g.get_downstream("etl-upstream")


class TestIntegrationDownloadTemplateDeps:
    """Integration tests for real templates with dependencies: blocks."""

    @pytest.fixture(scope="class")
    def graph(self) -> TemplateDependencyGraph:
        return TemplateDependencyGraph()

    def test_b3_indexes_theoretical_portfolio_has_ancestor(
        self, graph: TemplateDependencyGraph
    ):
        """b3-indexes-theoretical-portfolio declares a dep on b3-indexes-composition."""
        if "b3-indexes-theoretical-portfolio" not in graph:
            pytest.skip("b3-indexes-theoretical-portfolio template not available")
        ancestors = graph.get_ancestors("b3-indexes-theoretical-portfolio")
        assert any("b3-indexes-composition" in a for a in ancestors), (
            f"Expected b3-indexes-composition in ancestors, got {ancestors}"
        )

    def test_b3_indexes_composition_consolidated_has_downstream(
        self, graph: TemplateDependencyGraph
    ):
        """get_downstream() on the upstream ETL returns b3-indexes-theoretical-portfolio."""
        upstream = next(
            (
                tid
                for tid in graph.template_ids
                if "b3-indexes-composition" in tid
                and tid != "b3-indexes-theoretical-portfolio"
            ),
            None,
        )
        if upstream is None:
            pytest.skip("b3-indexes-composition* template not available")
        downstream = graph.get_descendants(upstream)
        assert "b3-indexes-theoretical-portfolio" in downstream, (
            f"Expected b3-indexes-theoretical-portfolio in descendants of {upstream}, "
            f"got {downstream}"
        )

    def test_execution_plan_includes_composition_consolidated(
        self, graph: TemplateDependencyGraph
    ):
        """Execution plan for b3-indexes-theoretical-portfolio must include
        b3-indexes-composition-consolidated before it.

        This simulates the ``indexes-b3`` download plan, which lists
        b3-indexes-theoretical-portfolio as a task.  Because that template
        declares a dependency on ``staging.b3-indexes-composition`` — produced
        by b3-indexes-composition-consolidated — the dependency graph must
        schedule b3-indexes-composition-consolidated before
        b3-indexes-theoretical-portfolio.
        """
        target = "b3-indexes-theoretical-portfolio"
        consolidated = "b3-indexes-composition-consolidated"

        if target not in graph:
            pytest.skip(f"{target} template not available")
        if consolidated not in graph:
            pytest.skip(f"{consolidated} template not available")

        with (
            patch.object(
                graph, "_check_download_template_staleness", return_value=True
            ),
            patch.object(graph, "_check_etl_template_staleness", return_value=True),
        ):
            plan = graph.get_execution_plan(target)

        template_ids_in_plan = [s.template_id for s in plan.steps]

        assert consolidated in template_ids_in_plan, (
            f"{consolidated} missing from execution plan steps: {template_ids_in_plan}"
        )

        idx_consolidated = template_ids_in_plan.index(consolidated)
        idx_target = template_ids_in_plan.index(target)
        assert idx_consolidated < idx_target, (
            f"{consolidated} (step {idx_consolidated}) must come before "
            f"{target} (step {idx_target})"
        )

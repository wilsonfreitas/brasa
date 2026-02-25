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
    DatasetOutput,
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
) -> MarketDataTemplate:
    """Build a minimal download-type template mock.

    Args:
        template_id: The template id.
        datasets: If provided, creates a multi-output template.
        writer_layer: Writer layer string.
        has_pipeline: Whether the reader uses a pipeline.

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

    def test_download_templates_are_source_nodes(self, graph: TemplateDependencyGraph):
        """Download templates should have no upstream dependencies."""
        for tid in graph.template_ids:
            if graph.get_template_type(tid) == "download":
                assert (
                    graph.get_upstream(tid) == []
                ), f"Download template '{tid}' should be a source node"

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

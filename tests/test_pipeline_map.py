"""Unit tests for the pipeline map (brasa map) — data and renderers."""

from __future__ import annotations

import itertools
import json
from contextlib import closing
from unittest.mock import patch

import pytest

from brasa.engine import CacheManager
from tests.test_dependency_graph import (
    _build_graph_from_templates,
    _make_download_template,
    _make_etl_template,
)

_ROW_COUNTER = itertools.count()


def _insert_meta_row(template_id: str, processed: bool) -> None:
    """Insert a synthetic cache_metadata row for tests."""
    cache = CacheManager()
    processed_files = json.dumps({"f.parquet": "abc"}) if processed else "{}"
    seq = next(_ROW_COUNTER)
    row_id = f"{template_id}-{seq}"
    checksum = f"chk-{seq}"
    with closing(cache.meta_db_connection) as conn, conn:
        c = conn.cursor()
        c.execute(
            "INSERT INTO cache_metadata "
            "(id, download_checksum, timestamp, response, download_args, "
            " template, downloaded_files, processed_files, extra_key, "
            " processing_errors) "
            "VALUES (?, ?, '2026-01-01T00:00:00', '{}', '{}', ?, '[]', ?, '', '')",
            (row_id, checksum, template_id, processed_files),
        )


class TestGetDownloadStatus:
    def test_never_run_when_no_rows(self):
        tmpl = _make_download_template("foo")
        graph = _build_graph_from_templates([tmpl])
        status, reason = graph.get_download_status("foo")
        assert status == "never-run"
        assert reason == "no downloads found"

    def test_ok_when_all_processed(self):
        tmpl = _make_download_template("foo")
        graph = _build_graph_from_templates([tmpl])
        _insert_meta_row("foo", processed=True)
        status, reason = graph.get_download_status("foo")
        assert status == "ok"
        assert reason == ""

    def test_stale_when_some_unprocessed(self):
        tmpl = _make_download_template("foo")
        graph = _build_graph_from_templates([tmpl])
        _insert_meta_row("foo", processed=True)
        _insert_meta_row("foo", processed=False)
        status, reason = graph.get_download_status("foo")
        assert status == "stale"
        assert "1 unprocessed" in reason

    def test_pluralisation(self):
        tmpl = _make_download_template("foo")
        graph = _build_graph_from_templates([tmpl])
        _insert_meta_row("foo", processed=False)
        _insert_meta_row("foo", processed=False)
        status, reason = graph.get_download_status("foo")
        assert reason == "2 unprocessed entries"

    def test_unknown_template_raises(self):
        tmpl = _make_download_template("foo")
        graph = _build_graph_from_templates([tmpl])
        with pytest.raises(KeyError):
            graph.get_download_status("missing")


def _tmp_path_db(cache, dataset_id):
    """Return a Path object for a dataset under the test cache."""
    from pathlib import Path

    return Path(cache.db_path(dataset_id))


class TestGetEtlStatus:
    def test_never_run_when_output_dir_missing(self):
        upstream = _make_download_template("up")
        etl = _make_etl_template("dn", input_datasets=["input.up"])
        graph = _build_graph_from_templates([upstream, etl])
        status, reason = graph.get_etl_status("dn")
        assert status == "never-run"
        assert "never produced" in reason

    def test_never_run_when_no_parquet_files(self):
        upstream = _make_download_template("up")
        etl = _make_etl_template("dn", input_datasets=["input.up"])
        graph = _build_graph_from_templates([upstream, etl])
        cache = CacheManager()
        out = _tmp_path_db(cache, "staging/dn")
        out.mkdir(parents=True, exist_ok=True)
        status, reason = graph.get_etl_status("dn")
        assert status == "never-run"

    def test_stale_when_upstream_newer(self):
        import time

        upstream = _make_download_template("up")
        etl = _make_etl_template("dn", input_datasets=["input.up"])
        graph = _build_graph_from_templates([upstream, etl])
        cache = CacheManager()
        out = _tmp_path_db(cache, "staging/dn")
        out.mkdir(parents=True, exist_ok=True)
        (out / "data.parquet").write_text("x")
        time.sleep(0.05)
        up_dir = _tmp_path_db(cache, "input/up")
        up_dir.mkdir(parents=True, exist_ok=True)
        (up_dir / "data.parquet").write_text("y")
        status, reason = graph.get_etl_status("dn")
        assert status == "stale"
        assert "upstream 'up' newer" in reason

    def test_ok_when_output_newer_than_upstream(self):
        import time

        upstream = _make_download_template("up")
        etl = _make_etl_template("dn", input_datasets=["input.up"])
        graph = _build_graph_from_templates([upstream, etl])
        cache = CacheManager()
        up_dir = _tmp_path_db(cache, "input/up")
        up_dir.mkdir(parents=True, exist_ok=True)
        (up_dir / "data.parquet").write_text("y")
        time.sleep(0.05)
        out = _tmp_path_db(cache, "staging/dn")
        out.mkdir(parents=True, exist_ok=True)
        (out / "data.parquet").write_text("x")
        status, reason = graph.get_etl_status("dn")
        assert status == "ok"
        assert reason == ""

    def test_unknown_template_raises(self):
        upstream = _make_download_template("up")
        etl = _make_etl_template("dn", input_datasets=["input.up"])
        graph = _build_graph_from_templates([upstream, etl])
        with pytest.raises(KeyError):
            graph.get_etl_status("missing")


from brasa.engine.pipeline_map import TemplateStatus, build_pipeline_map  # noqa: E402


class TestBuildPipelineMap:
    def test_empty_graph(self):
        with patch("brasa.engine.pipeline_map.TemplateDependencyGraph") as mocked:
            inst = mocked.return_value
            inst.template_ids = []
            inst.templates = {}
            inst.edges = {}
            assert build_pipeline_map() == []

    def test_topological_order_simple_chain(self):
        up = _make_download_template("up")
        dn = _make_etl_template("dn", input_datasets=["input.up"])
        graph = _build_graph_from_templates([up, dn])
        with patch(
            "brasa.engine.pipeline_map.TemplateDependencyGraph",
            return_value=graph,
        ):
            items = build_pipeline_map(include_ok=True)
        ids = [it.template_id for it in items]
        assert ids == ["up", "dn"]

    def test_filters_ok_by_default(self):
        up = _make_download_template("up")
        dn = _make_etl_template("dn", input_datasets=["input.up"])
        graph = _build_graph_from_templates([up, dn])
        with (
            patch.object(graph, "get_download_status", return_value=("ok", "")),
            patch.object(graph, "get_etl_status", return_value=("ok", "")),
            patch(
                "brasa.engine.pipeline_map.TemplateDependencyGraph",
                return_value=graph,
            ),
        ):
            assert build_pipeline_map() == []
            full = build_pipeline_map(include_ok=True)
        assert [s.status for s in full] == ["ok", "ok"]

    def test_template_type_set_correctly(self):
        up = _make_download_template("up")
        dn = _make_etl_template("dn", input_datasets=["input.up"])
        graph = _build_graph_from_templates([up, dn])
        with (
            patch.object(graph, "get_download_status", return_value=("stale", "x")),
            patch.object(graph, "get_etl_status", return_value=("stale", "y")),
            patch(
                "brasa.engine.pipeline_map.TemplateDependencyGraph",
                return_value=graph,
            ),
        ):
            items = build_pipeline_map()
        types = {it.template_id: it.template_type for it in items}
        assert types == {"up": "download", "dn": "etl"}


from io import StringIO  # noqa: E402

from rich.console import Console  # noqa: E402

from brasa.engine.pipeline_map import (  # noqa: E402
    render_flat,
    render_grouped,
    render_tree,
)


def _capture(renderer, *args, **kwargs):
    buf = StringIO()
    console = Console(file=buf, force_terminal=False, width=120, no_color=True)
    renderer(*args, console=console, **kwargs)
    return buf.getvalue()


class TestRenderFlat:
    def test_empty_prints_all_clear(self):
        output = _capture(render_flat, [])
        assert "All up to date" in output

    def test_single_stale_download(self):
        items = [
            TemplateStatus(
                template_id="b3-bvbg028",
                template_type="download",
                status="stale",
                reason="12 unprocessed entries",
            )
        ]
        output = _capture(render_flat, items)
        assert "1." in output
        assert "[download]" in output
        assert "b3-bvbg028" in output
        assert "stale" in output
        assert "12 unprocessed entries" in output

    def test_multiple_items_numbered(self):
        items = [
            TemplateStatus("a", "download", "stale", "x"),
            TemplateStatus("b", "etl", "stale", "y"),
            TemplateStatus("c", "etl", "never-run", "z"),
        ]
        output = _capture(render_flat, items)
        assert "1." in output and "2." in output and "3." in output
        assert output.index("1.") < output.index("2.") < output.index("3.")


class TestRenderGrouped:
    def test_empty_prints_all_clear(self):
        output = _capture(render_grouped, [], graph=None)
        assert "All up to date" in output

    def test_groups_by_stage(self):
        up = _make_download_template("dl1")
        st = _make_etl_template(
            "st1", input_datasets=["input.dl1"], writer_layer="staging"
        )
        cu = _make_etl_template(
            "cu1", input_datasets=["staging.st1"], writer_layer="curated"
        )
        graph = _build_graph_from_templates([up, st, cu])
        items = [
            TemplateStatus("dl1", "download", "stale", "1 unprocessed entry"),
            TemplateStatus("st1", "etl", "stale", "upstream 'dl1' newer"),
            TemplateStatus("cu1", "etl", "never-run", "output never produced"),
        ]
        output = _capture(render_grouped, items, graph=graph)
        assert "Downloads to process" in output
        assert "Staging ETLs" in output
        assert "Curated ETLs" in output
        assert output.index("Downloads to process") < output.index("Staging ETLs")
        assert output.index("Staging ETLs") < output.index("Curated ETLs")


class TestRenderTree:
    def test_empty_prints_all_clear(self):
        output = _capture(render_tree, [], graph=None)
        assert "All up to date" in output

    def test_forward_roots_at_sources(self):
        up = _make_download_template("dl1")
        st = _make_etl_template(
            "st1", input_datasets=["input.dl1"], writer_layer="staging"
        )
        graph = _build_graph_from_templates([up, st])
        items = [
            TemplateStatus("dl1", "download", "stale", "1 unprocessed entry"),
            TemplateStatus("st1", "etl", "stale", "upstream 'dl1' newer"),
        ]
        output = _capture(render_tree, items, graph=graph, reverse=False)
        assert output.index("dl1") < output.index("st1")
        assert "  st1" in output or "└" in output or "├" in output

    def test_reverse_roots_at_leaves(self):
        up = _make_download_template("dl1")
        st = _make_etl_template(
            "st1", input_datasets=["input.dl1"], writer_layer="staging"
        )
        graph = _build_graph_from_templates([up, st])
        items = [
            TemplateStatus("dl1", "download", "stale", "1 unprocessed entry"),
            TemplateStatus("st1", "etl", "stale", "upstream 'dl1' newer"),
        ]
        output = _capture(render_tree, items, graph=graph, reverse=True)
        assert output.index("st1") < output.index("dl1")

    def test_skips_subtrees_with_no_stale(self):
        up = _make_download_template("dl1")
        st = _make_etl_template(
            "st1", input_datasets=["input.dl1"], writer_layer="staging"
        )
        graph = _build_graph_from_templates([up, st])
        items = [
            TemplateStatus("st1", "etl", "stale", "upstream is newer"),
        ]
        output = _capture(render_tree, items, graph=graph, reverse=False)
        assert "dl1" not in output
        assert "st1" in output

"""Unit tests for the pipeline map (brasa map) — data and renderers."""

from __future__ import annotations

import itertools
import json
from contextlib import closing

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

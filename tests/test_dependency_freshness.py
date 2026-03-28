"""Tests for dependency freshness tracking."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

from brasa.engine.dependency_resolver import (
    _is_output_fresh,
    _touch_marker,
)
from tests.test_dependency_graph import (
    _build_graph_from_templates,
    _make_download_template,
    _make_etl_template,
)

MARKER_NAME = ".last_processed"


class TestTouchMarker:
    """Tests for _touch_marker helper."""

    def test_creates_marker_in_dataset_folder(self, tmp_path):
        dataset_dir = tmp_path / "db" / "staging" / "my-dataset"
        dataset_dir.mkdir(parents=True)
        _touch_marker(str(dataset_dir))
        marker = dataset_dir / MARKER_NAME
        assert marker.exists()

    def test_updates_marker_mtime(self, tmp_path):
        dataset_dir = tmp_path / "db" / "staging" / "my-dataset"
        dataset_dir.mkdir(parents=True)
        _touch_marker(str(dataset_dir))
        marker = dataset_dir / MARKER_NAME
        first_mtime = marker.stat().st_mtime
        time.sleep(0.05)
        _touch_marker(str(dataset_dir))
        second_mtime = marker.stat().st_mtime
        assert second_mtime > first_mtime

    def test_noop_when_directory_missing(self, tmp_path):
        missing_dir = tmp_path / "does-not-exist"
        # Should not raise
        _touch_marker(str(missing_dir))


class TestIsOutputFresh:
    """Tests for _is_output_fresh helper."""

    def test_stale_when_no_marker(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        assert _is_output_fresh(str(output_dir), [str(input_dir)]) is False

    def test_fresh_when_marker_newer_than_inputs(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data.parquet").write_text("data")
        time.sleep(0.05)
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _touch_marker(str(output_dir))
        assert _is_output_fresh(str(output_dir), [str(input_dir)]) is True

    def test_stale_when_input_newer_than_marker(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _touch_marker(str(output_dir))
        time.sleep(0.05)
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        (input_dir / "data.parquet").write_text("data")
        assert _is_output_fresh(str(output_dir), [str(input_dir)]) is False

    def test_stale_when_output_dir_missing(self, tmp_path):
        input_dir = tmp_path / "input"
        input_dir.mkdir()
        missing = tmp_path / "missing"
        assert _is_output_fresh(str(missing), [str(input_dir)]) is False

    def test_fresh_with_no_inputs(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _touch_marker(str(output_dir))
        assert _is_output_fresh(str(output_dir), []) is True

    def test_stale_with_no_inputs_and_no_marker(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        assert _is_output_fresh(str(output_dir), []) is False

    def test_multiple_inputs_stale_if_any_newer(self, tmp_path):
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        _touch_marker(str(output_dir))
        time.sleep(0.05)
        # One input older, one newer
        old_input = tmp_path / "old_input"
        old_input.mkdir()
        new_input = tmp_path / "new_input"
        new_input.mkdir()
        (new_input / "data.parquet").write_text("new")
        assert (
            _is_output_fresh(str(output_dir), [str(old_input), str(new_input)]) is False
        )


class TestGraphDatasetPaths:
    """Tests for TemplateDependencyGraph.get_dataset_paths."""

    def test_returns_output_paths(self, tmp_path):
        dl = _make_download_template("b3-raw")
        etl = _make_etl_template(
            "b3-etl",
            ["input/b3-raw"],
            writer_layer="staging",
            writer_dataset="b3-processed",
        )
        graph = _build_graph_from_templates([dl, etl])
        with patch("brasa.engine.dependency_graph.CacheManager") as MockCM:
            instance = MockCM.return_value
            instance.db_path.side_effect = lambda name: str(tmp_path / name)
            output_paths = graph.get_dataset_paths("b3-etl")
        assert len(output_paths) == 1
        assert "staging/b3-processed" in output_paths[0]

    def test_returns_input_paths(self, tmp_path):
        dl = _make_download_template("b3-raw")
        etl = _make_etl_template(
            "b3-etl",
            ["input/b3-raw"],
            writer_layer="staging",
            writer_dataset="b3-processed",
        )
        graph = _build_graph_from_templates([dl, etl])
        with patch("brasa.engine.dependency_graph.CacheManager") as MockCM:
            instance = MockCM.return_value
            instance.db_path.side_effect = lambda name: str(tmp_path / name)
            input_paths = graph.get_input_dataset_paths("b3-etl")
        assert len(input_paths) == 1
        assert "input/b3-raw" in input_paths[0]


class TestRunUpstreamSkipsFresh:
    """Tests that _run_upstream_templates skips fresh upstream templates."""

    @patch("brasa.engine.dependency_resolver._is_output_fresh", return_value=True)
    def test_skips_when_output_fresh(self, mock_fresh):
        from brasa.engine.dependency_resolver import _run_upstream_templates

        graph = MagicMock()
        graph.get_producer.return_value = "b3-indexes-consolidated"
        graph.get_template_type.return_value = "etl"
        graph.get_dataset_paths.return_value = ["/cache/db/staging/b3-idx"]
        graph.get_input_dataset_paths.return_value = ["/cache/db/input/b3-raw"]

        with (
            patch("brasa.engine.api.process_etl") as mock_etl,
            patch("brasa.engine.api.process_marketdata") as mock_pm,
        ):
            _run_upstream_templates(
                "consumer-template",
                "index",
                ["staging.b3-indexes-composition"],
                graph,
                required=True,
            )
            mock_etl.assert_not_called()
            mock_pm.assert_not_called()

    @patch("brasa.engine.dependency_resolver._is_output_fresh", return_value=False)
    def test_runs_when_output_stale(self, mock_fresh):
        from brasa.engine.dependency_resolver import _run_upstream_templates

        graph = MagicMock()
        graph.get_producer.return_value = "b3-indexes-consolidated"
        graph.get_template_type.return_value = "etl"
        graph.get_dataset_paths.return_value = ["/cache/db/staging/b3-idx"]
        graph.get_input_dataset_paths.return_value = ["/cache/db/input/b3-raw"]

        mock_report = MagicMock()
        mock_report.success = True

        with patch(
            "brasa.engine.api.process_etl", return_value=mock_report
        ) as mock_etl:
            _run_upstream_templates(
                "consumer-template",
                "index",
                ["staging.b3-indexes-composition"],
                graph,
                required=True,
            )
            mock_etl.assert_called_once_with(
                "b3-indexes-consolidated", resolve_dependencies=True
            )

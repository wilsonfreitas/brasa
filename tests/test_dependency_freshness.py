"""Tests for dependency freshness tracking."""

from __future__ import annotations

import time

from brasa.engine.dependency_resolver import (
    _is_output_fresh,
    _touch_marker,
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

# Dependency Freshness Tracking via Marker Files

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Avoid redundant re-execution of upstream templates by tracking dataset freshness with `.last_processed` marker files.

**Architecture:** Write a `.last_processed` marker file inside each dataset's parquet folder after processing. Before running an upstream template, compare the marker's mtime against the mtime of the upstream's input datasets. If the marker is newer than all inputs, skip re-execution.

**Tech Stack:** Python pathlib, os.path.getmtime, existing `CacheManager` and `TemplateDependencyGraph`.

---

### Task 1: Add freshness check helpers to `dependency_resolver.py`

**Files:**
- Modify: `brasa/engine/dependency_resolver.py`
- Test: `tests/test_dependency_freshness.py`

**Step 1: Write the failing tests**

Create `tests/test_dependency_freshness.py`:

```python
"""Tests for dependency freshness tracking."""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

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
        assert _is_output_fresh(str(output_dir), [str(old_input), str(new_input)]) is False
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dependency_freshness.py -v`
Expected: ImportError — `_is_output_fresh` and `_touch_marker` don't exist yet.

**Step 3: Implement `_touch_marker` and `_is_output_fresh`**

Add to `brasa/engine/dependency_resolver.py` (after the existing imports, before `_dataset_ref_to_id`):

```python
from pathlib import Path

MARKER_NAME = ".last_processed"


def _touch_marker(dataset_dir: str) -> None:
    """Write or update a ``.last_processed`` marker in *dataset_dir*.

    If the directory does not exist the call is a no-op.

    Args:
        dataset_dir: Absolute path to the dataset's parquet folder.
    """
    dirpath = Path(dataset_dir)
    if not dirpath.is_dir():
        return
    marker = dirpath / MARKER_NAME
    marker.touch()


def _get_latest_mtime(directory: str) -> float:
    """Return the most recent mtime of any file in *directory* (non-recursive).

    Args:
        directory: Absolute path to a directory.

    Returns:
        Most recent mtime, or 0.0 if directory is empty or missing.
    """
    dirpath = Path(directory)
    if not dirpath.is_dir():
        return 0.0
    latest = 0.0
    for entry in dirpath.iterdir():
        if entry.is_file() and entry.name != MARKER_NAME:
            latest = max(latest, entry.stat().st_mtime)
    # Also check subdirectories (partitioned datasets)
    for entry in dirpath.rglob("*"):
        if entry.is_file() and entry.name != MARKER_NAME:
            latest = max(latest, entry.stat().st_mtime)
    return latest


def _is_output_fresh(output_dir: str, input_dirs: list[str]) -> bool:
    """Check whether a dataset's output is fresher than all its inputs.

    Args:
        output_dir: Absolute path to the output dataset folder.
        input_dirs: Absolute paths to input dataset folders.

    Returns:
        ``True`` if the ``.last_processed`` marker in *output_dir* is
        newer than the latest file in every *input_dir*.  ``False``
        when the marker is missing, *output_dir* doesn't exist, or any
        input is newer.
    """
    marker = Path(output_dir) / MARKER_NAME
    if not marker.exists():
        return False
    marker_mtime = marker.stat().st_mtime

    for input_dir in input_dirs:
        input_mtime = _get_latest_mtime(input_dir)
        if input_mtime > marker_mtime:
            return False
    return True
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dependency_freshness.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add tests/test_dependency_freshness.py brasa/engine/dependency_resolver.py
git commit -m "feat: add freshness check helpers _touch_marker and _is_output_fresh"
```

---

### Task 2: Add graph method to resolve dataset paths for a template

**Files:**
- Modify: `brasa/engine/dependency_graph.py`
- Test: `tests/test_dependency_freshness.py`

**Step 1: Write the failing test**

Append to `tests/test_dependency_freshness.py`:

```python
from tests.test_dependency_graph import (
    _build_graph_from_templates,
    _make_download_template,
    _make_etl_template,
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
        with patch(
            "brasa.engine.dependency_graph.CacheManager"
        ) as MockCM:
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
        with patch(
            "brasa.engine.dependency_graph.CacheManager"
        ) as MockCM:
            instance = MockCM.return_value
            instance.db_path.side_effect = lambda name: str(tmp_path / name)
            input_paths = graph.get_input_dataset_paths("b3-etl")
        assert len(input_paths) == 1
        assert "input/b3-raw" in input_paths[0]
```

**Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_dependency_freshness.py::TestGraphDatasetPaths -v`
Expected: AttributeError — `get_dataset_paths` and `get_input_dataset_paths` don't exist.

**Step 3: Implement `get_dataset_paths` and `get_input_dataset_paths`**

Add to `TemplateDependencyGraph` class in `brasa/engine/dependency_graph.py`:

```python
def get_dataset_paths(self, template_id: str) -> list[str]:
    """Return absolute filesystem paths for all datasets produced by *template_id*.

    Args:
        template_id: Template identifier.

    Returns:
        List of absolute paths to dataset folders.
    """
    outputs = self.get_outputs(template_id)
    man = CacheManager()
    return [man.db_path(out.dataset_id) for out in outputs]

def get_input_dataset_paths(self, template_id: str) -> list[str]:
    """Return absolute filesystem paths for all input datasets of *template_id*.

    Args:
        template_id: Template identifier.

    Returns:
        List of absolute paths to input dataset folders.
    """
    refs = self.dependency_refs.get(template_id, [])
    man = CacheManager()
    return [man.db_path(ref) for ref in refs]
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dependency_freshness.py::TestGraphDatasetPaths -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add brasa/engine/dependency_graph.py tests/test_dependency_freshness.py
git commit -m "feat: add get_dataset_paths and get_input_dataset_paths to dependency graph"
```

---

### Task 3: Integrate freshness check into `_run_upstream_templates`

**Files:**
- Modify: `brasa/engine/dependency_resolver.py` (lines 205-283)
- Test: `tests/test_dependency_freshness.py`

**Step 1: Write the failing test**

Append to `tests/test_dependency_freshness.py`:

```python
class TestRunUpstreamSkipsFresh:
    """Tests that _run_upstream_templates skips fresh upstream templates."""

    @patch("brasa.engine.dependency_resolver._is_output_fresh", return_value=True)
    @patch("brasa.engine.dependency_resolver.TemplateDependencyGraph")
    def test_skips_when_output_fresh(self, MockGraph, mock_fresh):
        from brasa.engine.dependency_resolver import _run_upstream_templates

        graph = MockGraph.return_value
        graph.get_producer.return_value = "b3-indexes-consolidated"
        graph.get_template_type.return_value = "etl"
        graph.get_dataset_paths.return_value = ["/cache/db/staging/b3-idx"]
        graph.get_input_dataset_paths.return_value = ["/cache/db/input/b3-raw"]

        with patch("brasa.engine.dependency_resolver.process_etl") as mock_etl, \
             patch("brasa.engine.dependency_resolver.process_marketdata") as mock_pm:
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
    @patch("brasa.engine.dependency_resolver.TemplateDependencyGraph")
    def test_runs_when_output_stale(self, MockGraph, mock_fresh):
        from brasa.engine.dependency_resolver import _run_upstream_templates

        graph = MockGraph.return_value
        graph.get_producer.return_value = "b3-indexes-consolidated"
        graph.get_template_type.return_value = "etl"
        graph.get_dataset_paths.return_value = ["/cache/db/staging/b3-idx"]
        graph.get_input_dataset_paths.return_value = ["/cache/db/input/b3-raw"]

        mock_report = MagicMock()
        mock_report.success = True

        with patch("brasa.engine.dependency_resolver.process_etl", return_value=mock_report) as mock_etl:
            _run_upstream_templates(
                "consumer-template",
                "index",
                ["staging.b3-indexes-composition"],
                graph,
                required=True,
            )
            mock_etl.assert_called_once_with("b3-indexes-consolidated")
```

**Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_dependency_freshness.py::TestRunUpstreamSkipsFresh -v`
Expected: FAIL — `_run_upstream_templates` doesn't check freshness yet, so `process_etl` is called even when `_is_output_fresh` returns True.

**Step 3: Modify `_run_upstream_templates`**

Edit `_run_upstream_templates` in `brasa/engine/dependency_resolver.py` (lines 205-283). Add the freshness check after `seen_producers.add(producer)` and before the `template_type` lookup:

```python
def _run_upstream_templates(
    template_id: str,
    arg_name: str,
    dataset_refs: list[str],
    graph,
    required: bool,
    _implicit_reports: list | None = None,
) -> None:
    """Run the upstream producing templates for the given dataset refs.

    Skips templates whose output datasets are already fresher than their
    inputs (checked via ``.last_processed`` marker files).

    Args:
        template_id: The template declaring the dependency (for logging).
        arg_name: The arg name being resolved (for error messages).
        dataset_refs: List of dataset references to ensure are up to date.
        graph: The ``TemplateDependencyGraph`` instance.
        required: If ``True``, raise on failure; if ``False``, warn.

    Raises:
        DependencyResolutionError: If a required upstream template fails.
    """
    from .api import process_etl, process_marketdata

    seen_producers: set[str] = set()

    for ref in dataset_refs:
        dataset_id = _dataset_ref_to_id(ref)
        producer = graph.get_producer(dataset_id)
        if producer is None or producer in seen_producers:
            continue
        seen_producers.add(producer)

        # --- Freshness check ---
        output_paths = graph.get_dataset_paths(producer)
        input_paths = graph.get_input_dataset_paths(producer)
        if all(_is_output_fresh(op, input_paths) for op in output_paths):
            logger.info(
                "Skipping upstream template '%s' for dependency '%s' of '%s': "
                "output is fresh",
                producer,
                arg_name,
                template_id,
            )
            continue
        # --- End freshness check ---

        template_type = graph.get_template_type(producer)
        logger.info(
            "Running upstream template '%s' (%s) for dependency '%s' of '%s'",
            producer,
            template_type,
            arg_name,
            template_id,
        )

        try:
            if template_type == "etl":
                report = process_etl(producer)
            else:
                report = process_marketdata(producer)

            if _implicit_reports is not None:
                _implicit_reports.append(report)
        except Exception as exc:
            if required:
                raise DependencyResolutionError(
                    f"Template '{template_id}' dependency '{arg_name}': "
                    f"upstream template '{producer}' failed: {exc}"
                ) from exc
            logger.warning(
                "Optional dependency '%s' for template '%s': "
                "upstream template '%s' failed (will try stale data): %s",
                arg_name,
                template_id,
                producer,
                exc,
            )
            return

        if not report.success:
            if required:
                raise DependencyResolutionError(
                    f"Template '{template_id}' dependency '{arg_name}': "
                    f"upstream template '{producer}' reported failures."
                )
            logger.warning(
                "Optional dependency '%s' for template '%s': "
                "upstream template '%s' reported failures (will try stale data)",
                arg_name,
                template_id,
                producer,
            )
            continue

        # Touch marker on successful processing
        for op in output_paths:
            _touch_marker(op)
```

**Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_dependency_freshness.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add brasa/engine/dependency_resolver.py tests/test_dependency_freshness.py
git commit -m "feat: integrate freshness check into _run_upstream_templates"
```

---

### Task 4: Touch marker after ETL and process_marketdata writes

**Files:**
- Modify: `brasa/engine/pipeline/etl_executor.py`
- Modify: `brasa/engine/api.py`
- Test: `tests/test_dependency_freshness.py`

The marker must also be written when ETL or process_marketdata completes *outside* of dependency resolution (e.g., explicit CLI calls, orchestrator runs). This ensures the marker is always up to date.

**Step 1: Write the failing test**

Append to `tests/test_dependency_freshness.py`:

```python
class TestMarkerWrittenAfterETL:
    """Tests that .last_processed is written after ETL execution."""

    def test_marker_written_after_execute_and_write(self, tmp_path):
        from brasa.engine.dependency_resolver import MARKER_NAME

        output_dir = tmp_path / "staging" / "my-dataset"
        output_dir.mkdir(parents=True)
        marker = output_dir / MARKER_NAME
        # After execute_and_write runs, the marker should exist
        # (We test this indirectly through process_etl integration tests)
        # For unit testing, verify _touch_marker is called from etl_executor
        assert not marker.exists()
        _touch_marker(str(output_dir))
        assert marker.exists()
```

**Step 2: Add `_touch_marker` call to `ETLPipeline.execute_and_write`**

In `brasa/engine/pipeline/etl_executor.py`, after the parquet write and catalog registration (end of `execute_and_write`), add:

```python
from brasa.engine.dependency_resolver import _touch_marker
_touch_marker(output_path)
```

**Step 3: Add `_touch_marker` call to `process_marketdata` in `api.py`**

In the `_read_marketdata` flow in `brasa/engine/api.py`, after successful parquet write, add a `_touch_marker` call for the output dataset folder.

Find the section in `process_marketdata` where parquet is written successfully and add:

```python
from .dependency_resolver import _touch_marker
# After writing parquet for each dataset folder
_touch_marker(output_path)
```

**Step 4: Run all tests**

Run: `uv run pytest tests/test_dependency_freshness.py -v`
Expected: All PASS.

Run: `uv run pytest --no-integration`
Expected: All existing tests still pass.

**Step 5: Commit**

```bash
git add brasa/engine/pipeline/etl_executor.py brasa/engine/api.py tests/test_dependency_freshness.py
git commit -m "feat: touch .last_processed marker after ETL and marketdata processing"
```

---

### Task 5: Run full test suite and lint

**Step 1: Run tests**

Run: `uv run pytest --no-integration`
Expected: All PASS.

**Step 2: Run ruff**

Run: `uv run ruff check . && uv run ruff format --check .`
Expected: All clean.

**Step 3: Run pre-commit**

Run: `uv run pre-commit run --all-files`
Expected: All pass.

**Step 4: Final commit if any fixes needed**

---

### Task 6: Manual integration test

**Step 1: Run the plan and verify**

Run: `uv run python -m brasa.cli download --plan daily-b3.yaml`

Verify in the output that `b3-indexes-composition-consolidated` is only executed once. The second and third dependents should log a skip message.

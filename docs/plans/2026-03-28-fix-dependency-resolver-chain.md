# Fix Dependency Resolver Chain Processing

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `_run_upstream_templates()` so it resolves the full dependency chain before running an ETL producer, ensuring freshly-downloaded data is processed before downstream templates consume it.

**Architecture:** The dependency resolver currently calls `process_etl(producer)` with `resolve_dependencies=False` (the default). This means the ETL runs on stale inputs. The fix passes `resolve_dependencies=True` so the `PipelineOrchestrator` handles the full ancestor chain — the same mechanism the `run` CLI command already uses successfully.

**Tech Stack:** Python, pytest, unittest.mock

---

## Bug Summary

**Reproduction:** `uv run python -m brasa.cli download --plan companies-b3.yaml`

**Chain:**
```
b3-company-info (download, 61 new files)
  → b3-companies-names (etl, reads input.b3-company-info-info)
    → b3-company-details (download, reads staging.b3-companies-names for codeCVM)
```

**What happens:**
1. `execute_download_plan()` downloads b3-company-info → 61 raw files cached (NOT processed)
2. `execute_download_plan()` downloads b3-company-details → `resolve_dependencies()` triggers
3. Resolver finds producer of `staging.b3-companies-names` → `b3-companies-names` (ETL)
4. Calls `process_etl("b3-companies-names")` **without `resolve_dependencies=True`**
5. ETL reads `input.b3-company-info-info` → **STALE** (61 new downloads unprocessed)
6. `codeCVM` list is based on old data → b3-company-details misses new companies

**Root cause:** `dependency_resolver.py:324` — `process_etl(producer)` defaults to `resolve_dependencies=False`.

**Why the `run` command works:** `brasa.cli run` uses `PipelineOrchestrator.execute()` which builds a full topological execution plan and processes every ancestor in order. The dependency resolver bypasses this.

---

## File Structure

| File | Change | Responsibility |
|---|---|---|
| `brasa/engine/dependency_resolver.py:324` | Modify | Pass `resolve_dependencies=True` to `process_etl()` |
| `tests/test_dependency_resolver.py:346` | Modify | Update assertion to verify `resolve_dependencies=True` is passed |
| `tests/test_dependency_resolver.py` (new test) | Add | Test that ETL upstream with stale inputs triggers full chain |

---

## Task 1: Update the existing test to expect `resolve_dependencies=True`

**Files:**
- Modify: `tests/test_dependency_resolver.py:330-346`

- [ ] **Step 1: Update the assertion in `test_run_upstream_etl_template`**

The existing test verifies that `process_etl` is called with the producer name. After the fix, it must also pass `resolve_dependencies=True`.

```python
# In test_run_upstream_etl_template (line 346), change:
mock_etl.assert_called_once_with("upstream-etl")

# To:
mock_etl.assert_called_once_with("upstream-etl", resolve_dependencies=True)
```

- [ ] **Step 2: Run the test to verify it FAILS (red)**

Run: `uv run pytest tests/test_dependency_resolver.py::test_run_upstream_etl_template -v`

Expected: FAIL — `process_etl` is currently called without `resolve_dependencies=True`.

---

## Task 2: Fix `_run_upstream_templates` to resolve the full chain

**Files:**
- Modify: `brasa/engine/dependency_resolver.py:324`

- [ ] **Step 1: Add `resolve_dependencies=True` to the `process_etl` call**

In `_run_upstream_templates()`, change line 324 from:

```python
            if template_type == "etl":
                report = process_etl(producer)
```

to:

```python
            if template_type == "etl":
                report = process_etl(producer, resolve_dependencies=True)
```

This single change makes the ETL call use the `PipelineOrchestrator` internally, which:
- Builds the full execution plan for the producer
- Processes any unprocessed downloads in ancestor templates
- Runs intermediate ETLs in topological order
- Then runs the target ETL on fresh data

- [ ] **Step 2: Run the updated test to verify it PASSES (green)**

Run: `uv run pytest tests/test_dependency_resolver.py::test_run_upstream_etl_template -v`

Expected: PASS

- [ ] **Step 3: Run the full dependency resolver test suite**

Run: `uv run pytest tests/test_dependency_resolver.py -v`

Expected: All tests PASS.

---

## Task 3: Add a test proving the full chain is resolved

**Files:**
- Modify: `tests/test_dependency_resolver.py` (add new test in the `_run_upstream_templates — dispatch by template type` section)

- [ ] **Step 1: Write a test that verifies `resolve_dependencies=True` is propagated**

Add after `test_run_upstream_download_template` (around line 367), in the dispatch-by-type section:

```python
def test_run_upstream_etl_uses_resolve_dependencies():
    """process_etl is called with resolve_dependencies=True so the full
    ancestor chain (downloads, intermediate ETLs) is processed before
    the target ETL runs.

    Regression test for: b3-company-info downloads not processed before
    b3-companies-names ETL when running companies-b3.yaml download plan.
    """
    from brasa.engine.dependency_resolver import _run_upstream_templates

    graph = _make_graph(producer="b3-companies-names", template_type="etl")
    mock_report = _make_report(success=True)

    with patch("brasa.engine.api.process_etl", return_value=mock_report) as mock_etl:
        _run_upstream_templates(
            "b3-company-details",
            "codeCVM",
            ["staging.b3-companies-names"],
            graph,
            required=True,
        )

    # The critical assertion: resolve_dependencies=True ensures the orchestrator
    # processes the full chain (e.g., process_marketdata for b3-company-info
    # before running b3-companies-names ETL)
    mock_etl.assert_called_once_with("b3-companies-names", resolve_dependencies=True)
```

- [ ] **Step 2: Run the new test to verify it PASSES**

Run: `uv run pytest tests/test_dependency_resolver.py::test_run_upstream_etl_uses_resolve_dependencies -v`

Expected: PASS

---

## Task 4: Run the full test suite and lint

- [ ] **Step 1: Run all tests**

Run: `uv run pytest`

Expected: All tests PASS (no regressions).

- [ ] **Step 2: Run ruff**

Run: `uv run ruff check . && uv run ruff format --check .`

Expected: No errors.

- [ ] **Step 3: Run pre-commit**

Run: `uv run pre-commit run --all-files`

Expected: All hooks PASS.

---

## Task 5: Commit

- [ ] **Step 1: Stage and commit the changes**

```bash
git add brasa/engine/dependency_resolver.py tests/test_dependency_resolver.py
git commit -m "fix: resolve full dependency chain in _run_upstream_templates

Pass resolve_dependencies=True to process_etl() so the
PipelineOrchestrator processes all ancestor templates before
running the target ETL. This ensures freshly-downloaded data
(e.g. b3-company-info) is processed into parquet before
downstream ETLs (e.g. b3-companies-names) consume it.

Previously, the dependency resolver called process_etl() with
the default resolve_dependencies=False, causing ETLs to run
on stale input data when a download plan downloaded but did
not process upstream templates between tasks."
```

---

## Why this fix is minimal and safe

1. **One line of production code changed** — `process_etl(producer)` → `process_etl(producer, resolve_dependencies=True)`
2. **Uses existing, proven machinery** — `PipelineOrchestrator` is already battle-tested by the `run` CLI command
3. **No behavioral change for fresh data** — If all ancestors are already fresh, the orchestrator skips them (same as before)
4. **No change to download plan** — `execute_download_plan()` stays download-only; the resolver handles processing
5. **No circular dependency risk** — The orchestrator calls `process_marketdata`/`process_etl` (without resolve_dependencies), not `download_marketdata`

## What this does NOT fix (out of scope)

- The download plan still doesn't explicitly process between tasks (by design — that's the resolver's job)
- Download templates called as upstream producers still use `process_marketdata()` without the orchestrator (correct — download templates don't have ETL-style dependency chains)

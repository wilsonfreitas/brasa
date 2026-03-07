# ETL Skip Bug: Stale Plan After Upstream Execution

## Context

When running `brasa run b3-indexes-composition-consolidated`, the orchestrator skips the ETL step with "output is up to date" even though the upstream `b3-indexes-composition` was just processed (9 new files passed). The ETL should re-run to consume the new upstream data.

## Root Cause

The bug is a **timing issue in execution plan evaluation**:

1. `orchestrator.execute()` calls `get_execution_plan()` **once upfront** (line 203 of `brasa/engine/orchestrator.py`) before running any steps.
2. `get_execution_plan()` calls `_check_etl_template_staleness()` at plan-build time, which **compares filesystem mtimes** of ETL output parquet files vs upstream parquet files.
3. At plan-build time, the upstream parquet files have **not yet been updated** (the process step runs later). So mtime comparison shows ETL output is fresh → action = `"skip"`.
4. The orchestrator then executes the process step, which **updates the upstream parquet files** — but the plan is already frozen. The ETL step stays `"skip"`.

## Fix: Re-evaluate ETL Staleness at Execution Time

In `brasa/engine/orchestrator.py`, modify the `execute()` loop to re-check ETL staleness before deciding to skip, when any of the step's upstream dependencies were actually executed in the current run.

**Critical files:**
- `brasa/engine/orchestrator.py` — `execute()` method (lines 224–242): add staleness re-check logic
- `brasa/engine/dependency_graph.py` — `_check_etl_template_staleness()` (lines 685–737): already correct; re-used as-is
- `tests/test_orchestrator.py` — add regression test for this scenario

## Implementation

### 1. Modify `orchestrator.execute()` in `brasa/engine/orchestrator.py`

Track which template IDs were actually executed (not skipped). Before processing a `"skip"` action for an ETL step, re-check if any of its direct upstream dependencies were executed in this run. If so, re-evaluate staleness and promote to `"etl"` if stale.

```python
executed_templates: set[str] = set()

for step in plan.steps:
    # Re-evaluate ETL steps that were planned as "skip" if any upstream was executed
    if step.action == "skip" and step.template_type == "etl":
        upstreams = self.graph.edges.get(step.template_id, [])
        if any(t in executed_templates for t in upstreams):
            if self.graph._check_etl_template_staleness(step.template_id):
                step.action = "etl"
                step.reason = "upstream dependency was updated"

    if step.action == "skip":
        logger.debug("Skipping '%s': %s", step.template_id, step.reason)
        continue

    step_report = self._execute_step(step, verbosity)
    report.step_reports[step.template_id] = step_report
    executed_templates.add(step.template_id)

    # Check for failures ...
```

### 2. Add a regression test in `tests/test_orchestrator.py`

Test scenario: A dependency graph where a download template is stale but the ETL template appears fresh at plan-build time. After the download step executes (updating upstream parquets), the ETL step should be re-evaluated and promoted to `"etl"` action.

The test should mock `_check_etl_template_staleness` to return `False` on the first call (plan build time) and `True` on the second call (re-evaluation after upstream runs), confirming the promotion happens.

## Verification

```bash
# Run tests
uv run pytest tests/test_orchestrator.py -v

# Run full test suite
uv run pytest --no-integration

# Lint
uv run ruff check . && uv run ruff format --check .

# Manual end-to-end: should see ETL execute instead of skip
uv run python -m brasa.cli run b3-indexes-composition-consolidated
```

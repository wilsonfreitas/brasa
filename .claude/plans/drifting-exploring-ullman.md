# Plan: Surface Dependency-Triggered Templates in DownloadPlanReport Summary

## Context

When `execute_download_plan` runs `b3-indexes-theoretical-portfolio`, it calls
`download_marketdata`, which calls `resolve_dependencies`, which calls
`_run_upstream_templates`, which calls `process_etl("b3-indexes-composition-consolidated")`.
That ETL executes and its `TaskReport` is checked for success — but then discarded.

`DownloadPlanReport.task_reports` only stores reports for explicit plan tasks, so
`b3-indexes-composition-consolidated` never appears in the final summary even though
it ran. This is the bug.

## Root Cause

`_run_upstream_templates` (`dependency_resolver.py:241`) captures `report = process_etl(...)`
and only checks `report.success`. The report object is never returned up the call stack
to `execute_download_plan`.

## Approach

Use an opt-in out-parameter `_implicit_reports: list | None = None` throughout the
resolver chain to collect reports without changing return types (which would break
many existing tests). Then attach collected reports to the returned `TaskReport` via
a new `dependency_reports` field, collect them in `DownloadPlanReport`, and show them
in the summary.

## Files to Modify

- `brasa/engine/reporting.py` — add `dependency_reports` field to `TaskReport`
- `brasa/engine/dependency_resolver.py` — thread `_implicit_reports` parameter
- `brasa/engine/api.py` — pass and attach implicit reports in `download_marketdata`
- `brasa/engine/download_plan.py` — store and display implicit reports

## Implementation Steps

### 1. `brasa/engine/reporting.py` — `TaskReport.__init__`

Add one line after `self._captured_warnings = ...`:

```python
self.dependency_reports: list[TaskReport] = []
```

No constructor signature change. Fully backward-compatible.

---

### 2. `brasa/engine/dependency_resolver.py` — `_run_upstream_templates`

Add optional parameter `_implicit_reports: list | None = None`:

```python
def _run_upstream_templates(
    template_id: str,
    arg_name: str,
    dataset_refs: list[str],
    graph,
    required: bool,
    _implicit_reports: list | None = None,   # NEW
) -> None:
```

After obtaining `report` (lines 241 and 243), append to the list if provided:

```python
    if template_type == "etl":
        report = process_etl(producer)
    else:
        report = process_marketdata(producer)

    if _implicit_reports is not None:          # NEW
        _implicit_reports.append(report)       # NEW
```

---

### 3. `brasa/engine/dependency_resolver.py` — `resolve_dependencies`

Add optional parameter `_implicit_reports: list | None = None`:

```python
def resolve_dependencies(
    template,
    caller_args: dict,
    _implicit_reports: list | None = None,    # NEW
) -> dict:
```

Forward to `_run_upstream_templates`:

```python
        _run_upstream_templates(
            template.id, arg_name, dataset_refs, graph, required,
            _implicit_reports=_implicit_reports,   # NEW
        )
```

---

### 4. `brasa/engine/api.py` — `download_marketdata`

Collect implicit reports and attach to the returned `TaskReport`:

```python
    from .dependency_resolver import resolve_dependencies

    template = retrieve_template(template_name)

    implicit_reports: list[TaskReport] = []                           # NEW
    resolved = resolve_dependencies(
        template, kwargs, _implicit_reports=implicit_reports          # CHANGED
    )
    kwargs = {**kwargs, **resolved}
    ...
    report.finish()
    report.dependency_reports = implicit_reports                       # NEW
```

---

### 5. `brasa/engine/download_plan.py` — `DownloadPlanReport`

Add field after `task_reports`:

```python
@dataclass
class DownloadPlanReport:
    plan_name: str
    task_reports: dict[str, TaskReport] = field(default_factory=dict)
    implicit_task_reports: dict[str, TaskReport] = field(default_factory=dict)  # NEW
    _start_time: datetime | None = field(default=None, repr=False)
    _end_time: datetime | None = field(default=None, repr=False)
```

Update `success` property to include implicit reports:

```python
    @property
    def success(self) -> bool:
        for report in {**self.task_reports, **self.implicit_task_reports}.values():
            for result in report.results:
                if result.status in (TaskStatus.ERROR, TaskStatus.FAILED):
                    return False
        return True
```

Update `summary()` — after the existing template loop, before the totals, add:

```python
        if self.implicit_task_reports:
            lines.append("")
            lines.append("  [auto] Dependencies executed:")
            for template, report in self.implicit_task_reports.items():
                passed = sum(1 for r in report.results if r.status == TaskStatus.PASSED)
                failed = sum(
                    1
                    for r in report.results
                    if r.status in (TaskStatus.FAILED, TaskStatus.ERROR)
                )
                skipped = sum(1 for r in report.results if r.status == TaskStatus.SKIPPED)
                parts = []
                if passed:
                    parts.append(f"{passed} passed")
                if failed:
                    parts.append(f"{failed} failed")
                if skipped:
                    parts.append(f"{skipped} skipped")
                status_str = ", ".join(parts) if parts else "no results"
                lines.append(f"    {template:<38}  {status_str}")
```

Update the totals line:

```python
        n = len(self.task_reports)
        n_auto = len(self.implicit_task_reports)
        auto_str = f", {n_auto} auto" if n_auto else ""
        lines.append(
            f"Download plan '{self.plan_name}': {n} tasks{auto_str} in {time_str}"
        )
```

Update `save_report` JSON to include implicit tasks:

```python
            "implicit_tasks": [
                {
                    "template": template,
                    "results": [r.to_dict() for r in report.results],
                }
                for template, report in self.implicit_task_reports.items()
            ],
```

---

### 6. `brasa/engine/download_plan.py` — `execute_download_plan`

After storing `plan_report.task_reports[task.template]`, collect dependency reports:

```python
        plan_report.task_reports[task.template] = _execute_task(
            task, resolved_args, verbosity
        )
        # NEW: collect dependency reports from the task
        task_report = plan_report.task_reports[task.template]
        for dep_report in getattr(task_report, "dependency_reports", []):
            name = dep_report.template_name
            if name not in plan_report.implicit_task_reports:
                plan_report.implicit_task_reports[name] = dep_report
```

---

## Tests to Update / Add

### `tests/test_dependency_resolver.py`

- `test_skip_when_caller_supplies_arg` — still passes (returns `{}`, no change)
- `test_happy_path_resolves_values` — still passes (mocks `_run_upstream_templates`)
- Existing tests for `_run_upstream_templates` — still pass (new param defaults to `None`)
- **Add**: `test_run_upstream_appends_report_to_implicit_list` — verifies that when
  `_implicit_reports=[]` is passed, the ETL/download report gets appended
- **Add**: `test_resolve_dependencies_propagates_implicit_reports` — verifies that
  `_implicit_reports` list is populated after `resolve_dependencies` call

### `tests/test_download_plan.py`

- Existing `TestDownloadPlanReport` tests — still pass (new field has default)
- **Add**: `test_summary_shows_implicit_dep` — verifies `[auto]` section appears
  when `implicit_task_reports` is populated
- **Add**: `test_execute_plan_collects_implicit_reports` — patches `_execute_task`
  to return a `TaskReport` with non-empty `dependency_reports`, verifies they land
  in `plan_report.implicit_task_reports`
- **Add**: `test_success_false_when_implicit_dep_fails` — verifies `success` is
  `False` when an implicit report has FAILED results

## Verification

```bash
uv run pytest tests/test_dependency_resolver.py tests/test_download_plan.py -v
uv run pytest --no-integration
uv run ruff check . && uv run ruff format --check .
uv run pre-commit run --all-files
```

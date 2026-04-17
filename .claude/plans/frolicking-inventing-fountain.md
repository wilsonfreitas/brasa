# Plan: Rename `reprocess` → `force` in download_plan.py

## Context

`download_marketdata` (in `brasa/engine/api.py`) already renamed its `reprocess` parameter to `force`. The CLI `download` command also uses `--force`. However, `download_plan.py` still uses `reprocess` in its dataclasses and YAML parsing, creating an inconsistency. The fix is to rename the internal field to `force` and accept both `reprocess` and `force` in YAML for backward compatibility.

---

## Changes

### 1. `brasa/engine/download_plan.py`

- **`DownloadPlanDefaults`**: rename field `reprocess: bool = False` → `force: bool = False`, update docstring.
- **`DownloadPlanTask`**: rename field `reprocess: bool = False` → `force: bool = False`, update docstring.
- **`DownloadPlan.from_dict()`**: update YAML key reads to accept both `force` and `reprocess` (compat):
  ```python
  # defaults block (line ~110)
  force=bool(defaults_data.get("force", defaults_data.get("reprocess", False))),
  # task block (line ~120)
  force=bool(task_data.get("force", task_data.get("reprocess", defaults.force))),
  ```
- **`_execute_task()`** (line 466): update `force=task.reprocess` → `force=task.force`.

### 2. `tests/test_download_plan.py`

- Update `VALID_PLAN_DICT` literals: `"reprocess"` → `"force"` (lines 52, 55).
- Update assertions: `plan.defaults.reprocess` → `plan.defaults.force`, `plan.tasks[N].reprocess` → `plan.tasks[N].force` (lines 72, 79, 82, 105).
- Rename test methods and update their bodies:
  - `test_task_inherits_default_reprocess` → `test_task_inherits_default_force`
  - `test_task_overrides_default_reprocess` → `test_task_overrides_default_force`

### 3. YAML plan files in project root (example files)

Update `reprocess:` → `force:` keys (not description text strings — those are cosmetic):
- `indexes-b3.yaml` lines 7, 20
- `equity-options-b3.yaml` line 9
- `bvbg086.yaml` line 7

---

## Out of scope

- `brasa/cli.py` `process` subcommand `--reprocess` flag — unrelated parameter, no change needed.
- `brasa/engine/api.py` — already correct.

---

## Verification

```bash
uv run pytest tests/test_download_plan.py -v
uv run ruff check . && uv run ruff format --check .
uv run pre-commit run --all-files
```

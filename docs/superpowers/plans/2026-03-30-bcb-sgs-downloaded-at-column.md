# `bcb-sgs` `downloaded_at` Column Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `downloaded_at` column with `datetime.now()` to the `bcb-sgs` template reader by extending `AddColumnStep` to support `from.where: now`.

**Architecture:** Add a single `elif where == "now"` branch to `AddColumnStep._resolve_value` in `column_steps.py`. Then update `bcb-sgs.yaml` to include the step and field. `AddColumnMultiStep` inherits the change for free.

**Tech Stack:** Python, pandas, PyArrow, pytest, uv

---

## Files

| Action | Path |
|--------|------|
| Modify | `brasa/engine/pipeline/steps/column_steps.py` (lines 107–123) |
| Modify | `templates/bcb/bcb-sgs.yaml` |
| Modify | `tests/test_pipeline.py` (add new test function) |

---

### Task 1: Extend `AddColumnStep` to support `where: now`

**Files:**
- Modify: `brasa/engine/pipeline/steps/column_steps.py`
- Test: `tests/test_pipeline.py`

- [ ] **Step 1: Write the failing test**

Add this function to `tests/test_pipeline.py`:

```python
def test_add_column_where_now():
    """Test that add_column with from.where: now adds a datetime column."""
    from datetime import datetime
    from unittest.mock import MagicMock

    import pandas as pd

    from brasa.engine.pipeline.context import PipelineContext
    from brasa.engine.pipeline.steps.column_steps import AddColumnStep

    # Build minimal context (download_args not needed for where: now)
    meta = MagicMock()
    meta.download_args = {}
    meta.extra_key = None
    context = PipelineContext(meta=meta, reader_config={})

    df = pd.DataFrame({"value": [1, 2, 3]})

    step = AddColumnStep(
        name="add_column",
        params={"name": "downloaded_at", "from": {"where": "now"}},
    )

    before = datetime.now()
    result = step.execute(df, context)
    after = datetime.now()

    assert "downloaded_at" in result.columns
    ts = result["downloaded_at"].iloc[0]
    assert isinstance(ts, datetime)
    assert before <= ts <= after
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_pipeline.py::test_add_column_where_now -v
```

Expected: FAIL with `ValueError: Unknown 'from.where' value: now`

- [ ] **Step 3: Add `where: now` branch to `_resolve_value`**

In `brasa/engine/pipeline/steps/column_steps.py`, update `_resolve_value` (currently lines 107–123):

```python
    def _resolve_value(self, context: PipelineContext) -> Any:
        """Resolve the column value from params or context.

        Returns:
            The resolved value to assign to the column.

        Raises:
            ValueError: If no valid value source is provided.
        """
        if "value" in self.params:
            return self.params["value"]
        elif "from" in self.params:
            from_param = self.params["from"]
            where = from_param["where"]
            if where == "context":
                key = from_param["key"]
                return context.get_result(key)
            elif where == "download_args":
                key = from_param["key"]
                return context.meta.download_args.get(key)
            elif where == "extra_key":
                return context.meta.extra_key
            elif where == "now":
                from datetime import datetime

                return datetime.now()
            else:
                raise ValueError(f"Unknown 'from.where' value: {where}")
        else:
            raise ValueError("add_column requires 'value', or 'from' parameter")
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_pipeline.py::test_add_column_where_now -v
```

Expected: PASS

- [ ] **Step 5: Run full test suite**

```bash
uv run pytest --no-integration
```

Expected: all tests pass

- [ ] **Step 6: Commit**

```bash
git add brasa/engine/pipeline/steps/column_steps.py tests/test_pipeline.py
git commit -m "feat: add where: now support to add_column step (WIL-32)"
```

---

### Task 2: Update `bcb-sgs.yaml` template

**Files:**
- Modify: `templates/bcb/bcb-sgs.yaml`

- [ ] **Step 1: Add the step and field**

Replace the contents of `templates/bcb/bcb-sgs.yaml` with:

```yaml
id: bcb-sgs
description: Séries temporais do SGS (Sistema Gerenciador de Séries) do BCB

downloader:
  function: brasa.downloaders.bcb_sgs_download
  validator: brasa.downloaders.validate_json_empty_file
  format: json
  args:
    code: ~
    start: ~
    end: ~

reader:
  locale: pt
  pipeline:
    - step: read_json
    - step: rename_columns
      mapping:
        data: refdate
        valor: value
    - step: add_column
      name: code
      from:
        where: download_args
        key: code
    - step: add_column
      name: downloaded_at
      from:
        where: now
    - step: apply_fields
      errors: coerce

writer:
  layer: input
  partitioning: [refdate, code]

fields:
  - name: refdate
    description: Data de referência
    type: date(format='%d/%m/%Y')
  - name: value
    description: Valor da série
    type: numeric(decimal=',')
  - name: code
    description: Código da série SGS
    type: integer
  - name: downloaded_at
    description: Timestamp when data was downloaded
    type: datetime
```

- [ ] **Step 2: Verify template loads correctly**

```bash
uv run python -c "
from brasa.engine.template import retrieve_template
t = retrieve_template('bcb-sgs')
print('fields:', [f.name for f in t.reader.fields])
print('steps:', [s['step'] for s in t.reader.pipeline])
"
```

Expected output includes `downloaded_at` in fields and `add_column` (twice) in steps:
```
fields: ['refdate', 'value', 'code', 'downloaded_at']
steps: ['read_json', 'rename_columns', 'add_column', 'add_column', 'apply_fields']
```

- [ ] **Step 3: Run full test suite**

```bash
uv run pytest --no-integration
```

Expected: all tests pass

- [ ] **Step 4: Run linting and formatting checks**

```bash
uv run ruff check . && uv run ruff format --check .
```

Expected: no errors

- [ ] **Step 5: Run pre-commit hooks**

```bash
uv run pre-commit run --all-files
```

Expected: all hooks pass

- [ ] **Step 6: Commit**

```bash
git add templates/bcb/bcb-sgs.yaml
git commit -m "feat: add downloaded_at column to bcb-sgs template (WIL-32)"
```

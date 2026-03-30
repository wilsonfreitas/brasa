# Design: Add `downloaded_at` Column to `bcb-sgs` Reader

**Date:** 2026-03-30
**Linear:** WIL-32

## Summary

Add a `downloaded_at` column to the `bcb-sgs` template reader pipeline that records the wall-clock timestamp when the pipeline step executes (`datetime.now()`). This requires extending the `add_column` step to support a new `from.where: now` value source, then using it in the template.

## Architecture

The change has two parts:

1. **Extend `AddColumnStep._resolve_value`** in `brasa/engine/pipeline/steps/column_steps.py` to handle `where: now` by returning `datetime.now()`.
2. **Update `templates/bcb/bcb-sgs.yaml`** to add the `add_column` step and `downloaded_at` field.

`AddColumnMultiStep` inherits `_resolve_value` from `AddColumnStep`, so it gets `where: now` support for free.

## Components

### 1. `AddColumnStep._resolve_value` (`column_steps.py`)

Add a new branch in the `from` resolution logic:

```python
elif where == "now":
    from datetime import datetime
    return datetime.now()
```

No new parameters or step types. The `where: now` source joins the existing `context`, `download_args`, and `extra_key` sources as a natural extension of the `from` pattern.

### 2. Template update (`templates/bcb/bcb-sgs.yaml`)

Add an `add_column` step before `apply_fields`:

```yaml
reader:
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
```

Add the field definition:

```yaml
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

The step is placed before `apply_fields` so the `datetime` field type is applied consistently. `pd.to_datetime` handles datetime objects directly, so no conversion issues arise.

## Data Flow

```
read_json → rename_columns → add_column(code) → add_column(downloaded_at=datetime.now()) → apply_fields → writer
```

## Error Handling

No special error handling needed. `datetime.now()` is infallible. If `apply_fields` runs with `errors: coerce`, any unexpected issue with the column is silently coerced to null — consistent with how other fields are handled.

## Testing

### Unit test (`tests/test_pipeline.py`)

Add a test for `where: now` in `AddColumnStep`:

- Assert the column is added to the DataFrame.
- Assert the column values are `datetime` instances.
- Assert the timestamp is recent (within a few seconds of `datetime.now()`).

### Integration test (`tests/test_templates.py`)

The existing template validation covers `bcb-sgs` schema parsing automatically. An additional `@pytest.mark.integration` test can download a real SGS series and assert `downloaded_at` is present and recent.

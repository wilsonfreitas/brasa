---
name: brasa-template-writer
description: Write new brasa YAML templates from scratch or migrate legacy templates (with reader.function and handler-based fields) to the modern pipeline-based format. Use when the user asks to create a new template, write a template, migrate a legacy template, convert an old template, or work with YAML template definitions. Also trigger when the user mentions template creation, template migration, or refers to legacy/old templates.
---

# Brasa Template Writer

Write new templates from scratch or migrate legacy templates to modern pipeline-based format.

## Mode Detection

- **Write new**: User asks to create/write/add a new template
- **Migrate**: User asks to migrate/convert/update a legacy template (one with `reader.function` or `handler:` field blocks)

## Workflow

### Write New Template

1. Determine template type:
   - **Single-dataset reader**: one set of fields, reads a single file format
   - **Multi-dataset reader**: multiple datasets from one source (e.g., XML with multiple tags)
   - **ETL pipeline**: transforms data from upstream datasets using SQL or steps

2. If user provides a full spec (URL, format, fields), generate directly
3. If info is missing, ask one question at a time:
   - What data source? (URL, institution, file format)
   - What file format? (CSV, FWF, JSON, XML, Excel)
   - What fields? (names, types, descriptions)
   - What layer? (input, staging, curated)
   - Partitioning? (typically `[refdate]`)

4. Generate the template YAML
5. Save to `templates/` in appropriate subdirectory based on data source:
   - `templates/b3/` for B3 data (subdirs: equities, futures, indexes, raw, options)
   - `templates/anbima/` for ANBIMA
   - `templates/bcb/` for BCB/SGS
   - `templates/cvm/` for CVM
   - `templates/td/` for Tesouro Direto

### Migrate Legacy Template

1. Read the legacy template from `templates/legacy/` or wherever it lives
2. Apply all transformations (see Migration Rules below)
3. Save the new template to the appropriate `templates/` subdirectory
4. Add a YAML comment at top noting it was migrated from the legacy version

## Migration Rules

### Field Type Migration

| Legacy (`handler:`) | Modern (`type:`) |
|---|---|
| `handler: {type: numeric, dec: 2.0}` | `type: numeric(dec=2.0)` |
| `handler: {type: numeric, dec: 0.0}` | `type: integer` |
| `handler: {type: Date, format: '%Y%m%d'}` | `type: date(format='%Y%m%d')` |
| `handler: {type: POSIXct, format: '%H%M%S'}` | `type: datetime(format='%H%M%S')` |
| `handler: {type: character}` | `type: character` |
| `handler: {type: factor, ...}` | `type: character` |

- Remove the `handler:` block entirely from each field
- Keep `name:`, `description:`, `width:` (if FWF), and any `tag:` attributes
- If `handler.dec` is a field reference (e.g., `dec: num_casas_decimais_2`), add a comment noting the dynamic decimal and use `type: numeric` without dec parameter

### Sign Field Migration

For numeric fields with `sign: some_column`:

1. Convert the field to `type: numeric(dec=N)` (without sign)
2. Add a YAML comment: `# sign: originally from <sign_column>`
3. After the `apply_fields` step in the pipeline, add steps:

```yaml
# Apply sign columns to their target numeric fields
- step: custom_simple
  code: |
    import numpy as np
    sign_map = {
      'cot_primeiro_negocio': 'sinal_cot_primeiro_negocio',
      # ... list all sign->target pairs
    }
    for target, sign_col in sign_map.items():
        if target in df.columns and sign_col in df.columns:
            mask = df[sign_col].str.strip() == '-'
            df.loc[mask, target] = -df.loc[mask, target]

# Drop sign columns (no longer needed)
- step: drop_columns
  columns: [sinal_cot_primeiro_negocio, sinal_cot_menor_negocio, ...]
```

Also remove the sign fields from the `fields:` list.

### Structural Migration

- Replace `reader: { function: ... }` with `reader: { pipeline: [...] }`
- Choose pipeline steps based on `filetype`:
  - `FWF` → `read_fwf` (dtype: str) → filter if needed → `apply_fields`
  - `CSV` → `read_csv` → `apply_fields`
  - `JSON` → `read_json` → `apply_fields`
  - `Excel/XLS` → `read_excel` → `apply_fields`
- Add `writer:` block with `layer: input` and `partitioning: [refdate]`
- Add `downloader:` block if the legacy template has URL info or if user provides it
- Remove `filename:` and `filetype:` top-level keys (these are inferred by the pipeline)

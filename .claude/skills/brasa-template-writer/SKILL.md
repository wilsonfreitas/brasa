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

## Field Type Reference

| Type | Parameters | Example |
|---|---|---|
| `character` / `string` / `char` | none | `type: character` |
| `integer` / `int` | none | `type: integer` |
| `numeric` / `number` | `dec`, `sign`, `thousands`, `decimal` | `type: numeric(dec=2.0)` |
| `date` | `format` (default: `%Y-%m-%d`) | `type: date(format='%Y%m%d')` |
| `datetime` / `posixct` | `format` (default: `%Y-%m-%d %H:%M:%S`) | `type: datetime(format='%H%M%S')` |
| `time` | `format` (default: `%H:%M:%S`) | `type: time(format='%H%M')` |
| `boolean` / `bool` | none | `type: boolean` |

### Syntax Rules

- Parameters in parentheses: `typename(key=value)`
- String values in single quotes: `date(format='%Y%m%d')`
- Multiple params comma-separated: `numeric(dec=2, decimal=',')`
- `numeric(dec=0.0)` for integers stored as fixed-width numbers → prefer `integer` in new templates

## Pipeline Steps Reference

Comprehensive reference of all registered pipeline steps organized by category.

### I/O Steps (Reading Data)

| Step | Parameters | Description |
|---|---|---|
| `read_csv` | `separator`, `skip`, `header`, `names`, `converters` | Read CSV file with optional custom separator and header |
| `read_fwf` | `colspecs`, `names`, `skip`, `dtype` | Read fixed-width format files (plain or gzip); derives column specs from field widths |
| `read_json` | `orient`, `path` | Read JSON file (supports gzip) into DataFrame |
| `read_excel` | `sheet`, `skip`, `header` | Read Excel file into DataFrame |

### Column Manipulation Steps

| Step | Parameters | Description |
|---|---|---|
| `set_columns` | `names` (required) | Set column names for DataFrame |
| `rename_columns` | `mapping` (required) | Rename columns using dict mapping |
| `select_columns` | `columns` (required) | Select specific columns to keep |
| `drop_columns` | `columns` (required) | Drop columns from data |
| `add_column` | `name`, `value`, `from` (with `where` and `key`), `only_if_missing` | Add new column with static or dynamic value |
| `reorder_columns` | `order` (required), `keep_rest` | Reorder columns in DataFrame |

### Data Transformation Steps

| Step | Parameters | Description |
|---|---|---|
| `apply_fields` | `errors` (coerce/raise/ignore), `set_columns` | Apply field type definitions using Fieldset |
| `apply_fields_multi` | `errors` | Apply field definitions to multiple DataFrames in dict |
| `parse_numeric` | `columns` (required), `errors` | Parse string columns as numeric using context settings |
| `parse_date` | `columns` (required), `format`, `errors` | Parse string columns as dates |
| `parse_datetime` | `columns` (required), `format`, `errors` | Parse string columns as datetime values |
| `fill_na` | `columns`, `value`, `method` (ffill/bfill) | Fill NA/NaN values |
| `drop_duplicates` | `subset`, `keep` (first/last/False) | Remove duplicate rows |
| `drop_na` | `columns`, `how` (any/all) | Drop rows with NA/NaN values |
| `filter_rows` | `column`, `operator` (eq/ne/gt/lt/etc), `value` | Filter rows based on conditions |
| `forward_fill_column` | `column` (required), `condition` | Forward fill values in column |
| `extract_regex` | `column`, `pattern` (required), `output`, `group` | Extract values using regex capture groups |
| `concat_columns` | `columns`, `output` (required), `separator` | Concatenate multiple columns into one |
| `melt` | `id_vars`, `value_vars`, `var_name`, `value_name` | Unpivot DataFrame from wide to long format |
| `sort` | `by` (required), `ascending`, `descending`, `na_position` | Sort data by columns |
| `make_date` | `year_column`, `month_column`, `day_column` (required), `output`, `errors` | Create date column from components |
| `str_replace` | `column`, `pattern` (required), `replacement`, `output`, `regex` | Replace pattern in string column |
| `cast` | `column`, `dtype` (required), `errors` | Cast column(s) to specific type |

### ETL Pipeline Steps

| Step | Parameters | Description |
|---|---|---|
| `load` | `template` OR (`input`, `layer`) | Load a dataset as PyArrow Dataset |
| `concat_datasets` | `inputs` (required), `layer` (required), `columns` | Concatenate multiple datasets vertically |
| `dataset_filter` | `where` (required) | Filter rows by equality conditions |
| `dataset_select` | `columns` (required) | Select columns from dataset |
| `select_fields` | (uses context.fields) | Select columns based on field names |
| `dataset_sort` | `by` (required), `descending` | Sort PyArrow dataset |
| `dataset_drop_columns` | `columns` (required) | Drop columns from dataset |
| `dataset_rename_columns` | `mapping` (required) | Rename columns in dataset |
| `dataset_drop_duplicates` | `subset`, `keep` | Remove duplicate rows from dataset |
| `dataset_fill_na` | `value`, `method`, `columns` | Fill missing values in dataset |
| `to_dataframe` | (none) | Convert PyArrow Dataset/Table to pandas DataFrame |
| `sql_query` | `datasets` (required), `query` (required) | Execute SQL on datasets in in-memory DuckDB |
| `future_maturity_to_date` | `code_column`, `date_column` (required), `maturity_day`, `calendar` | Convert future maturity codes to dates |
| `following_bizday` | `date_column`, `adjusted_column` (required), `calendar` | Adjust dates to following business day |
| `bizdays` | `from_column`, `to_column`, `output_column` (required), `calendar` | Calculate business days between dates |
| `implied_rate` | `price_column`, `rate_column`, `days_to_maturity_column` (required), `compounding`, `forward_price` | Calculate implied interest rate from price |
| `flatten_columns` | `columns` (required), `separator` | Flatten delimited values into separate rows |

### B3-Specific Steps

| Step | Parameters | Description |
|---|---|---|
| `b3_read_bvbg028_xml` | (uses datasets config) | Read/parse B3 BVBG028 gzipped XML file (returns Dict[str, DataFrame]) |
| `b3_read_bvbg086_xml` | (uses field tags) | Read/parse B3 BVBG086 gzipped XML file |
| `b3_read_bvbg087_xml` | (uses datasets config) | Read/parse B3 BVBG087 gzipped XML file |
| `b3_read_company_info_json` | (uses datasets config) | Read B3 company info gzipped JSON (returns Dict[str, DataFrame]) |
| `b3_read_company_details_json` | (none) | Read B3 company details JSON, expands otherCodes array |
| `b3_add_columns_from_json_fields` | `mapping` (required) | Parse B3 JSON fields and add as columns |
| `b3_parse_refdate_from_html` | `xpath`, `attribute`, `store_as` | Parse reference date from B3 HTML page |
| `b3_forward_fill_commodity` | `column` | Forward fill commodity names in B3 settlement prices |
| `b3_extract_commodity_code` | `column` | Extract commodity code from commodity name |
| `b3_create_symbol` | `commodity_column`, `maturity_column`, `output_column` | Create futures symbol by concatenating commodity and maturity |

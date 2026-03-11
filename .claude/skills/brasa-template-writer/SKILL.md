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
2. **Analyze the reader function** (see "Analyzing the Reader Function" below) to understand how fields are parsed
3. Apply all transformations (see Migration Rules below)
4. Save the new template to the appropriate `templates/` subdirectory
5. Add a YAML comment at top noting it was migrated from the legacy version

### Analyzing the Reader Function

The legacy `reader.function` contains valuable information about field parsing that should inform your field type definitions. Always examine the function implementation in `brasa/readers/` before migrating.

**Common patterns to look for:**

| Code Pattern | Field Type |
|---|---|
| `pd.to_numeric(df[col].str.replace(",", "."), ...)` | `type: numeric(decimal=",")` |
| `pd.to_numeric(df[col].str.replace(".", "").str.replace(",", "."), ...)` | `type: numeric(decimal=",", thousands=".")` |
| `pd.to_datetime(df[col], format='%d/%m/%Y', ...)` | `type: date(format='%d/%m/%Y')` |
| `pd.to_datetime(df[col], dayfirst=True, ...)` | `type: date` (with dayfirst in reader context) |
| `pd.to_numeric(df[col], ...)` | `type: numeric` (plain numeric parsing) |
| `str.replace(",", ".")` on numeric field | indicates comma as decimal separator → use `decimal=","` |
| `str.replace(".", "").str.replace(",", ".")` | indicates European format (dot=thousands, comma=decimal) → use `thousands=".", decimal=","` |

**Example:** If the reader function has:
```python
df["volume"] = pd.to_numeric(df["volume"].str.replace(",", "."), errors="coerce")
df["price"] = pd.to_numeric(df["price"].str.replace(",", "."), errors="coerce")
```

Then in the YAML, these should be:
```yaml
- name: volume
  type: numeric(decimal=",")
- name: price
  type: numeric(decimal=",")
```

Not just `type: numeric` with no parameters.

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

## Canonical Examples

These templates serve as models for creating new templates of each type.

### Example 1: Single-Dataset FWF Reader

Fixed-width format (FWF) file with type filtering and field conversion.

```yaml
id: b3-cotahist-daily
description: Cotações Históricas do Pregão de Ações - Arquivo Diário

downloader:
  verify_ssl: false
  function: brasa.downloaders.datetime_download
  url: https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D%d%m%Y.ZIP
  format: zip
  args:
    refdate: ~

reader:
  encoding: latin1
  locale: en
  pipeline:
    - step: read_fwf
      dtype: str
    - step: filter_rows
      column: regtype
      operator: eq
      value: '01'
    - step: apply_fields
      errors: coerce

writer:
  layer: input
  partitioning: [refdate]

fields:
  - name: regtype
    description: Tipo de registro
    type: character
    width: 2
  - name: refdate
    description: Data do pregão
    type: date(format='%Y%m%d')
    width: 8
  - name: symbol
    description: Código de negociação
    type: character
    width: 12
  - name: open
    description: Preço de abertura
    type: numeric(dec=2.0)
    width: 13
  - name: high
    description: Preço máximo
    type: numeric(dec=2.0)
    width: 13
  - name: low
    description: Preço mínimo
    type: numeric(dec=2.0)
    width: 13
  - name: close
    description: Preço último negócio
    type: numeric(dec=2.0)
    width: 13
  - name: volume
    description: Volume total negociado
    type: numeric(dec=2.0)
    width: 18
  - name: traded_contracts
    description: Quantidade negociada
    type: integer
    width: 18
```

**Key features:**
- `read_fwf` reads fixed-width format (widths derived from field definitions)
- `filter_rows` keeps only type "01" records
- `apply_fields` with `errors: coerce` converts to proper types
- `datetime_download` with `refdate` parameter

---

### Example 2: Single-Dataset CSV Reader

CSV with custom separator, encoding, column renaming, and context variable injection.

```yaml
id: cvm-companies-registration
description: Cadastro de companhias abertas da CVM

downloader:
  function: brasa.downloaders.simple_download
  verify_ssl: false
  extra-key: date
  url: https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv
  format: csv

reader:
  locale: pt
  encoding: latin1
  pipeline:
    - step: read_csv
      separator: ";"
    - step: add_column
      from:
        where: extra_key
      name: refdate
    - step: rename_columns
      mapping:
        CNPJ_CIA: cnpj_cia
        DENOM_SOCIAL: denom_social
        DT_REG: dt_reg
        SETOR: setor
        SUBSETOR: subsetor
    - step: apply_fields
      errors: coerce

writer:
  layer: input
  partitioning: [refdate]

fields:
  - name: cnpj_cia
    description: CNPJ da companhia
    type: character
  - name: denom_social
    description: Denominação social
    type: character
  - name: dt_reg
    description: Data de registro
    type: date(format='%Y-%m-%d')
  - name: setor
    description: Setor econômico
    type: character
  - name: subsetor
    description: Subsetor econômico
    type: character
  - name: refdate
    description: Data de referência
    type: date
```

**Key features:**
- `read_csv` with custom separator (semicolon)
- `add_column` from `extra_key` (download metadata)
- `rename_columns` mapping for CSV headers
- `encoding: latin1` for non-UTF8 files
- `locale: pt` for Portuguese number formatting

---

### Example 3: Multi-Dataset XML Reader

XML file with multiple datasets (equities and options) extracted from different tags.

```yaml
id: b3-bvbg028
description: Arquivo de Preços de Mercado

downloader:
  function: brasa.downloaders.datetime_download
  url: https://www.b3.com.br/pesquisapregao/download?filelist=IN%y%m%d.zip
  format: zip
  args:
    refdate: ~

reader:
  locale: en
  pipeline:
    - step: b3_read_bvbg028_xml
    - step: apply_fields_multi

writer:
  partitioning: [refdate]

datasets:
  equities:
    tag: EqtyInf
    fields:
      - name: refdate
        description: Data de referência
        tag: RptParams/RptDtAndTm/Dt
        type: date
      - name: symbol
        description: Código de negociação
        tag: InstrmInf/EqtyInf/TckrSymb
        type: character
      - name: isin
        description: Código ISIN
        tag: InstrmInf/EqtyInf/ISIN
        type: character
      - name: corporation_name
        description: Razão social
        tag: InstrmInf/EqtyInf/CrpnNm
        type: character
      - name: open
        description: Preço de abertura
        tag: InstrmInf/EqtyInf/FrstPric
        type: numeric
      - name: close
        description: Preço de fechamento
        tag: InstrmInf/EqtyInf/LastPric
        type: numeric

  options_on_equities:
    tag: OptnOnEqtsInf
    fields:
      - name: refdate
        description: Data de referência
        tag: RptParams/RptDtAndTm/Dt
        type: date
      - name: symbol
        description: Código de negociação
        tag: InstrmInf/OptnOnEqtsInf/TckrSymb
        type: character
      - name: exercise_price
        description: Preço de exercício
        tag: InstrmInf/OptnOnEqtsInf/ExrcPric
        type: numeric
      - name: maturity_date
        description: Data de vencimento
        tag: InstrmInf/OptnOnEqtsInf/XprtnDt
        type: date
```

**Key features:**
- `b3_read_bvbg028_xml` is a B3-specific step that parses XML into multiple DataFrames
- `datasets:` block defines multiple output datasets with different XML tags
- Each dataset has its own `fields:` with `tag:` attributes for XPath extraction
- `apply_fields_multi` applies field conversions to all datasets

---

### Example 4: ETL with SQL

ETL template that loads upstream datasets and transforms via SQL query.

```yaml
id: b3-equities-returns
description: Dataset de retornos de ações

etl:
  pipeline:
    - step: sql_query
      datasets:
        - staging.b3-cotahist
        - staging.b3-equities-spot-market
      query: |
        WITH equity_symbols AS (
          SELECT DISTINCT symbol
          FROM 'staging.b3-equities-spot-market'
          WHERE security_category IN (1, 11, 13)
        )
        SELECT
            t.refdate,
            t.symbol,
            (t.close / LAG(t.close) OVER (PARTITION BY t.symbol ORDER BY t.refdate)) - 1 AS pct_return,
            LN(t.close / LAG(t.close) OVER (PARTITION BY t.symbol ORDER BY t.refdate)) AS log_return
        FROM 'staging.b3-cotahist' t
        INNER JOIN equity_symbols s ON t.symbol = s.symbol
        WHERE ROW_NUMBER() OVER (PARTITION BY t.symbol ORDER BY t.refdate) > 1
        ORDER BY t.refdate, t.symbol

    - step: apply_fields
      errors: coerce

writer:
  layer: staging

fields:
  - name: refdate
    description: Data de referência
    type: date
  - name: symbol
    description: Símbolo do ativo
    type: character
  - name: pct_return
    description: Retorno percentual
    type: numeric
  - name: log_return
    description: Retorno logarítmico
    type: numeric
```

**Key features:**
- `etl:` block with pipeline instead of `reader:`
- `sql_query` loads upstream datasets and executes SQL
- Uses CTEs, window functions, and JOINs for transformations
- Writes to `staging` layer instead of `input`
- No `downloader:` needed (data from upstream templates)

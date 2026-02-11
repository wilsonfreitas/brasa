# Templates: Configuration & Design

This document provides comprehensive guidance on **pipeline-based templates** in brasa—the new approach using `reader.pipeline` and `etl.pipeline` with typed fields/datasets. These templates enable declarative, data-driven configurations for both download/read and ETL workflows.

**Legacy function-based templates** (using `reader.function` and `etl.function`) are documented in [CONFIGURATION.md](CONFIGURATION.md). This guide focuses exclusively on the modern pipeline approach.

---

## Table of Contents

1. [Overview](#overview)
   - [Template Types Taxonomy](#template-types-taxonomy)
   - [Validation Rules by Type](#validation-rules-by-type)
2. [Template Inheritance](#template-inheritance)
   - [Inheritance Rules](#inheritance-rules)
   - [Merge Semantics](#merge-semantics)
   - [Using the Template Compiler](#using-the-template-compiler)
3. [Download & Read Templates (Single Dataset)](#download--read-templates-single-dataset)
4. [Download & Read Templates (Multi-Dataset)](#download--read-templates-multi-dataset)
5. [ETL Templates (Single Dataset)](#etl-templates-single-dataset)
6. [Field Schema & Type System](#field-schema--type-system)
7. [Pipeline Steps Reference](#pipeline-steps-reference)
8. [Common Pitfalls & Best Practices](#common-pitfalls--best-practices)
9. [Architecture & Processing Flow](#architecture--processing-flow)

---

## Overview

Pipeline-based templates use **YAML-defined processing steps** instead of custom Python functions. This approach enables:

- **Declarative configuration** - Define transformations in YAML
- **Reusable steps** - Common operations (filtering, type conversion, date parsing) without code
- **Type safety** - Explicit field types (`character`, `number`, `date`, `integer`) with validation
- **Chainable processing** - Steps are executed sequentially; output of step N becomes input to N+1
- **Multi-output support** - Single template can produce multiple named datasets with independent schemas

### Template Types Taxonomy

Brasa supports **four distinct template types** based on their purpose and output structure:

| Type | Purpose | Sections | Output | Example |
|------|---------|----------|--------|----------|
| **Download & Read (Single)** | Extract raw data → one dataset | `downloader:`, `reader:`, `fields:` | Single parquet dataset | `b3-futures-settlement-prices` |
| **Download & Read (Multi)** | Extract raw data → multiple datasets | `downloader:`, `reader:`, `datasets:` | Multiple independent datasets | `b3-bvbg087` (indexes, IOPV, BDR) |
| **ETL (Single)** | Transform dataset → derived dataset | `etl:`, `writer:`, `fields:` | Single parquet dataset | `b3-futures-dap` |
| **ETL (Multi)** | Transform → multiple datasets | `etl:`, `writer:`, `datasets:` | Multiple independent datasets | (rare, custom use) |

**Key distinction:** Templates using `fields:` produce **one dataset**. Templates using `datasets:` produce **multiple datasets** with independent schemas.

### Validation Rules by Type

- **Download & Read (Single):** Must have `downloader:`, `reader:`, `fields:`, `writer:` (optional layer)
- **Download & Read (Multi):** Must have `downloader:`, `reader:`, `datasets:` (no top-level `fields:`)
- **ETL (Single):** Must have `etl:` with `load` step, `fields:`, `writer:` (required layer)
- **ETL (Multi):** Must have `etl:`, `datasets:` (no top-level `fields:`)
- **Mutual exclusivity:** Cannot have both `fields:` and `datasets:` in same template
- **Multi-dataset readers:** Must use `apply_fields_multi` step, not `apply_fields`

### Key Differences: Download/Read vs. ETL

| Aspect | Download & Read | ETL |
|--------|-----------------|-----|
| **Purpose** | Extract raw data from external sources | Transform existing datasets |
| **Input** | External files (HTML, JSON, CSV, XML) | Existing parquet datasets (input/staging/curated) |
| **Required sections** | `downloader:`, `reader:` | `etl:` |
| **Processing flow** | download → read pipeline → validate → save | load → etl pipeline → validate → save |
| **Output layer** | Usually `input` | Usually `staging` or `curated` |

---

## Template Inheritance

Template inheritance enables **code reuse and DRY principles** across templates by allowing child templates to extend base templates. This is **optional** and fully backward-compatible—existing templates work unchanged.

### Inheritance Rules

1. **`extends` keyword**: Child templates declare their parent using `extends: <parent-template-id>`
2. **Recursive resolution**: Inheritance chains are resolved depth-first (grandparent → parent → child)
3. **Deep merge for maps**: Nested mappings (e.g., `downloader:`, `reader:`) are merged recursively
4. **Keyed merge for lists**: Arrays like `fields:`, `datasets:`, and `reader.pipeline:` merge by key:
   - `fields:` merge by `name`
   - `datasets:` merge by dataset key (e.g., `indexes_info`)
   - `pipeline:` merge by `step` name
5. **Child overrides parent**: When the same key exists in both parent and child, child wins
6. **Optional feature**: Templates without `extends` behave exactly as before

### Merge Semantics

#### Map (Dictionary) Deep Merge

Nested mappings are merged recursively, with child values overriding parent values:

```yaml
# base.yaml
downloader:
  verify_ssl: false
  encoding: utf-8
  args:
    param1: value1

# child.yaml
extends: base
downloader:
  encoding: latin1
  args:
    param2: value2

# Result after compilation:
downloader:
  verify_ssl: false
  encoding: latin1  # child overrides
  args:
    param1: value1  # from parent
    param2: value2  # from child
```

#### List Keyed Merge

Lists are merged by their identifying key field. Items with the same key are merged; unique items are combined:

**Fields merge by `name`:**

```yaml
# base.yaml
fields:
  - name: refdate
    type: date[%Y-%m-%d]
  - name: symbol
    type: character

# child.yaml
extends: base
fields:
  - name: symbol
    description: Ticker symbol  # adds description to existing field
  - name: price
    type: number

# Result:
fields:
  - name: refdate
    type: date[%Y-%m-%d]
  - name: symbol
    type: character
    description: Ticker symbol  # merged
  - name: price
    type: number  # new field from child
```

**Pipeline steps merge by `step`:**

```yaml
# base.yaml
reader:
  pipeline:
    - step: read_csv
      separator: ","
    - step: select_columns
      columns: [date, price]

# child.yaml
extends: base
reader:
  pipeline:
    - step: read_csv
      separator: ";"  # override separator
    - step: apply_fields  # new step

# Result:
reader:
  pipeline:
    - step: read_csv
      separator: ";"  # child overrides
    - step: select_columns
      columns: [date, price]
    - step: apply_fields  # added from child
```

**Datasets merge by dataset key:**

```yaml
# base.yaml
datasets:
  indexes_info:
    tag: IndxInf
    fields:
      - name: code
        type: character

# child.yaml
extends: base
datasets:
  indexes_info:
    fields:
      - name: name
        type: character
  composition:
    tag: IndxCompnts
    fields:
      - name: ticker
        type: character

# Result:
datasets:
  indexes_info:  # merged
    tag: IndxInf
    fields:
      - name: code
        type: character
      - name: name
        type: character
  composition:  # new dataset
    tag: IndxCompnts
    fields:
      - name: ticker
        type: character
```

### Using the Template Compiler

The template compiler resolves `extends` references and generates fully-expanded YAML files.

#### Compilation Workflow

```bash
# Compile all templates with inheritance
poetry run python -m brasa.cli compile-templates

# Compile specific templates
poetry run python -m brasa.cli compile-templates b3-futures-di1 b3-curves-di1

# Output directory (preserves filenames)
# templates/ → templates/compiled/
```

#### Runtime Behavior

By default, the runtime loader reads from the original `templates/` directory. To use compiled templates:

```python
import brasa

# Use compiled templates (if available)
brasa.get_marketdata("b3-futures-di1", compiled=True)
```

Or via environment variable:

```bash
export BRASA_USE_COMPILED_TEMPLATES=1
poetry run python script.py
```

### Schema Extension

The `extends` key is **optional** and can appear in any template type:

```yaml
extends: <parent-template-id>  # Optional: inherit from parent
id: <template-id>
description: <description>
# ... rest of template
```

**Validation:**
- Parent template must exist in the `templates/` directory
- Circular inheritance is not allowed
- Runtime templates ignore `extends` (preprocessing only)

---

## Download & Read Templates (Single Dataset)

These templates extract data from external sources and produce **one dataset**. They follow a two-stage pipeline: **downloader** + **reader**.

**Type signature:** `downloader:` + `reader:` + `fields:` → **one dataset**

### Template Structure

```yaml
id: <template-id>
description: <description>
downloader:
  function: <downloader-function>
  url: <url-or-pattern>
  format: <html | json | csv | zip | etc>
  encoding: <encoding>
  args:
    <param>: <value>
reader:
  encoding: <encoding>
  decimal: <separator>
  thousands: <separator>
  pipeline:
    - step: <step-name>
      <step-params>
writer:
  layer: input | staging | curated
  partitioning: [<column1>, <column2>]
fields:
  - name: <field-name>
    description: <description>
    type: <type-definition>
```

### Example 1: Reading HTML with Pipeline

**Template:** `b3-futures-settlement-prices` (simplified)

```yaml
id: b3-futures-settlement-prices
description: Preços de Ajustes Diários de Contratos Futuros da B3
downloader:
  verify_ssl: false
  function: brasa.downloaders.settlement_prices_download
  url: https://www2.bmf.com.br/pages/portal/bmfbovespa/lumis/lum-ajustes-do-pregao-ptBR.asp
  format: html
  encoding: latin1
  args:
    refdate: ~

reader:
  encoding: latin1
  decimal: ","
  thousands: "."
  locale: en  # Reader-specific locale setting (optional)
  pipeline:
    # Extract HTML table by ID
    - step: read_html
      attrs:
        id: tblDadosAjustes

    # Select first table
    - step: first_table

    # Set column names
    - step: set_columns
      names:
        - commodity
        - maturity_code
        - previous_price
        - price
        - price_change
        - settlement_value

    # Parse refdate from HTML
    - step: b3_parse_refdate_from_html
      xpath: "//input[@id='dData1']"
      attribute: value
      store_as: refdate

    # Add refdate column from context
    - step: add_column
      name: refdate
      from_context: refdate

    # Fill forward commodity names
    - step: b3_forward_fill_commodity
      column: commodity

    # Extract commodity code
    - step: b3_extract_commodity_code
      column: commodity

    # Create symbol
    - step: b3_create_symbol
      commodity_column: commodity
      maturity_column: maturity_code
      output_column: symbol

    # Apply field type conversions
    - step: apply_fields
      errors: coerce

writer:
  layer: input
  partitioning: [refdate]

fields:
  - name: commodity
    description: Nome e código da mercadoria
    type: character
  - name: maturity_code
    description: Código de vencimento do contrato futuro com 3 caractéres
    type: character
  - name: previous_price
    description: Preço de ajuste do dia anterior
    type: number
  - name: price
    description: Preço de ajuste atual
    type: number
  - name: price_change
    description: Variação do preço de ajuste
    type: number
  - name: settlement_value
    description: Valor do ajuste por contrato (R$)
    type: number
  - name: refdate
    description: Data de referência
    type: date(format="%d/%m/%Y")
  - name: symbol
    description: Símbolo do contrato futuro
    type: character
```

### Example 2: Reading JSON with Field Extraction

**Template:** `b3-indexes-composition` (simplified)

```yaml
id: b3-indexes-composition
description: Composição dos índices da B3
downloader:
  function: brasa.downloaders.b3_paged_url_encoded_download
  url: https://sistemaswebb3-listados.b3.com.br/indexProxy/indexCall/GetStockIndex
  format: json
  encoding: latin1
  args:
    pageNumber: 1
    pageSize: 9999
  extra-key: date  # Passes date as context

reader:
  encoding: latin1
  locale: pt  # Reader-specific locale (optional)
  pipeline:
    # Parse JSON result array
    - step: read_json
      path: results

    # Add refdate from context (passed by downloader)
    - step: add_column
      from:
        where: extra_key
      name: refdate

    # Extract nested JSON fields and add as columns
    - step: b3_add_columns_from_json_fields
      mapping:
        update_date: header.update
        start_month: header.startMonth
        end_month: header.endMonth
        year: header.year

    # Set column order and names
    - step: set_columns
      names:
        - corporation_name
        - specification_code
        - symbol
        - indexes
        - refdate
        - update_date
        - start_month
        - end_month
        - year

    # Convert types
    - step: apply_fields
      errors: coerce

writer:
  layer: input
  partitioning: [refdate]

fields:
  - name: corporation_name
    description: Nome da companhia
    type: character
  - name: specification_code
    description: Código de especificação da ação
    type: character
  - name: symbol
    description: Código da ação
    type: character
  - name: indexes
    description: Lista de índexes separados por vírgula
    type: character
  - name: refdate
    description: Data de referência
    type: date
  - name: update_date
    description: Data de atualização do índice
    type: date
  - name: start_month
    description: Mês de início de vigência do índice
    type: integer
  - name: end_month
    description: Mês de fim de vigência do índice
    type: integer
  - name: year
    description: Ano de criação do índice
    type: integer
```

### Key Concepts

**Downloader** (`downloader:`)
- Fetches raw data from URL/API
- Can use `extra-key` to pass context data to reader
- Supports URL patterns with date substitution (e.g., `%y%m%d`)
- Returns downloaded file(s) cached by CacheManager

**Reader Pipeline** (`reader.pipeline`)
- Sequence of transformation steps
- Each step receives the output of the previous step
- Steps can read files, parse formats, extract data, add columns, convert types
- Final output is a DataFrame matching the `fields:` schema

**Writer** (`writer:`)
- Controls output layer and partitioning
- Default layer: `input`
- Partitioning: list of columns (e.g., `[refdate]` creates date-based folders)

**Fields** (`fields:`)
- Schema definition with type information
- Each field has: `name`, `description`, `type`, optional `tag` (for field mapping)
- Type definitions support format parameters, e.g., `date(format="%d/%m/%Y")`

---

## ETL Templates (Single Dataset)

ETL (Extract-Transform-Load) templates transform existing datasets into derived datasets. They use only `etl:` and `writer:` sections—no downloader or reader.

**Type signature:** `etl:` (with `load` step) + `fields:` → **one derived dataset**

**Note:** Multi-dataset ETL templates (using `datasets:`) are possible but rare. Most ETL transforms produce a single consolidated dataset.

### Template Structure

```yaml
id: <template-id>
description: <description>
etl:
  pipeline:
    - step: load
      input: <source-dataset-id>
      layer: input | staging | curated
    - step: <transformation-step>
      <step-params>
writer:
  layer: input | staging | curated
  dataset: <output-dataset-name>
  partitioning: [<column1>, <column2>]
fields:
  - name: <field-name>
    description: <description>
    type: <type-definition>
```

### Example 1: Simple ETL Pipeline

**Template:** `b3-futures-dap` (simplified)

```yaml
id: b3-futures-dap
description: Futuros de Juros Real (Cupom de IPCA) - DAP

etl:
  pipeline:
    # Load base settlement prices
    - step: load
      input: b3-futures-settlement-prices
      layer: staging

    # Filter for DAP contracts only
    - step: filter
      where:
        commodity: "DAP"

    # Convert maturity code to date
    - step: future_maturity_to_date
      code_column: maturity_code
      date_column: maturity_date
      maturity_day: 15th day
      calendar: ANBIMA

    # Adjust to next business day
    - step: following_bizday
      date_column: maturity_date
      adjusted_column: maturity_date_adj
      calendar: ANBIMA

    # Calculate business days to maturity
    - step: bizdays
      from_column: refdate
      to_column: maturity_date_adj
      output_column: business_days
      calendar: ANBIMA

    # Calculate calendar days
    - step: bizdays
      from_column: refdate
      to_column: maturity_date_adj
      output_column: calendar_days
      calendar: Actual

    # Calculate implied interest rate
    - step: implied_rate
      price_column: price
      rate_column: adjusted_tax
      days_to_maturity_column: business_days
      compounding: discrete

    # Sort results
    - step: sort
      by: [refdate, maturity_date]

    # Select final columns
    - step: select
      columns:
        - refdate
        - symbol
        - maturity_date
        - price
        - adjusted_tax
        - business_days
        - calendar_days

writer:
  layer: staging
  dataset: b3-futures-dap
  partitioning: [refdate]

fields:
  - name: refdate
    description: Data de referência
    type: date
  - name: symbol
    description: Código do contrato futuro
    type: character
  - name: maturity_date
    description: Data de vencimento
    type: date
  - name: price
    description: Preço de ajuste
    type: number
  - name: adjusted_tax
    description: Taxa implícita ajustada
    type: number
  - name: business_days
    description: Dias úteis até vencimento
    type: integer
  - name: calendar_days
    description: Dias corridos até vencimento
    type: integer
```

### Example 2: Multi-Dataset Consolidation

**Template:** `b3-indexes-composition-consolidated` (simplified)

```yaml
id: b3-indexes-composition-consolidated
description: Consolidated information on B3 index compositions

etl:
  pipeline:
    # Load base composition data
    - step: load
      input: b3-indexes-composition
      layer: input

    # Flatten comma-separated index list into rows
    - step: flatten_columns
      columns:
        - indexes
      separator: ","

writer:
  layer: staging
  dataset: b3-indexes-composition
  partitioning: [refdate]

fields:
  # Same as b3-indexes-composition source
  - name: corporation_name
    description: Nome da companhia
    type: character
  - name: specification_code
    description: Código de especificação da ação
    type: character
  - name: symbol
    description: Código da ação
    type: character
  - name: index
    description: Índice individual (exploded from list)
    type: character
  - name: refdate
    description: Data de referência
    type: date
  - name: update_date
    description: Data de atualização do índice
    type: date
  - name: start_month
    description: Mês de início de vigência do índice
    type: integer
  - name: end_month
    description: Mês de fim de vigência do índice
    type: integer
  - name: year
    description: Ano de criação do índice
    type: integer
```

### Key Concepts

**ETL Pipeline** (`etl.pipeline`)
- Starts with `load` step referencing an input dataset
- Transforms data through subsequent steps
- Each step output becomes next step's input
- Final output passed to writer for saving

**Input Layer Specification**
- `load` step must specify `input` (dataset ID) and `layer` (where to find it)
- Layers: `input` (raw), `staging` (derived), `curated` (final)
- Allows chaining ETL: dataset A → B → C

**Writer for ETL**
- `layer:` - where to save output (usually `staging` for intermediate, `curated` for final)
- `dataset:` - optional explicit output name (defaults to template ID)
- `partitioning:` - output folder structure (e.g., by date or category)

**Step Chaining**
- Output of step N is automatically passed as input to step N+1
- No explicit variable passing needed
- Steps operate on the entire DataFrame

---

## Download & Read Templates (Multi-Dataset)

Some data sources naturally produce **multiple separate datasets** from a single download, requiring the **multi-dataset pattern**.

**Type signature:** `downloader:` + `reader:` + `datasets:` → **multiple independent datasets**

**Common use cases:**
- B3 BVBG087: XML file containing three datasets (indexes, IOPV, BDR)
- Complex hierarchical data with multiple entity types
- Single source file with different record types

**Key difference from single-dataset:** Uses `datasets:` section instead of top-level `fields:`, allowing each output to have its own schema with type-tagged field mappings.

**Validation requirement:** Multi-dataset templates:
- MUST use `datasets:` (not `fields:`)
- MUST NOT have top-level `fields:` section
- MUST use `apply_fields_multi` step (not `apply_fields`)
- Each dataset MUST have unique `tag:` for source mapping

### Template Structure

```yaml
id: <template-id>
description: <description>
downloader:
  # ... downloader config
reader:
  pipeline:
    - step: <custom-multi-output-step>
    - step: apply_fields_multi
writer:
  partitioning: [<column>]
datasets:
  <output-name-1>:
    tag: <source-tag-1>  # e.g., XML element name
    fields:
      - name: <field-name>
        description: <description>
        tag: <source-field-path>  # e.g., XML path
        type: <type-definition>
  <output-name-2>:
    tag: <source-tag-2>
    fields:
      # ... field definitions
```

### Example: B3 BVBG087 (Multi-Dataset XML)

**Template:** `b3-bvbg087` (simplified)

**Type:** Download & Read (Multi-Dataset) — produces 3 independent datasets

```yaml
id: b3-bvbg087
description: Arquivo de Índices, IOPV, e BDR da B3
calendar: Brazil/B3
downloader:
  function: brasa.downloaders.datetime_download
  url: https://www.b3.com.br/pesquisapregao/download?filelist=IR%y%m%d.zip
  format: zip
  args:
    refdate: ~

reader:
  locale: en
  pipeline:
    steps:
      # Custom step that parses XML and returns dict of DataFrames
      - b3_read_bvbg087_xml
      # Apply field conversions for each dataset
      - apply_fields_multi

writer:
  partitioning: [refdate]

# Multiple datasets with independent schemas
datasets:
  indexes_info:
    tag: IndxInf  # XML element name
    fields:
      - name: refdate
        description: Reference date
        type: date
      - name: symbol
        description: Ticker symbol
        tag: SctyInf/SctyId/TckrSymb  # XPath to field in XML
        type: string
      - name: security_id
        description: Security ID
        tag: SctyInf/FinInstrmId/OthrId/Id
        type: integer
      - name: settlement_price
        description: Settlement price
        tag: SttlmVal
        type: numeric
      - name: open_price
        description: Open price
        tag: SctyInf/OpngPric
        type: numeric
      - name: close_price
        description: Close price
        tag: SctyInf/ClsgPric
        type: numeric
      - name: last_price
        description: Last price
        tag: SctyInf/IndxVal
        type: numeric

  iopv_info:
    tag: IOPVInf  # Different XML element
    fields:
      - name: refdate
        description: Reference date
        type: date
      - name: symbol
        description: Ticker symbol
        tag: SctyId/TckrSymb
        type: string
      - name: security_id
        description: Security ID
        tag: FinInstrmId/OthrId/Id
        type: integer
      - name: open_price
        description: Open price
        tag: OpngPric
        type: numeric
      - name: close_price
        description: Close price
        tag: ClsgPric
        type: numeric
      - name: last_price
        description: Last price
        tag: IndxVal
        type: numeric

  bdr_info:
    tag: BDRInf  # Third dataset
    fields:
      - name: refdate
        description: Reference date
        type: date
      - name: symbol
        description: Ticker symbol
        tag: SctyId/TckrSymb
        type: string
      - name: security_id
        description: Security ID
        tag: FinInstrmId/OthrId/Id
        type: integer
      - name: reference_price
        description: Reference price
        tag: RefPric
        type: numeric
```

### Processing Multi-Dataset Output

When `datasets:` is defined:

1. **Reader pipeline** produces a **dict of DataFrames** instead of single DataFrame
   - Key: output name (e.g., `indexes_info`)
   - Value: DataFrame for that dataset

2. **apply_fields_multi step** validates each DataFrame against its dataset's field schema

3. **Writer** saves each output to separate parquet folder:
   - `db/input/indexes_info/`
   - `db/input/iopv_info/`
   - `db/input/bdr_info/`

4. **Dataset catalog** registers each as independent dataset with its own schema

### Field Mapping for Multi-Datasets

The `tag:` property in fields enables extraction from hierarchical data:
- **XML**: `tag: SctyInf/SctyId/TckrSymb` = XPath to extract field
- **JSON**: `tag: "security.info.code"` = dot-notation path
- Used by custom steps like `b3_read_bvbg087_xml` to map source paths to field names

---

## Field Schema & Type System

Fields define the **output schema** with type information for validation and conversion.

### Field Definition

```yaml
fields:
  - name: <field-name>              # Required: output column name
    description: <description>       # Recommended: human-readable description
    type: <type-definition>          # Required: data type with optional format
    tag: <source-path>               # Optional: path in source data (XML/JSON)
    handler: <handler-name>          # Optional: custom parsing handler (legacy)
```

### Supported Types

| Type | Syntax | Example | Notes |
|------|--------|---------|-------|
| String/Text | `character` | `character` | Default if no handler |
| Numeric | `number(decimal=..., thousands=...)` | `number(decimal=",", thousands=".")` | Supports optional separators (defaults to US format) |
| Integer | `integer` | `integer` | Whole numbers |
| Boolean | `boolean` | `boolean` | True/false |
| Date | `date(format=...)` | `date(format="%d/%m/%Y")` | Parses string to date with format |
| Date | `date` | `date` | ISO format YYYY-MM-DD (default) |
| Datetime | `datetime(format=...)` | `datetime(format="%Y-%m-%d %H:%M:%S")` | Parses string to datetime with format |
| JSON | `json` | `json` | Nested JSON structure |

### Type Format Parameters

#### Numeric Types

The `number` type supports optional parameters for parsing numeric values with specific decimal and thousands separators:

```yaml
type: number(decimal=",", thousands=".")
```

**Parameters:**
- `decimal` - Decimal separator character (default: `.`)
  - `.` for English/US format: `1,234.56`
  - `,` for European/Brazilian format: `1.234,56`
  - `|` or other separator if needed

- `thousands` - Thousands separator character (default: `,`)
  - `,` for English/US format: `1,234.56`
  - `.` for European/Brazilian format: `1.234,56`
  - Empty string `""` if no thousands separator
  - `|` or other separator if needed

**Examples:**
```yaml
fields:
  - name: price_us
    description: Price in US format
    type: number(decimal=".", thousands=",")  # Parses "1,234.56" → 1234.56

  - name: price_br
    description: Price in Brazilian format
    type: number(decimal=",", thousands=".")  # Parses "1.234,56" → 1234.56

  - name: simple_number
    description: Number without thousands separator
    type: number(decimal=".", thousands="")   # Parses "1234.56" → 1234.56
```

**Common Configurations:**
| Region | Format | Type Definition |
|--------|--------|---|
| **US/UK** | 1,234.56 | `number(decimal=".", thousands=",")` |
| **Brazil/Europe** | 1.234,56 | `number(decimal=",", thousands=".")` |
| **No separator** | 1234.56 | `number(decimal=".", thousands="")` |

**Default Behavior:**
If not specified, `number` defaults to `number(decimal=".", thousands=",")` (US format).

#### Date/Datetime Types

**Date/datetime types** support format strings using Python's `strftime` format codes:

**Format Codes:**
- `%Y` - 4-digit year (e.g., 2024)
- `%y` - 2-digit year (e.g., 24)
- `%m` - 2-digit month (01-12)
- `%d` - 2-digit day (01-31)
- `%H` - hour in 24-hour format (00-23)
- `%M` - minute (00-59)
- `%S` - second (00-59)
- `%j` - day of year (001-366)

**Examples:**
```yaml
fields:
  - name: refdate
    type: date(format="%d/%m/%Y")  # 25/01/2024

  - name: timestamp
    type: datetime(format="%Y-%m-%d %H:%M:%S")  # 2024-01-25 14:30:45

  - name: maturity
    type: date(format="%y%m%d")  # 240125

  - name: created_at
    type: datetime(format="%d/%m/%Y %H:%M")  # 25/01/2024 14:30

  - name: settlement_date
    type: date(format="%Y%m%d")  # 20240125
```

**Default Behavior:**
If no format specified:
- `date` defaults to ISO format `YYYY-MM-DD`
- `datetime` defaults to ISO format `YYYY-MM-DD HH:MM:SS`

### Type Validation in Pipeline

The `apply_fields` step validates and converts all columns to declared types:

```yaml
reader:
  pipeline:
    # ... other steps
    - step: apply_fields
      errors: coerce  # Options: raise, coerce, ignore
```

**Error handling**:
- `raise` - Stop on invalid data
- `coerce` - Convert best effort, set invalid to NULL
- `ignore` - Skip validation

### Example: Field Definitions

```yaml
fields:
  - name: refdate
    description: Reference date for market data
    type: date(format="%d/%m/%Y")

  - name: symbol
    description: Stock ticker symbol
    type: character

  - name: open_price
    description: Opening price (Brazilian real)
    type: number

  - name: volume
    description: Trading volume in shares
    type: integer

  - name: active
    description: Whether security is actively traded
    type: boolean
```

---

## Pipeline Steps Reference

This is a **summary** of common steps. For complete reference, see [ETL_PIPELINE_DESIGN.md](ETL_PIPELINE_DESIGN.md#pipeline-steps).

### Reader Pipeline Steps

Steps used in `reader.pipeline` to parse raw data:

| Step | Purpose | Common Params |
|------|---------|---------------|
| `read_html` | Extract HTML tables | `attrs: {id: ...}` |
| `read_json` | Parse JSON, select path | `path: "results"` |
| `read_csv` | Read CSV data | (uses reader config) |
| `first_table` | Select first table | (none) |
| `set_columns` | Set or rename columns | `names: [...]` |
| `add_column` | Add new column | `name:`, `from:`, or `from_context:` |
| `rename_columns` | Rename multiple columns | `mapping: {old: new}` |
| `select` | Keep specific columns | `columns: [...]` |
| `filter` | Filter rows | `where: {col: value}` |
| `apply_fields` | Type conversion & validation | `errors: raise\|coerce\|ignore` |
| `b3_parse_refdate_from_html` | Extract date from HTML attributes | `xpath:`, `attribute:`, `store_as:` |
| `b3_add_columns_from_json_fields` | Extract nested JSON to columns | `mapping: {col: json.path}` |
| `b3_forward_fill_commodity` | Forward-fill commodity names | `column:` |
| `b3_extract_commodity_code` | Extract commodity code | `column:` |
| `b3_create_symbol` | Concatenate columns to symbol | `commodity_column:`, `maturity_column:`, `output_column:` |

### ETL Pipeline Steps

Steps used in `etl.pipeline` to transform datasets:

| Step | Purpose | Common Params |
|------|---------|---------------|
| `load` | Load input dataset | `input:`, `layer:` |
| `select` | Keep specific columns | `columns: [...]` |
| `filter` | Filter rows | `where: {col: value}` |
| `sort` | Sort data | `by: [...]` |
| `rename_columns` | Rename columns | `mapping: {old: new}` |
| `drop_duplicates` | Remove duplicate rows | (none) |
| `drop_nulls` | Remove rows with NULL | `columns: [...]` (optional) |
| `flatten_columns` | Explode delimited values | `columns:`, `separator:` |
| `future_maturity_to_date` | Convert contract code to date | `code_column:`, `date_column:`, `maturity_day:`, `calendar:` |
| `following_bizday` | Move date to next business day | `date_column:`, `adjusted_column:`, `calendar:` |
| `bizdays` | Count business days | `from_column:`, `to_column:`, `output_column:`, `calendar:` |
| `implied_rate` | Calculate interest rate | `price_column:`, `rate_column:`, `days_to_maturity_column:`, `compounding:` |

### Multi-Dataset Steps

Steps for handling multiple output datasets:

| Step | Purpose | Notes |
|------|---------|-------|
| `b3_read_bvbg087_xml` | Parse B3 BVBG087 XML format | Returns dict of DataFrames |
| `apply_fields_multi` | Apply field conversions to each dataset | Used with `datasets:` section |

---

## Common Pitfalls & Best Practices

### Pitfall 1: Reader Pipeline Step Ordering

**Problem:** Steps are executed sequentially; wrong order causes failures.

**Example (wrong):**
```yaml
reader:
  pipeline:
    - step: apply_fields  # Type conversion first!
      errors: coerce
    - step: set_columns   # Column names set after conversion—fails
      names: [col1, col2]
```

**Fix:** Establish column structure before type conversion.
```yaml
reader:
  pipeline:
    - step: read_html
      attrs: {id: tblData}
    - step: set_columns
      names: [col1, col2, col3]
    - step: apply_fields  # After columns are defined
      errors: coerce
```

**Best practice:** Follow this order:
1. Read/parse file → DataFrame
2. Extract/select/rename columns
3. Add computed columns (dates from context, etc.)
4. Type conversion with `apply_fields`

---

### Pitfall 2: Multi-Dataset Schema Mismatch

**Problem:** Each dataset in `datasets:` has independent schema; field mismatches cause silent data loss.

**Example (wrong):**
```yaml
datasets:
  indexes_info:
    tag: IndxInf
    fields:
      - name: symbol
        type: character
  iopv_info:
    tag: IOPVInf
    fields:
      - name: ticker  # Different name for same field!
        type: character
```

**Fix:** Use consistent field names across datasets if they represent the same concept.
```yaml
datasets:
  indexes_info:
    fields:
      - name: symbol
        type: character
  iopv_info:
    fields:
      - name: symbol  # Same field name
        type: character
```

**Best practice:**
- Align common fields (e.g., `refdate`, `symbol`) across datasets
- Document why fields differ if intentional
- Test that all datasets produce expected columns

---

### Pitfall 3: Wrong Type Definition Syntax

**Problem:** Inconsistent type syntax between old and new approaches.

**Legacy (handler-based):** Field handlers like `@CurrencyHandler`
**New (type-based):** Type strings like `character`, `number`, `date(...)`

**Example (wrong mix):**
```yaml
fields:
  - name: price
    handler: CurrencyHandler  # Old syntax—may not work with pipeline!
  - name: date
    type: date(format="%d/%m/%Y")  # New syntax
```

**Fix:** Use only `type:` with new pipelines.
```yaml
fields:
  - name: price
    type: number
  - name: date
    type: date(format="%d/%m/%Y")
```

**Best practice:**
- New templates always use `type:` not `handler:`
- Apply `apply_fields` step to convert types
- Legacy `handler:` only in old function-based templates

---

### Pitfall 4: Missing Partitioning in Writer

**Problem:** No `partitioning:` results in huge monolithic parquet files.

**Example (wrong):**
```yaml
writer:
  layer: input
  # No partitioning—creates single huge file!
```

**Fix:** Specify partitioning columns.
```yaml
writer:
  layer: input
  partitioning: [refdate]  # Partition by date
```

**Best practice:**
- Partition by date (`refdate`) for time-series data
- Partition by category (`symbol`, `commodity`) for categorical data
- Avoid partitioning by high-cardinality columns (too many folders)

---

### Pitfall 5: Dataset vs. Layer Confusion in ETL

**Problem:** `etl.pipeline` `load` step requires both `input` name and `layer` specification.

**Example (wrong):**
```yaml
etl:
  pipeline:
    - step: load
      input: b3-futures-settlement-prices
      # Missing layer!—where is this dataset?
```

**Fix:** Always specify layer.
```yaml
etl:
  pipeline:
    - step: load
      input: b3-futures-settlement-prices
      layer: input  # Found in db/input/b3-futures-settlement-prices/
```

**Best practice:**
- Know which layer your input dataset lives in
- Use `brasa list-datasets` to find datasets
- Use `brasa describe-dataset input.b3-futures-settlement-prices` to verify schema

---

### Pitfall 6: Forgetting `apply_fields` in Multi-Dataset

**Problem:** Multi-dataset outputs skip type conversion without `apply_fields_multi` step.

**Example (wrong):**
```yaml
reader:
  pipeline:
    - step: b3_read_bvbg087_xml  # Produces dict of DataFrames
    # Missing apply_fields_multi!—data stays as strings
```

**Fix:** Always include `apply_fields_multi` after multi-output steps.
```yaml
reader:
  pipeline:
    - step: b3_read_bvbg087_xml
    - step: apply_fields_multi  # Converts types for all datasets
```

---

### Pitfall 7: Using Wrong Template Type (fields vs datasets)

**Problem:** Using `fields:` for multi-output source or `datasets:` for single output.

**Example (wrong — multi-output needs datasets):**
```yaml
id: b3-bvbg087
description: XML with 3 entity types
reader:
  pipeline:
    - step: b3_read_bvbg087_xml  # Returns dict of DataFrames
    - step: apply_fields  # Wrong! Should be apply_fields_multi
fields:  # Wrong! Should be datasets:
  - name: symbol
    type: character
```

**Fix:** Use `datasets:` for multi-output sources.
```yaml
id: b3-bvbg087
reader:
  pipeline:
    - step: b3_read_bvbg087_xml
    - step: apply_fields_multi  # Correct for multi-output
datasets:  # Correct!
  indexes_info:
    tag: IndxInf
    fields:
      - name: symbol
        type: character
  iopv_info:
    tag: IOPVInf
    fields:
      - name: symbol
        type: character
```

**Example (wrong — single-output needs fields):**
```yaml
id: b3-futures-settlement-prices
reader:
  pipeline:
    - step: read_html
    # ...
    - step: apply_fields_multi  # Wrong for single output!
datasets:  # Wrong! Should be fields:
  prices:
    fields:
      - name: price
        type: number
```

**Fix:** Use `fields:` for single-output.
```yaml
id: b3-futures-settlement-prices
reader:
  pipeline:
    - step: read_html
    # ...
    - step: apply_fields  # Correct for single output
fields:  # Correct!
  - name: price
    type: number
```

**Best practice:**
- **Single dataset output** → use `fields:` + `apply_fields`
- **Multiple dataset outputs** → use `datasets:` + `apply_fields_multi`
- Never mix: cannot have both `fields:` and `datasets:` in same template
- Template validation should enforce this mutual exclusivity

---

1. **Template Organization**
   - Keep templates focused: one source per template
   - Use descriptive IDs: `b3-futures-settlement-prices` not `b3-fp`
   - Always include `description:`
   - Choose correct template type (single vs multi-dataset) at design time

2. **Field Definitions**
   - Include `type:` for all fields
   - Use `format:` for date/datetime types
   - Use `tag:` for field mapping in structured data (XML/JSON)

3. **Pipeline Design**
   - Keep reader pipelines focused on parsing/extraction
   - Keep ETL pipelines focused on business logic
   - Use generic steps (filter, select, sort) not custom functions

4. **Testing**
   - Test with small sample data first
   - Check output schema matches template `fields:`
   - Verify partitioning structure is correct

5. **Documentation**
   - Comment complex steps
   - Document custom step parameters
   - Link to external data sources

---

## Architecture & Processing Flow

### High-Level Processing

```
┌─────────────────────────────────────────────────────┐
│ Download & Read Templates                           │
├─────────────────────────────────────────────────────┤
│ 1. Downloader fetches file(s)                        │
│ 2. Reader pipeline parses → DataFrame               │
│ 3. apply_fields validates types                     │
│ 4. Writer saves to db/[layer]/[dataset]/             │
│ 5. DatasetCatalog registers schema                  │
└─────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────┐
│ ETL Templates                                       │
├─────────────────────────────────────────────────────┤
│ 1. Load step reads input dataset                    │
│ 2. ETL pipeline transforms → DataFrame             │
│ 3. apply_fields validates types                     │
│ 4. Writer saves to db/[layer]/[dataset]/             │
│ 5. DatasetCatalog registers schema                  │
└─────────────────────────────────────────────────────┘
```

### Data Layers

Brasa uses three data layers:

| Layer | Purpose | Typical Content | Access |
|-------|---------|-----------------|--------|
| `input` | Raw data from sources | Downloaded/parsed market data | Read-only for users |
| `staging` | Intermediate processing | Derived datasets, transformations | Read/write for ETL |
| `curated` | Final, cleaned data | Public datasets, models, analysis | Read-only for users |

### Template Type Detection

The processing engine detects template type by inspecting configuration:

```python
def detect_template_type(template):
    has_downloader = hasattr(template, 'downloader')
    has_etl = hasattr(template, 'etl')
    has_fields = hasattr(template, 'fields') and template.fields is not None
    has_datasets = hasattr(template, 'datasets') and template.datasets is not None

    if has_downloader and has_fields:
        return "DOWNLOAD_READ_SINGLE"
    elif has_downloader and has_datasets:
        return "DOWNLOAD_READ_MULTI"
    elif has_etl and has_fields:
        return "ETL_SINGLE"
    elif has_etl and has_datasets:
        return "ETL_MULTI"
    else:
        raise ValueError("Invalid template configuration")
```

**Validation rules:**
- Cannot have both `downloader:` and `etl:` (mutually exclusive)
- Cannot have both `fields:` and `datasets:` (mutually exclusive)
- Multi-dataset templates must use `apply_fields_multi` step
- ETL templates must have at least one `load` step in pipeline

### Code Architecture

- **Templates:** [templates/](../templates/) - YAML template definitions
- **Template Engine:** [brasa/engine/template.py](../brasa/engine/template.py) - Template loader, parser, schema
- **Pipeline Engine:** [brasa/engine/pipeline/](../brasa/engine/pipeline/) - Step registry, context, execution
- **Processing:** [brasa/engine/processing.py](../brasa/engine/processing.py) - Orchestrates download/read/ETL/write
- **Catalog:** [brasa/engine/catalog.py](../brasa/engine/catalog.py) - Dataset metadata management
- **API:** [brasa/api.py](../brasa/api.py) - Public functions for templates, datasets

### Key Classes

**MarketDataTemplate** ([brasa/engine/template.py](../brasa/engine/template.py))
- Represents a loaded template
- Properties: `id`, `description`, `downloader`, `reader`, `etl`, `writer`, `fields`, `datasets`
- Methods: `retrieve_template(id)` to load by ID

**PipelineContext** ([brasa/engine/pipeline/context.py](../brasa/engine/pipeline/context.py))
- Maintains state during pipeline execution
- Stores: current DataFrame/dict, reader config, datasets config, metadata
- Methods: store/retrieve context variables

**StepRegistry** ([brasa/engine/pipeline/__init__.py](../brasa/engine/pipeline/__init__.py))
- Singleton registry of all available steps
- Register custom steps: `@StepRegistry.register("step_name")`
- Lookup: `StepRegistry.get_step("step_name")`

**DatasetCatalog** ([brasa/engine/catalog.py](../brasa/engine/catalog.py))
- SQLite-backed metadata store
- Methods: `register_dataset()`, `get_dataset_info()`, `list_datasets()`
- Schema: parquet files in each layer auto-register on first write

### Adding Custom Steps

Steps are registered via decorator. Example:

```python
from brasa.engine.pipeline import PipelineStep, StepRegistry
from brasa.engine.pipeline.context import PipelineContext

@StepRegistry.register("my_custom_step")
class MyCustomStep(PipelineStep):
    """Custom processing step."""

    def validate_params(self) -> None:
        """Validate step parameters."""
        self.validate_required(["column_name"])

    def execute(self, data, context: PipelineContext):
        """Execute transformation.

        Args:
            data: Current DataFrame or dict of DataFrames
            context: Pipeline execution context

        Returns:
            Transformed data (same type as input)
        """
        df = data if isinstance(data, pd.DataFrame) else data
        df["new_col"] = df[self.params["column_name"]].apply(my_func)
        return df
```

Then use in template:
```yaml
reader:
  pipeline:
    - step: my_custom_step
      column_name: input_column
```

---

## Related Documentation

- [CONFIGURATION.md](CONFIGURATION.md) - Legacy function-based templates and general config
- [ETL_PIPELINE_DESIGN.md](ETL_PIPELINE_DESIGN.md) - Detailed ETL design, step reference, examples
- [PROJECT_OVERVIEW.md](PROJECT_OVERVIEW.md) - Architecture overview
- [USER_GUIDE.md](USER_GUIDE.md) - Getting started, common tasks

---

## Glossary

**Template** - YAML configuration defining how to download, parse, or transform market data

**Template Type** - Classification of template by purpose and output:
- Download & Read (Single): External source → one dataset
- Download & Read (Multi): External source → multiple datasets
- ETL (Single): Transform datasets → one derived dataset
- ETL (Multi): Transform datasets → multiple derived datasets

**Downloader** - Component that fetches raw files from external sources (URLs, APIs)

**Reader** - Component that parses downloaded files into structured DataFrames

**Pipeline Step** - Single transformation operation in a pipeline (filter, sort, type conversion)

**ETL** - Extract-Transform-Load; process of reading source data, transforming, and saving to output layer

**Layer** - Data storage level: input (raw), staging (intermediate), curated (final)

**Writer** - Component that saves DataFrames to parquet format and registers in catalog

**Field** - Column definition with name, type, description

**Fields Section** (`fields:`) - Top-level schema for **single-dataset** templates

**Datasets Section** (`datasets:`) - Multi-output schema for **multi-dataset** templates (replaces `fields:`)

**Schema** - Structure of a dataset: list of fields with types

**Dataset Catalog** - Metadata registry tracking all datasets in all layers

**Partitioning** - Splitting parquet files by column values (e.g., by date) for efficient queries

**Multi-Dataset** - Template producing multiple independent datasets from single source (e.g., XML with multiple entity types)

---

**Last updated:** January 30, 2025

# Convert b3-equities-register to Pipeline-Based ETL

## Objective

Convert [b3-equities-register.yaml](templates/b3-equities-register.yaml) from function-based to declarative pipeline-based ETL, with output partitioned by `refdate` and comprehensive testing.

## Current State

```yaml
id: b3-equities-register
description: Dataset de dados cadastrais de ações
etl:
  function: brasa.etl.copy_dataset_and_drop_duplicates
  futures_dataset: b3-bvbg028-equities
  columns:
    - refdate
    - security_id
    - security_proprietary
    - security_market
    - instrument_asset
    - instrument_asset_description
    - instrument_market
    - instrument_segment
    - instrument_description
    - security_category
    - isin
    - distribution_id
    - cfi_code
    - specification_code
    - corporation_name
    - symbol
    - payment_type
    - allocation_lot_size
    - price_factor
    - trading_start_date
    - trading_end_date
    - corporate_action_start_date
    - ex_distribution_number
    - custody_treatment_type
    - trading_currency
    - market_capitalisation
    - close
    - open
    - days_to_settlement
    - right_issue_price
    - instrument_type
    - governance_indicator
```

## Desired State

```yaml
id: b3-equities-register
description: Dataset de dados cadastrais de ações

etl:
  pipeline:
    - step: load
      input: b3-bvbg028-equities

    - step: select
      columns:
        - refdate
        - security_id
        - security_proprietary
        - security_market
        - instrument_asset
        - instrument_asset_description
        - instrument_market
        - instrument_segment
        - instrument_description
        - security_category
        - isin
        - distribution_id
        - cfi_code
        - specification_code
        - corporation_name
        - symbol
        - payment_type
        - allocation_lot_size
        - price_factor
        - trading_start_date
        - trading_end_date
        - corporate_action_start_date
        - ex_distribution_number
        - custody_treatment_type
        - trading_currency
        - market_capitalisation
        - close
        - open
        - days_to_settlement
        - right_issue_price
        - instrument_type
        - governance_indicator

    - step: apply_fields
      errors: coerce

    - step: drop_duplicates
      keep: first

writer:
  partitioning: [refdate]

fields:
  - name: refdate
    description: Reference date
    type: date
  - name: security_id
    description: Security ID
    type: string
  # ... (remaining fields)
```

## Implementation Steps

### Step 1: Update Template YAML

Replace the `etl` section in [b3-equities-register.yaml](templates/b3-equities-register.yaml):
- Remove `function` and `futures_dataset` keys
- Replace with `pipeline` section containing four steps:
  1. `load` - references `b3-bvbg028-equities` dataset
  2. `select` - specifies all 31 columns to keep
  3. `apply_fields` - applies field schema validation and type conversion (optional but recommended)
  4. `drop_duplicates` - removes duplicate rows, keeping first occurrence

Add `writer` section with `partitioning: [refdate]` to organize output by date.

Add `fields` section with field definitions for all 31 columns (optional but recommended for schema documentation and field validation).

### Step 2: Create Test File

Create new test file `tests/test_b3_equities_register_pipeline.py` with:

#### Unit Test
- Load `b3-bvbg028-equities` dataset (or use fixture with sample data)
- Execute pipeline steps using `ETLPipeline.execute()`
- Execute legacy function `brasa.etl.copy_dataset_and_drop_duplicates()` for comparison
- Assert DataFrames are equal:
  - Same column names
  - Same data types
  - Same row count after deduplication
  - Same values (allow for row order differences if appropriate)

#### Integration Test
- Use template loader to process `b3-equities-register` template
- Verify output parquet files exist in partition structure
- Validate `refdate=YYYY-MM-DD/` partition directories created
- Spot-check row counts and sample values match baseline

#### Baseline Data Strategy
- Use existing sample data from `data/` directory (if contains b3-bvbg028-equities subset)
- Or create minimal fixture with 5-10 rows from the source dataset
- Store expected output for regression testing

### Step 3: Validation Checks

Before committing:
- [ ] Template YAML is valid (check via template loader)
- [ ] All tests pass (`uv run pytest tests/test_b3_equities_register_pipeline.py`)
- [ ] Pipeline and legacy function produce identical DataFrames
- [ ] Partitioned output structure is correct
- [ ] No lint/format issues (`uv run ruff check . --fix && uv run ruff format .`)

### Step 4: Documentation (if needed)

- Update any documentation referencing this template
- Add comment in template file explaining pipeline steps (optional)

## Configuration Details

- **Partitioning**: `[refdate]` - Output organized by date
- **Deduplication**: `keep: first` - Retains first occurrence of duplicates
- **Dataset dependency**: `b3-bvbg028-equities` (no changes)
- **Column count**: 31 columns preserved from original

## Implementation Notes

### Fields Attribute

- **Not Required**: The `fields` attribute is optional in ETL templates
- **Nice to Have**: Including fields provides:
  - Schema documentation for the output dataset
  - Type validation and coercion via `apply_fields` step
  - Better IDE support and clarity
  - Field-level error handling

### Apply Fields Step

- **When to Use**: When `fields` attribute is defined, include `apply_fields` step in pipeline
- **Purpose**: Validates and transforms data according to field definitions
- **Placement**: After `select` step (column selection) and before final transformations
- **Error Handling**: Use `errors: coerce` for graceful type conversion or `errors: raise` for strict validation



| Risk | Mitigation |
|------|-----------|
| Pipeline execution differs from legacy function | Comprehensive unit test comparing outputs |
| Partition structure breaks downstream pipelines | Integration test validates output format |
| Data loss during column selection | Select step includes all original columns |
| Deduplication logic changes | Test with varied duplicate scenarios |

## Benefits

✅ Declarative configuration (no Python code)
✅ Easier to understand and maintain
✅ Automatically tracked in dependency graph
✅ Reuses shared transformation logic
✅ Supports complex multi-step transformations

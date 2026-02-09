---
description: 'Brasa template configuration instructions: guidelines for creating and modifying YAML templates that define data processing pipelines, field schemas, and validation rules for ETL workflows.'
applyTo: '**/*.yaml, **/*.yml'
---

# Template Configuration Instructions

## Overview

This instruction file guides agents when working with brasa's template system for ETL pipelines and data processing.

## Reference Documentation

The complete template system documentation is available in [docs/TEMPLATES.md](../../docs/TEMPLATES.md). Agents should refer to that document for detailed specifications.

## Key Principles

### Template Types

Brasa uses **pipeline-based templates** (modern approach) with these configurations:

1. **Download & Read Templates (Single Dataset)**: Use `reader.pipeline` with a single `dataset` field
2. **Download & Read Templates (Multi-Dataset)**: Use `reader.pipeline` with multiple `datasets`
3. **ETL Templates (Single Dataset)**: Use `etl.pipeline` with a single `dataset` field

**Legacy function-based templates** (`reader.function`, `etl.function`) are deprecated—use pipeline approach instead.

### Template Structure

When creating or modifying templates:

```yaml
# Single Dataset Template Example
name: example-template
reader:
  pipeline:
    steps:
      - step: read_json
      - step: add_column
        name: issuingCompany
        from:
          where: download_args
          key: issuingCompany
      - step: add_column
        from:
          where: extra_key
        name: refdate
      - step: apply_fields
fields:
  - name: date
    description: Reference date
    type: date("%Y-%m-%d")
  - name: value
    description: Numeric value
    type: numeric
```

```yaml
# Multi-Dataset Template Example
name: multi-example
reader:
  locale: en
  pipeline:
    steps:
      - b3_read_bvbg028_xml
      - apply_fields_multi

writer:
  partitioning: [refdate]

datasets:
  equities:
    tag: EqtyInf
    fields:
      # Header fields (common)
      - name: refdate
        description: Reference date
        tag: RptParams/RptDtAndTm/Dt
        type: date
      - name: security_id
        description: Security ID
        tag: FinInstrmId/OthrId/Id
        type: string
      - name: security_proprietary
        description: Security proprietary code
        tag: FinInstrmId/OthrId/Tp/Prtry
        type: string
      - name: security_market
        description: Security market code
        tag: FinInstrmId/PlcOfListg/MktIdrCd
        type: string
      - name: instrument_asset
        description: Instrument asset code
        tag: FinInstrmAttrCmon/Asst
        type: string

  options_on_equities:
    tag: OptnOnEqtsInf
    fields:
      # Header fields (common)
      - name: refdate
        description: Reference date
        tag: RptParams/RptDtAndTm/Dt
        type: date
      - name: security_id
        description: Security ID
        tag: FinInstrmId/OthrId/Id
        type: string
      - name: security_proprietary
        description: Security proprietary code
        tag: FinInstrmId/OthrId/Tp/Prtry
        type: string
      - name: security_market
        description: Security market code
        tag: FinInstrmId/PlcOfListg/MktIdrCd
        type: string
      - name: instrument_asset
        description: Instrument asset code
        tag: FinInstrmAttrCmon/Asst
        type: string
      - name: instrument_asset_description
        description: Instrument asset description
        tag: FinInstrmAttrCmon/AsstDesc
        type: string
```

### Field Types and Parsers

Always use typed fields with appropriate parsers:

- **Dates**: `type: date("%Y-%m-%d")` or `date("%d/%m/%Y")`
- **Numbers**: `type: numeric`
- **Text**: `type: string`
- **Categories**: `type: category`

### Pipeline Steps

Common pipeline steps in order:

1. **Read**: Parse file into DataFrame
2. **Transform**: Apply transformations (rename, filter, etc.)
3. **Validate**: Check data quality
4. **Store**: Save to parquet

### Validation Rules

- **Single dataset templates**: Must have exactly one `dataset` field
- **Multi-dataset templates**: Must have `datasets` list (2+ items)
- **ETL templates**: Use `source` to reference upstream datasets
- **Field names**: Use snake_case, must be valid Python identifiers

## Common Patterns

### Renaming Columns

```yaml
reader:
  pipeline:
    dataset:
      rename:
        "Old Name": "new_name"
        "Another Old": "another_new"
```

### Filtering Data

```yaml
reader:
  pipeline:
    dataset:
      filter:
        column_name: "expected_value"
```

### Date Handling

```yaml
fields:
  - name: reference_date
    type: date
    parser: date(%Y-%m-%d)
    source: DataReferencia  # Original column name
```

### ETL Source References

```yaml
etl:
  pipeline:
    dataset:
      name: processed_data
      source: raw_data  # Reference to upstream dataset
```

## File Locations

- **Templates**: `templates/*.yaml`
- **Field schemas**: `brasa/fieldsets/*.py`
- **Parsers**: `brasa/parsers/*.py`
- **Documentation**: `docs/TEMPLATES.md`

## When Creating New Templates

1. Check existing templates for similar patterns
2. Use pipeline-based configuration (not function-based)
3. Define typed fields with appropriate parsers
4. Follow naming conventions (snake_case)
5. Add validation rules where appropriate
6. Document any special handling in comments
7. Test with sample data before committing

## When Modifying Templates

1. Review [docs/TEMPLATES.md](../../docs/TEMPLATES.md) for validation rules
2. Ensure changes maintain backward compatibility
3. Update field types if data format changes
4. Test the full pipeline (download → read → ETL → query)
5. Update related documentation if needed

## Anti-Patterns to Avoid

❌ Mixing `dataset` (singular) with `datasets` (plural)
❌ Using function-based templates for new code
❌ Hardcoding dates or values that should be parameters
❌ Missing type information on fields
❌ Using camelCase or PascalCase for field names
❌ Skipping validation steps

## Additional Resources

- Full template specification: [docs/TEMPLATES.md](../../docs/TEMPLATES.md)
- Architecture overview: [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md)
- ETL pipeline design: [docs/ETL_PIPELINE_DESIGN.md](../../docs/ETL_PIPELINE_DESIGN.md)

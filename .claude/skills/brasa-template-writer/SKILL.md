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

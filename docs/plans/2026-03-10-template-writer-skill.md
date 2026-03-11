# Template Writer Skill Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a Claude Code skill that writes new brasa templates from scratch and migrates legacy templates to the modern pipeline-based format.

**Architecture:** Single SKILL.md file in `.claude/skills/brasa-template-writer/` containing all reference material, migration rules, and generation guidelines. The skill instructs Claude to produce YAML templates — no Python code needed.

**Tech Stack:** YAML templates, Claude Code skill system (Markdown frontmatter)

---

### Task 1: Create the skill file with frontmatter and overview

**Files:**
- Create: `.claude/skills/brasa-template-writer/SKILL.md`

**Step 1: Create the skill directory**

```bash
mkdir -p .claude/skills/brasa-template-writer
```

**Step 2: Write the SKILL.md with frontmatter, overview, and mode selection**

Write the file with:
- Frontmatter: `name: brasa-template-writer` and a `description` that triggers on template creation, migration, YAML template work
- Overview section explaining the two modes
- Mode selection: detect whether user wants to write new or migrate existing

```markdown
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
```

**Step 3: Commit**

```bash
git add .claude/skills/brasa-template-writer/SKILL.md
git commit -m "feat: create brasa-template-writer skill skeleton"
```

---

### Task 2: Add the migration rules section

**Files:**
- Modify: `.claude/skills/brasa-template-writer/SKILL.md`

**Step 1: Append migration rules**

Add a `## Migration Rules` section covering:

#### Field Type Migration

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

#### Sign Field Migration

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

#### Structural Migration

- Replace `reader: { function: ... }` with `reader: { pipeline: [...] }`
- Choose pipeline steps based on `filetype`:
  - `FWF` → `read_fwf` (dtype: str) → filter if needed → `apply_fields`
  - `CSV` → `read_csv` → `apply_fields`
  - `JSON` → `read_json` → `apply_fields`
  - `Excel/XLS` → `read_excel` → `apply_fields`
- Add `writer:` block with `layer: input` and `partitioning: [refdate]`
- Add `downloader:` block if the legacy template has URL info or if user provides it
- Remove `filename:` and `filetype:` top-level keys (these are inferred by the pipeline)

**Step 2: Commit**

```bash
git add .claude/skills/brasa-template-writer/SKILL.md
git commit -m "feat: add migration rules to template-writer skill"
```

---

### Task 3: Add the field type reference

**Files:**
- Modify: `.claude/skills/brasa-template-writer/SKILL.md`

**Step 1: Append field type reference section**

Add `## Field Type Reference` with all supported types:

```markdown
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
```

**Step 2: Commit**

```bash
git add .claude/skills/brasa-template-writer/SKILL.md
git commit -m "feat: add field type reference to template-writer skill"
```

---

### Task 4: Add the pipeline steps reference

**Files:**
- Modify: `.claude/skills/brasa-template-writer/SKILL.md`

**Step 1: Append pipeline steps reference**

Add `## Pipeline Steps Reference` organized by category. Include only the step name, key parameters, and one-line description. This is reference material for when Claude generates pipelines.

Categories:
- **I/O Steps**: `read_csv`, `read_fwf`, `read_json`, `read_excel`
- **Column Steps**: `set_columns`, `rename_columns`, `select_columns`, `drop_columns`, `add_column`, `reorder_columns`
- **Transform Steps**: `apply_fields`, `parse_numeric`, `parse_date`, `filter_rows`, `drop_duplicates`, `drop_na`, `fill_na`, `sort`, `extract_regex`, `concat_columns`, `melt`, `make_date`, `str_replace`, `cast`
- **ETL Steps**: `load`, `concat_datasets`, `dataset_filter`, `dataset_select`, `select_fields`, `dataset_sort`, `sql_query`, `to_dataframe`
- **B3-Specific**: `b3_read_bvbg028_xml`, `b3_read_bvbg086_xml`, `b3_read_bvbg087_xml`, `b3_create_symbol`, `b3_forward_fill_commodity`, `b3_extract_commodity_code`

Keep it concise — just name + key params + one-liner. Full details are in the source code.

**Step 2: Commit**

```bash
git add .claude/skills/brasa-template-writer/SKILL.md
git commit -m "feat: add pipeline steps reference to template-writer skill"
```

---

### Task 5: Add canonical template examples

**Files:**
- Modify: `.claude/skills/brasa-template-writer/SKILL.md`

**Step 1: Append canonical examples section**

Add `## Canonical Examples` with four complete templates that serve as models:

1. **Single-dataset FWF reader** — based on `b3-cotahist-daily.yaml`:
   - `read_fwf` → `filter_rows` → `apply_fields`
   - Shows width-based fields, date and numeric types

2. **Single-dataset CSV reader** — based on `cvm-companies-registration.yaml`:
   - `read_csv` → `add_column` → `rename_columns` → `apply_fields`
   - Shows separator, encoding, rename mapping

3. **Multi-dataset XML reader** — based on `b3-bvbg028.yaml`:
   - Custom XML reader → `apply_fields_multi`
   - Shows `datasets:` block with tags and per-dataset fields

4. **ETL with SQL** — based on `b3-equities-returns.yaml`:
   - `sql_query` → `apply_fields`
   - Shows upstream dataset references, SQL query, staging layer

Each example should be a complete, minimal YAML template (trimmed to ~15-20 fields max for brevity).

**Step 2: Commit**

```bash
git add .claude/skills/brasa-template-writer/SKILL.md
git commit -m "feat: add canonical template examples to template-writer skill"
```

---

### Task 6: Update memory file

**Files:**
- Modify: `/home/wilson/.claude/projects/-home-wilson-dev-python-brasa/memory/MEMORY.md`

**Step 1: Add entry for the new skill**

Under `## Skills`, add:

```markdown
### brasa-template-writer
- Location: `.claude/skills/brasa-template-writer/SKILL.md`
- Purpose: Write new YAML templates from scratch or migrate legacy templates to modern format
- Trigger: user asks to create, write, migrate, or convert templates
```

**Step 2: Commit**

```bash
git add /home/wilson/.claude/projects/-home-wilson-dev-python-brasa/memory/MEMORY.md
git commit -m "docs: add brasa-template-writer skill to memory"
```

---

### Task 7: Test the skill with a dry run

**Step 1: Verify the skill file is well-formed**

Read the complete SKILL.md and check:
- Frontmatter has valid `name:` and `description:`
- All sections are present and properly formatted
- YAML examples are syntactically valid
- No broken markdown

**Step 2: Verify skill is discoverable**

Check that the skill appears in the `.claude/skills/` directory alongside `brasa-db-explorer`.

```bash
ls -la .claude/skills/
```

Expected: both `brasa-db-explorer/` and `brasa-template-writer/` directories.

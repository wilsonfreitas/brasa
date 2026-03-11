# Template Writer Skill Design

## Overview

A Claude Code skill with two modes: **write new templates from scratch** and **migrate legacy templates** to the modern pipeline-based format. Supports all three template types (single-dataset reader, multi-dataset reader, ETL).

## Mode 1: Write from Scratch

**Input:** User provides a description or spec. If information is missing, ask interactively.

**Flow:**

1. Determine template type (reader single/multi, ETL)
2. Gather: data source URL, file format, fields, partitioning, layer
3. Generate complete YAML with appropriate pipeline steps
4. Save to `templates/` using the existing directory structure convention (e.g., `templates/b3/equities/`)

Template structure follows modern conventions: `id`, `description`, `downloader` (for reader templates), `reader.pipeline` or `etl.pipeline`, `writer`, `fields`/`datasets`.

## Mode 2: Migrate Legacy Templates

### Field Transformations

| Legacy Pattern | Migration |
|---|---|
| `handler: {type: numeric, dec: 2.0}` | `type: numeric(dec=2.0)` |
| `handler: {type: Date, format: '%Y%m%d'}` | `type: date(format='%Y%m%d')` |
| `handler: {type: POSIXct, format: '%H%M%S'}` | `type: datetime(format='%H%M%S')` |
| `handler: {type: character}` | `type: character` |
| `handler: {type: factor, levels: [...], labels: [...]}` | `type: character` |

### Sign Field Handling

For numeric fields with `sign: sinal_field` references:

- Add a YAML comment on the field noting the sign dependency
- Generate pipeline steps after `apply_fields` that multiply the value by the parsed sign column, then drop all sign columns

### Structural Migrations

| Legacy Pattern | Migration |
|---|---|
| `reader.function: ...` | `reader.pipeline:` with appropriate steps |
| Missing `writer:` | Add `writer:` with layer and partitioning |
| Missing `downloader:` | Add `downloader:` if URL/source info available |

Migrated templates go to the appropriate `templates/` subdirectory (not `templates/legacy/`).

## Skill Content

The skill embeds:

- Complete list of registered pipeline steps (IO, transform, column, ETL, B3-specific)
- Field type syntax reference
- Canonical examples of each template type (single-dataset FWF, single-dataset CSV, multi-dataset XML, ETL with SQL)

## Decisions

- **Factor types** become `character` (not preserved as factors)
- **No validation** of generated templates (user tests manually)
- **File placement** always in `templates/` following existing directory conventions
- **Interactive or spec-based** creation: ask questions if info is missing, generate directly if full spec provided

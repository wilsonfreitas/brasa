# Brasa CLI Reference

The brasa CLI provides commands for downloading, processing, and querying Brazilian financial market data. All commands are run via:

```bash
uv run python -m brasa.cli <command> [options]
```

Or, if installed as a package:

```bash
brasa <command> [options]
```

---

## Commands at a Glance

| Group | Command | Purpose |
|-------|---------|---------|
| Setup | `setup` | Initialize cache directories and metadata database |
| Execution | `download` | Download raw market data files |
| Execution | `process` | Parse raw files into Parquet datasets |
| Execution | `run` | Download + process with automatic dependency resolution |
| Templates | `deps` | Show upstream/downstream dependencies |
| Templates | `plan` | Show execution plan for a template |
| Templates | `graph` | Export dependency graph (DOT, ASCII, PNG, SVG, PDF) |
| Datasets | `head` | Preview first N rows of a dataset |
| Datasets | `list-datasets` | List all registered datasets |
| Datasets | `describe-dataset` | Show schema and metadata for a dataset |
| Datasets | `list-unprocessed` | List templates with downloaded but unprocessed files |
| Datasets | `sync-catalog` | Register on-disk datasets not yet in catalog |
| Database | `create-views` | Create DuckDB views for all datasets |
| Database | `create-view` | Create DuckDB view for specific templates |
| Database | `list-tables` | List available DuckDB tables/views |
| Database | `query` | Execute SQL queries against the database |
| Maintenance | `doctor` | Diagnose cache health issues |

---

## Setup

### `setup`

Creates the `.brasa-cache/` directory structure and initializes `metadata.db`. Run this once before using other commands, or set `BRASA_DATA_PATH` to change the cache location.

```bash
brasa setup
```

---

## Execution

### `download`

Downloads raw market data files for one or more templates. Files are stored in `.brasa-cache/raw/`.

```bash
brasa download <template> [<template> ...] [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--arg KEY=VALUE` | Pass typed arguments to the download (repeatable) |
| `--calendar {B3,ANBIMA}` | Default calendar for date arguments (default: B3) |
| `--force` | Re-download even if files exist in cache |
| `--plan FILE` | Use a download plan YAML file instead of template names |
| `-v / --verbose` | Show each download task on its own line |
| `-q / --quiet` | Only show summary if there are errors |
| `--report FILE` | Save download report to file (.json or .txt) |

#### The `--arg` DSL

The `--arg` flag accepts `KEY=VALUE` pairs where the value is parsed through a type-aware DSL:

| Prefix | Meaning | Example | Resolves To |
|--------|---------|---------|-------------|
| `@` | Date or date range | `@2026-03-15` | Single date |
| `@` | Month range | `@2026-01` | All business days in January 2026 |
| `@` | Explicit range | `@2026-01-01:2026-01-31` | Date range |
| `@...~CAL` | Calendar override | `@2026-01~ANBIMA` | Month range using ANBIMA calendar |
| `$` | Symbol lookup | `$index` | List of index symbols |
| *(numeric)* | Integer | `2026` | `2026` |
| *(other)* | Plain string | `IBOV` | `"IBOV"` |

Commas split values into lists, with each element parsed individually: `IBOV,BOVA11` becomes `["IBOV", "BOVA11"]` and `2024,2025` becomes `[2024, 2025]`.

#### Use Cases

**Download daily stock prices for January 2026:**

```bash
brasa download b3-cotahist-daily --arg refdate=@2026-01
```

**Download a specific date:**

```bash
brasa download b3-bvbg087 --arg refdate=@2026-03-15
```

**Download a date range:**

```bash
brasa download b3-bvbg087 --arg refdate=@2026-01-01:2026-03-31
```

**Download using the ANBIMA business calendar:**

```bash
brasa download anbima-debentures --arg refdate=@2026-01~ANBIMA
```

**Download with multiple arguments:**

```bash
brasa download my-template --arg year=2026 --arg index=IBOV
```

**Force re-download with verbose output:**

```bash
brasa download b3-cotahist-daily --arg refdate=@2026-03-15 --force -v
```

**Download using a plan file:**

```bash
brasa download --plan daily-b3.yaml
```

**Override dates in a plan:**

```bash
brasa download --plan daily-b3.yaml --arg refdate=@2026-01
```

**Download multiple templates at once:**

```bash
brasa download b3-cotahist-daily b3-bvbg087 --arg refdate=@2026-03-15
```

#### Status Codes

During download, each file shows a single-character status:

| Code | Meaning |
|------|---------|
| `.` | Passed (downloaded successfully) |
| `F` | Failed |
| `E` | Error |
| `S` | Skipped (already exists) |
| `D` | Duplicated (same checksum) |
| `I` | Invalid |
| `C` | Corrupted |

---

### `process`

Parses downloaded raw files into Parquet datasets stored in `.brasa-cache/db/`. Handles both regular templates (raw-to-input) and ETL templates (input-to-staging or staging-to-curated).

```bash
brasa process <template> [<template> ...] [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--reprocess` | Reprocess all files, even if already processed |
| `-v / --verbose` | Verbose output |
| `-q / --quiet` | Quiet output |
| `--report FILE` | Save report to file |

**Use Cases:**

```bash
# Process a single template
brasa process b3-cotahist-daily

# Process multiple templates
brasa process b3-cotahist-daily b3-bvbg087

# Force reprocessing of all files
brasa process b3-cotahist-daily --reprocess

# Process an ETL template (input -> staging)
brasa process b3-equities-returns
```

---

### `run`

Executes a template with automatic dependency resolution. Builds a dependency graph, determines which upstream templates need to run, and executes them in order.

```bash
brasa run <template> [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--force` | Re-execute all upstream templates regardless of staleness |
| `--dry-run` | Show execution plan without running anything |
| `-v / --verbose` | Verbose output |
| `-q / --quiet` | Quiet output |
| `--report FILE` | Save report to file |

**Use Cases:**

```bash
# Run a template and all its dependencies
brasa run b3-equities-returns

# Preview what would be executed
brasa run b3-equities-returns --dry-run

# Force full re-execution of the pipeline
brasa run b3-equities-returns --force
```

---

## Templates

### `deps`

Shows the dependency tree for a template: direct upstream/downstream dependencies, all ancestors, and output datasets.

```bash
brasa deps <template>
```

**Example:**

```bash
brasa deps b3-equities-returns
```

Output shows:
- Template type (download or etl)
- Output datasets
- Direct upstream dependencies
- All ancestors (transitive)
- Direct downstream dependents

---

### `plan`

Shows the execution plan for a template — the ordered list of steps that `run` would execute.

```bash
brasa plan <template> [--force]
```

**Use Cases:**

```bash
# Show execution plan (only stale steps)
brasa plan b3-equities-returns

# Show full plan (all ancestors marked for execution)
brasa plan b3-equities-returns --force
```

---

### `graph`

Exports the full dependency graph or a subgraph for a specific template.

```bash
brasa graph [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--format {dot,ascii,png,svg,pdf}` | Output format (default: dot) |
| `--output FILE` | Write output to file (required for png/svg/pdf) |
| `--template NAME` | Show only the subgraph for this template |

**Use Cases:**

```bash
# Print DOT format to stdout
brasa graph

# Render ASCII tree
brasa graph --format ascii

# Export PNG (requires graphviz installed)
brasa graph --format png --output deps.png

# Show subgraph for one template
brasa graph --template b3-equities-returns --format ascii
```

---

## Datasets

### `head`

Previews the first N rows of a dataset. Works like Unix `head` but for Parquet datasets.

```bash
brasa head <layer.dataset> [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `-n, --lines N` | Number of rows (default: 10) |
| `-o, --output FILE` | Output to file (.csv, .json, .parquet, .xlsx) |
| `-w, --width N` | Terminal width override |
| `--max-colwidth N` | Max column content width (default: 50) |
| `-c, --columns COL [COL ...]` | Show only specific columns |
| `--wrap` | Wrap columns across multiple rows |

**Use Cases:**

```bash
# Preview daily stock prices
brasa head input.b3-cotahist-daily

# Show 5 rows with specific columns
brasa head staging.b3-equities-returns -n 5 -c refdate symbol pct_return

# Export to CSV
brasa head input.b3-cotahist-daily -n 100 -o sample.csv

# Wide dataset with wrapping
brasa head input.b3-bvbg087 --wrap
```

---

### `list-datasets`

Lists all datasets registered in the catalog.

```bash
brasa list-datasets [--layer {input,staging,curated}] [--format {table,json}]
```

**Use Cases:**

```bash
# List all datasets
brasa list-datasets

# List only staging datasets
brasa list-datasets --layer staging

# Get JSON output for scripting
brasa list-datasets --format json
```

---

### `describe-dataset`

Shows detailed metadata and schema for a specific dataset.

```bash
brasa describe-dataset <layer.dataset> [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--compare-template` | Compare catalog schema with the template's expected schema |
| `--format {text,json}` | Output format (default: text) |

**Use Cases:**

```bash
# Describe a dataset
brasa describe-dataset input.b3-cotahist-daily

# Check for schema drift
brasa describe-dataset input.b3-cotahist-daily --compare-template
```

---

### `list-unprocessed`

Lists templates that have downloaded files not yet processed into Parquet.

```bash
brasa list-unprocessed [--format {table,json}]
```

---

### `sync-catalog`

Scans the `db/` folder for Parquet datasets and registers any that are not yet in the catalog.

```bash
brasa sync-catalog [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--layer {input,staging,curated}` | Scan only a specific layer |
| `--dry-run` | Preview without making changes |
| `--force` | Overwrite existing catalog entries |
| `-v, --verbose` | Show detailed output |
| `--format {text,json}` | Output format |

---

## Database

### `create-views`

Creates DuckDB views for all datasets, making them queryable via SQL. Required before using `query` or `list-tables`.

```bash
brasa create-views [--layer {raw,input,staging,curated}]
```

---

### `create-view`

Creates a DuckDB view for specific templates.

```bash
brasa create-view <template> [<template> ...]
```

---

### `list-tables`

Lists available tables/views in the DuckDB database.

```bash
brasa list-tables [--layer {raw,input,staging,curated}] [-v]
```

With `-v`, shows row counts for each table.

---

### `query`

Executes read-only SQL queries against the DuckDB database. Automatically creates views on first use.

```bash
brasa query "<SQL>" [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `-o, --output FORMAT` | Output: display (default), or file path (.csv, .json, .parquet, .xlsx, .orc) |
| `--list-tables` | List available tables and exit |
| `-v, --verbose` | Show query execution plan |

**Use Cases:**

```bash
# Query stock prices
brasa query "SELECT * FROM \"input.b3-cotahist-daily\" WHERE symbol = 'PETR4' LIMIT 10"

# Export query results to CSV
brasa query "SELECT * FROM \"staging.b3-equities-returns\" LIMIT 1000" -o returns.csv

# List available tables
brasa query "" --list-tables

# Show query plan
brasa query "SELECT COUNT(*) FROM \"input.b3-cotahist-daily\"" -v
```

Note: Table names contain dots and hyphens, so they must be double-quoted in SQL.

---

## Maintenance

### `doctor`

Diagnoses cache health: finds orphan files, missing data, schema drift, and date gaps.

```bash
brasa doctor [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--fix` | Apply all auto-fixable issues |
| `--yes` | Skip confirmation prompt when using `--fix` |
| `--category {raw,db,meta,templates,gaps}` | Run only specific check categories |
| `--template TEMPLATE [...]` | Restrict gap/stale checks to specific templates |
| `--since DAYS` | Look back N days for gap checks (default: 30) |

**Use Cases:**

```bash
# Full health check
brasa doctor

# Check and auto-fix issues
brasa doctor --fix

# Check only date gaps for the last 7 days
brasa doctor --category gaps --since 7

# Check specific templates
brasa doctor --template b3-cotahist-daily b3-bvbg087
```

---

## Common Workflows

### Daily data refresh

```bash
# Download today's data
brasa download b3-cotahist-daily b3-bvbg087 --arg refdate=@2026-03-15

# Process raw files into Parquet
brasa process b3-cotahist-daily b3-bvbg087

# Run ETL to compute returns
brasa process b3-equities-returns

# Create/refresh DuckDB views
brasa create-views
```

### Backfill a month of data

```bash
brasa download b3-cotahist-daily --arg refdate=@2026-01
brasa process b3-cotahist-daily
```

### End-to-end with dependency resolution

```bash
# Downloads, processes, and runs ETL for the full dependency chain
brasa run b3-equities-returns
```

### Explore available data

```bash
# What datasets exist?
brasa list-datasets

# Preview a dataset
brasa head staging.b3-equities-returns -n 20

# Query with SQL
brasa query "SELECT symbol, AVG(pct_return) as avg_ret FROM \"staging.b3-equities-returns\" GROUP BY symbol ORDER BY avg_ret DESC LIMIT 10"
```

### Diagnose and fix issues

```bash
# Check health
brasa doctor

# See what needs processing
brasa list-unprocessed

# Sync catalog with what's on disk
brasa sync-catalog --dry-run
```

---

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `BRASA_DATA_PATH` | Root directory for the brasa cache | `.brasa-cache/` |

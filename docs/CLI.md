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
| Setup | `init` | Choose the data directory and persist it |
| Execution | `download` | Download raw market data files |
| Execution | `import` | Import local files into a template (no download) |
| Execution | `process` | Parse raw files into Parquet datasets |
| Execution | `run` | Download + process with automatic dependency resolution |
| Templates | `deps` | Show upstream/downstream dependencies |
| Templates | `plan` | Show execution plan for a template |
| Templates | `graph` | Export dependency graph (DOT, ASCII, PNG, SVG, PDF) |
| Templates | `map` | Global dependency-ordered staleness report |
| Templates | `list-templates` | List discovered templates and their source |
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
| Maintenance | `cache drop` | Drop a cache entry by meta id |

---

## Setup

### `init`

Chooses the brasa data directory and persists it in `~/.config/brasa/config.toml`. Run this once on a fresh install — data-touching commands fail with an actionable error until brasa is configured (either via `init` or the `BRASA_DATA_PATH` environment variable, which always takes precedence over the config file).

```bash
brasa init [--data-path PATH] [--yes]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--data-path PATH` | Use `PATH` directly, without prompting (scripts/CI) |
| `-y`, `--yes` | Accept the suggested default without prompting |

Without flags in an interactive terminal, `init` suggests a default (the previously persisted path, or the platform data dir such as `~/.local/share/brasa`) and asks for confirmation or a custom path. With non-interactive stdin it accepts the default automatically. Re-running `init` reconfigures: the current persisted value becomes the suggestion. The cache substructure (`raw/`, `db/`, `meta/`, metadata DB) is created lazily on first use.

---

## Execution

### `download`

Downloads raw market data files for one or more templates. Files are stored in `raw/` under the brasa home.

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

#### Flag precedence with `--plan`

Under `--plan`, top-level CLI flags override the plan's YAML defaults for that
run — a download plan behaves like the direct-template path, just with YAML
syntax sugar:

- `--force` / `--update` — when passed, apply to every task (one-directional:
  they can only turn the behavior on; omit them to use the plan's YAML).
- `--calendar NAME` — overrides `defaults.calendar` for date resolution.
- `--arg KEY=VALUE` — a global override applied **only** to tasks whose template
  declares `KEY`; it wins over that task's YAML value. If no task in the plan
  accepts `KEY`, the run fails fast with an error (rather than silently doing
  nothing).
- `--since DATE` — requires smart update: either `--update`, or a plan that sets
  `smart_update: true`. Passing `--since` when no task will run smart update
  fails fast with an error.
- `--update` and `--arg refdate=...` are mutually exclusive (smart update
  auto-resolves dates), in both the plan and direct-template paths.

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

### `import`

Imports local files into a template, reusing the same validate → gzip → checksum-dedup → parse → store pipeline as `download` — only the acquisition step differs (read from disk instead of HTTP). Useful for one-off manual ingestion, backfilling from files handed over out-of-band, or any source with no download URL.

```bash
brasa import <template> [<template> ...] [options]
```

Works in two modes:

- **Backfill an existing download template** — pass `--path` to point at the file on disk, plus the template's normal args (e.g. `refdate`). No template changes required.
- **Standalone import template** — a template written with an `importer:` block (see [TEMPLATES.md](TEMPLATES.md#importing-local-files-importer)) carries its own default path pattern, so `--path` can be omitted unless you want to override it.

**Options:**

| Flag | Description |
|------|-------------|
| `--path PATH` | Local file path or pattern (e.g. `/data/%Y-%m-%d.csv`). Overrides the template's `path:`. Required if the template has no `importer:` block. |
| `--arg KEY=VALUE` | Template arguments, repeatable — same `--arg` DSL as `download` (see above) |
| `--calendar {B3,ANBIMA}` | Calendar for date range expansion (default: B3) |
| `--force` | Re-import even if a matching entry already exists in cache |
| `-v / --verbose` | Show each import task on its own line |
| `-q / --quiet` | Only show summary if there are errors |
| `--report FILE` | Save import report to file (.json or .txt) |

The `--path` pattern supports strftime codes and `{name}` placeholders together, rendered as `refdate.strftime(path)` then `path.format(**other_args)` — so `/data/{asset}/%Y-%m-%d.csv` resolves in one shot from `--arg asset=PETR4 --arg refdate=@2026-06-20`.

#### Use Cases

**Backfill a single date into a template that normally downloads via HTTP:**

```bash
brasa import b3-cotahist-daily --path /data/backfill/COTAHIST_D02012024.TXT --arg refdate=2024-01-02
```

**Bulk import one file per business day using a date pattern:**

```bash
brasa import my-daily-template --path '/data/prices/%Y-%m-%d.csv' --arg refdate=@2026-06-01:2026-06-30
```

Each expanded date becomes its own cache entry; a date with no matching file is reported as `F` (Failed) rather than aborting the whole batch, so gaps in a backfill are easy to spot.

**Import via a standalone `importer:`-block template (path comes from the template):**

```bash
brasa import vendor-manual-upload --arg refdate=2026-06-20
```

**Force re-import of a corrected file:**

```bash
brasa import b3-cotahist-daily --path /data/corrected/file.txt --arg refdate=2024-01-02 --force
```

**Notes:**

- The source file is only ever **read**, never moved or deleted — the cache's gzipped copy is the canonical artifact, so re-imports are always repeatable.
- Status codes are the same as `download` (see above); a missing source file reports as `F`, not a crash.
- Retries are always disabled for imports — a missing local file won't be retried.

---

### `process`

Parses downloaded raw files into Parquet datasets stored in `db/` under the brasa home. Handles both regular templates (raw-to-input) and ETL templates (input-to-staging or staging-to-curated).

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

### `map`

Print a global, dependency-ordered staleness report covering every download and ETL template.

```bash
brasa map [--format flat|grouped|tree] [--all] [--reverse] [--no-color]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--format {flat,grouped,tree}` | Output layout (default: flat) |
| `--all` | Include up-to-date templates (status `ok`) |
| `--reverse` | With `--format tree`: root at leaves, branch upward to sources |
| `--no-color` | Disable ANSI colors (auto-disabled when stdout is not a TTY) |

**Statuses:** `stale` (red), `never-run` (yellow), `ok` (green; only with `--all`).

**Exit code:** `0` if nothing is stale, `1` otherwise. Suitable for CI / pre-merge checks.

**Example:**

```
$ brasa map
1. [download]  b3-bvbg028        stale  12 unprocessed entries
2. [download]  b3-cotahist-daily stale  3 unprocessed entries
3. [etl]       brasa-companies   stale  upstream 'b3-bvbg028' newer
4. [etl]       brasa-prices      stale  upstream 'b3-cotahist-daily' newer
```

---

### `list-templates`

List all discovered templates and where each comes from — `bundled`, or a user
directory from `BRASA_TEMPLATE_PATH`. User templates that override a bundled
name are marked `*shadows bundled`.

```bash
brasa list-templates
```

```
NAME              SOURCE
b3-futures        bundled
my-custom-etl     /home/me/my-templates (user)
b3-cotahist       /home/me/my-templates (user) *shadows bundled
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

Diagnoses cache health: orphan files, missing data, schema drift, metadata errors, stale ETL outputs, date gaps, download coverage, and declarative data validations.

```bash
brasa doctor [options]
```

**Options:**

| Flag | Description |
|------|-------------|
| `--fix` | Apply all auto-fixable issues |
| `--yes` | Skip confirmation prompt when using `--fix` |
| `--category {raw,db,meta,templates,gaps,validations,downloads}` | Run only specific check categories |
| `--template TEMPLATE [...]` | Restrict the `date-gaps`, `stale-etl`, `missing-etl-source` and `downloads` checks to specific templates |
| `--calendar NAME` | Business calendar for the `gaps` and `downloads` categories (default: B3) |
| `--last N\|all` | For the `gaps` and `downloads` categories, look back N days; `all` (or `-1`) reviews the full history (default: 30) |
| `--validations-file FILE` | Path to a validations YAML file (required for the `validations` category) |

**Exit codes:** `0` when no error-severity issue is found, `1` when at least one is, `2` on usage errors (e.g. `--category validations` without `--validations-file`). Suitable for CI.

**Use Cases:**

```bash
# Full health check
brasa doctor

# Check and auto-fix issues
brasa doctor --fix

# Check only date gaps for the last 7 days
brasa doctor --category gaps --last 7

# Review the full history instead of the default 30-day window
brasa doctor --category gaps downloads --template b3-cotahist-daily --last all

# Check specific templates
brasa doctor --template b3-cotahist-daily b3-bvbg087

# Check declarative completeness rules (e.g. CDI in staging.bcb-sgs)
brasa doctor --category validations --validations-file validations.yaml
```

The `validations` category always requires `--validations-file` — there is no packaged default rules file. On a broad run (no `--category`), validations are silently skipped with an info note when no file is given.

#### Coverage categories — `gaps` vs `downloads`

Both categories answer "which business days are missing?", but they inspect different pipeline stages, so together they localize a failure:

| | `gaps` | `downloads` |
|---|---|---|
| Inspects | `refdate=` partitions in `db/` (processed parquet) | `refdate` download args in the cache metadata |
| Answers | was the day **processed**? | was the day **downloaded**? |
| Scope | all refdate-partitioned datasets; `--template` optional | silent unless `--template` is given |

A day missing from `downloads` means acquisition failed — re-download it. A day present in `downloads` but missing from `gaps` means processing failed — re-process it. Both categories honor `--calendar` and `--last`, so their reports can be compared directly.

```bash
brasa doctor --category gaps downloads --template b3-trades-intraday --calendar B3
```

The `downloads` category reports, per template: missing business days in the window (`error`), downloaded dates that are not business days of the calendar (`info`, helps spot a wrong `--calendar`), or a `warning` when a template has no downloaded refdates in the metadata.

---

### `cache drop`

Drops a single cache entry by its meta id: deletes the raw files, the metadata row, and any download trials. Prompts for confirmation unless `--yes` is given.

```bash
brasa cache drop <META_ID> [--yes]
```

Look up meta ids in the metadata database (`cache_metadata.id`) or via `CacheManager()`.

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
| `BRASA_DATA_PATH` | Root directory for the brasa cache; overrides the `data_path` persisted by `brasa init` | unset (falls back to `~/.config/brasa/config.toml`; error if neither is configured) |

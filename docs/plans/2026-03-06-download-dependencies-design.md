# Download Template Dependencies — Design

## Overview

Download templates that require dynamic arguments sourced from existing datasets
should be able to declare those dependencies declaratively in the template YAML.
When a template is executed, the system automatically runs the required upstream
templates first, queries their output datasets in-memory, and injects the resolved
values into the downloader args before any download begins.

---

## Motivation

Templates like `b3-company-info` require an `issuingCompany` argument that must
be populated from `staging.b3-equities-instrument-assets`. Currently, the user
must run the upstream ETL manually and pass the values explicitly. This feature
makes the dependency explicit in the template and automates the resolution.

---

## YAML Syntax

A top-level `dependencies` block is added to download templates. Each entry is
keyed by the arg name it resolves:

```yaml
dependencies:
  - issuingCompany:
      required: true
      from:
        datasets:
          - staging.b3-equities-instrument-assets
        query: "SELECT DISTINCT instrument_asset FROM 'staging.b3-equities-instrument-assets'"
```

### Fields

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `<arg-name>` | yes | — | The downloader arg to populate |
| `required` | no | `true` | If `true`, abort on failure; if `false`, warn and continue |
| `from.datasets` | yes | — | List of datasets to register as DuckDB views (same format as `sql_query` step: `layer.dataset-name`) |
| `from.query` | yes | — | SQL query executed in-memory against the registered views; must return a single column |

Multiple args can be declared in the same `dependencies` block. Multiple datasets
can be listed under a single `from` block (for joined queries).

### Example with multiple dependencies

```yaml
dependencies:
  - issuingCompany:
      required: true
      from:
        datasets:
          - staging.b3-equities-instrument-assets
        query: "SELECT DISTINCT instrument_asset FROM 'staging.b3-equities-instrument-assets'"
  - someOtherArg:
      required: false
      from:
        datasets:
          - input.some-other-dataset
        query: "SELECT DISTINCT some_column FROM 'input.some-other-dataset'"
```

---

## Skip Condition

If the caller (download plan, CLI, or Python API) already supplies a value for
an arg, the entire dependency entry for that arg is skipped — no dataset lookup,
no template execution, no SQL query. This allows the user to override the
dependency resolution explicitly:

```yaml
# In a download plan — dependency resolution for issuingCompany is skipped
tasks:
  - template: b3-company-info
    args:
      issuingCompany: [ABEV, ITUB]
```

---

## Architecture

### New class: `DependencyResolver`

**Location**: `brasa/engine/dependency_resolver.py`

Responsibilities:

1. Parse the `dependencies` block from a `MarketDataTemplate`.
2. For each declared arg, check if it is already supplied by the caller — skip if so.
3. For each unsupplied arg, resolve the producing template(s) via
   `TemplateDependencyGraph.reverse_index` and execute them using the existing
   `process_marketdata` / `process_etl` functions. Each dependency runs
   independently with its own defaults — no args are forwarded from the caller.
4. Run the `from.query` in an in-memory DuckDB connection (same pattern as
   `RunQueryStep`), registering each dataset in `from.datasets` as a view via
   `get_dataset`.
5. Return the single-column query result as a list of values.
6. Inject the resolved values into `downloader.args` before execution.

### Integration point

`download_marketdata` calls `DependencyResolver.resolve(template, caller_args)`
before starting downloads. No changes are required to the orchestrator, CLI, or
download plan — they all go through `download_marketdata`.

### Dataset → template resolution

`TemplateDependencyGraph.reverse_index` maps `layer/dataset-name` → `template_id`.
The resolver uses this to find which template to run for each dataset declared in
`from.datasets`. Unknown datasets are caught at resolve time (fail-fast, before
any execution).

---

## Error Handling

| Situation | Behaviour |
|-----------|-----------|
| Required dependency fails (execution error or SQL returns no rows) | Raise `DependencyResolutionError` with template name, arg name, and cause. Aborts before any download starts. |
| Optional dependency fails (`required: false`) | Log a warning. Run the SQL query against whatever data already exists in the dataset (potentially stale). If the query returns rows, inject values and continue. If the query returns nothing, the download runs with no args and fails naturally. |
| Dataset not found in `reverse_index` | Raise `DependencyResolutionError` immediately (fail-fast at resolve time). Catches YAML typos before any execution. |
| Circular dependency | Reuse the existing `CyclicDependencyError` detection in `TemplateDependencyGraph`. |
| Caller supplies the arg | Skip the dependency entry entirely — no validation, no execution. |

`DependencyResolutionError` is a new exception subclassing the existing brasa
exception hierarchy (`brasa/engine/exceptions.py`).

---

## Testing

| Test | Description |
|------|-------------|
| Skip-if-provided | Resolver does nothing when caller supplies the arg |
| Happy path | Dependency runs, SQL returns values, args are injected correctly |
| Required failure — execution error | `DependencyResolutionError` raised, download does not start |
| Required failure — SQL returns no rows | `DependencyResolutionError` raised |
| Optional failure — execution error | Warning logged, SQL runs against stale data |
| Optional failure — query returns nothing | Warning logged, download runs with no args and fails naturally |
| Unknown dataset in `reverse_index` | Fail-fast `DependencyResolutionError` at resolve time |
| Circular dependency | `CyclicDependencyError` raised |
| Integration | `b3-company-info` with a pre-populated `staging.b3-equities-instrument-assets` fixture |

# Changelog

## Unreleased

### Fixed

- `brasa <listing> | head` no longer ends with a `BrokenPipeError` traceback:
  the CLI stops quietly when the reader closes the pipe (WIL-211).
- `get_industry_sectors()` and `get_symbols("equity")`, `get_symbols("equity", sector=...)`
  and `get_symbols("industry-sector" | "industry-subsector" | "industry-segment")`
  failed: they read `b3-equity-symbols-properties`, which no template produces.
  They now read `staging.brasa-companies` and `staging.b3-companies-symbols`;
  the equity list keeps shares, units and BDRs (no rights or receipts) (WIL-206).
- `get_symbols("equity", index=...)` failed (the composition column is now
  `symbol`), and `end_month="2024-12-31"` now matches the dataset's `year` +
  `end_month` period. `get_symbols("etf" | "fii" | "fietf")` returned empty
  lists: they read the raw `input.b3-listed-funds` with old type names. They now
  read `staging.b3-listed-funds` with B3's current types (`ETF`, `FII`,
  `ETF-RF`), each on its own latest date (WIL-206).

## v0.5.0 (2026-11-01) — brasa-core: templates move to brasa-templates

The PyPI distribution is renamed from `brasa-marketdata` to **`brasa-core`**,
and the templates now live in their own repo,
[brasa-templates](https://github.com/wilsonfreitas/brasa-templates). brasa is
a template-driven ETL engine; the templates are the data. `import brasa` and
the `brasa` command do not change.

### Upgrading from 0.4

1. `pip uninstall brasa-marketdata && pip install brasa-core`.
   (`pip install -U brasa-marketdata` only prints this command and keeps
   0.4.0 installed.)
2. Run `brasa init`. It keeps your data directory and clones the official
   templates into `<data path>/templates`. Use `brasa init --templates
   <url|path>` for a fork or for a folder you already have.
3. Personal templates: put them in a template folder (see *Template format
   changes* below) and add it to `BRASA_TEMPLATE_PATH` or `template_path`.
4. `brasa doctor --category validations --validations-file F` is now
   `brasa validate --rules F`.

### Breaking changes

- brasa ships no templates. Templates come only from template folders:
  `BRASA_TEMPLATE_PATH`, then `template_path` in `config.toml`. With none,
  commands that load templates fail with "No template folders configured.
  Run `brasa init` or set BRASA_TEMPLATE_PATH." and name any configured
  folder that is missing; `brasa info` still works. (WIL-205)
- The distribution is renamed `brasa-marketdata` → `brasa-core`. A last
  `brasa-marketdata` 0.5.0 installs nothing: it stops with the command to
  switch, and an installed 0.4.0 keeps working. (WIL-205)
- `brasa doctor` has no validations category and no `--validations-file`;
  use `brasa validate`. (WIL-201)
- Templates are read only from a folder's `templates/` subfolder. (WIL-198)

### Features

- Template folders: `<folder>/{brasa.toml, templates/, plans/, validations/,
  steps.py}`. `brasa.toml` declares `requires-brasa`, and a mismatch is an
  error. `brasa info` lists the folders with their manifest status and git
  origin and commit; `brasa list-templates` shows where each template comes
  from and marks `*shadows`. (WIL-198)
- `brasa init` clones brasa-templates; `brasa init --templates <url|path>`
  uses a fork or an existing folder. `brasa update-templates` runs
  `git pull --ff-only` in each git folder. (WIL-199)
- `brasa validate [layer.dataset ...] [--rules FILE]` checks datasets against
  rules from inline `validations:` blocks in templates, `validations/` files
  in template folders, and an extra rules file. Skipped rules are shown.
  Exit codes: 0 clean, 1 rule failures, 2 bad arguments. (WIL-201)
- Download plans by name: `brasa download --plan daily-b3` finds
  `plans/daily-b3.yaml` in the template folders; new `brasa list-plans`.
  (WIL-202)
- Custom pipeline steps: a template folder's `steps.py` (or `steps/`
  package) is imported when templates load, so its `@StepRegistry.register`
  steps work in its templates. (WIL-200)

### Fixes

- `sql_export` works with DuckDB 2.0, which a fresh install now gets: it
  failed with `'DuckDBPyConnection' object has no attribute 'arrow'`, so
  templates such as `b3-equities-register` could not be processed. The
  output schema is now read through a DuckDB relation, which works from
  DuckDB 1.2 to 2.0. CI also runs the tests on the newest DuckDB.
  (WIL-207)

### Template format changes

From this release on, every change to the template format is listed here,
with migration notes. While brasa is 0.x there is no deprecation period;
from 1.0, a change gets one release with a warning first.

- **Templates live in a template folder's `templates/` subfolder.** A folder
  with `.yaml` files at its top level loads nothing (brasa warns). Move them:
  `mkdir templates && git mv *.yaml templates/`.
- **`brasa.toml`** (optional) at the folder root: `name = "my-templates"` and
  `requires-brasa = ">=0.5,<0.6"`.
- **New optional folder parts:** `plans/` (download plans, called by name),
  `validations/` (rule files) and `steps.py` (custom steps).
- **Inline `validations:` block** in a template: rules for the dataset that
  template writes.

## v0.4.0 (2026-08-24) — Memory-safe run-all

### Features

- `brasa run` and `brasa run-all` now execute each template in its own
  child process (`python -m brasa process <template>`). A template killed
  by the kernel (e.g. out-of-memory) is reported as failed — with its
  dependents skipped — instead of crashing the whole run and the
  terminal. Every executed step is appended to
  `<data_path>/logs/run-all.log` (timestamp, template, status, exit code,
  duration, peak child RSS). (WIL-178)
- Internal in-memory DuckDB queries (ETL `run_query`/`sql_export` steps
  and staleness checks) are now memory-capped with disk spill, fixing the
  OOM kills of long `run-all` passes. New optional `config.toml` keys
  `duckdb_memory_limit` (default: 50% of physical RAM) and
  `duckdb_temp_directory` (default: `<data_path>/tmp/duckdb`), with
  `BRASA_DUCKDB_MEMORY_LIMIT` / `BRASA_DUCKDB_TEMP_DIRECTORY` env
  overrides. (WIL-178)
- `python -m brasa` now works as an alternative to the `brasa` script.

### Fixes

- `brasa process` exits with code 1 when any template fails (previously
  always exited 0).
- `brasa init` no longer discards other keys in `config.toml` when
  re-saving the data path.
- `staging.b3-cotahist` is now streamed via `sql_export` and drops
  duplicate rows in the pre-2018/post-2018 overlap.

## v0.3.0 (2026-08-02) — Explicit config & leaner core

### Breaking changes

- brasa now requires explicit configuration via `brasa init` or the
  `BRASA_DATA_PATH` environment variable; the implicit `./.brasa-cache`
  default was removed. Data-touching commands fail with an actionable
  error until one of the two is configured.
- The `brasa setup` command was renamed to `brasa init` and gained
  `--data-path PATH` and `--yes` flags; the chosen path is persisted in
  `~/.config/brasa/config.toml`.
- Python floor raised to 3.11 (was 3.10).
- Legacy template mechanisms removed: `reader.function` templates and the
  `handler:` field syntax are gone; `reader.pipeline` and `type:` are now
  the only supported forms. The `brasa/readers` package was deleted.

### Features

- New `brasa info` subcommand: shows version, whether/how brasa is
  configured (env var vs config file) and the resolved data path, with
  script-friendly exit codes. New `brasa --version` flag.
- `staging.b3-equities-register` and `staging.b3-equities-spot-market` are
  now full-span time series (all refdates since 2016), closing the
  survivorship bias of the previous listed-today-only snapshots. The
  spot-market universe excludes B3 test assets.
- `staging.b3-equities-unadjusted-prices` gained a `distribution_id`
  column sourced from the bvbg028 register.

### Maintenance

- 2026-07 audit cleanup milestone: removed dead code and legacy engine
  surface, replaced hand-rolled utilities with stdlib equivalents,
  dropped 6 unused dependencies and mypy, consolidated the test suite
  (76 → 66 files) and moved unreferenced fixtures/notebooks to the
  workspace attic.
- Removed obsolete one-off scripts whose purpose was fulfilled:
  `scripts/migrate_download_trials_status.py`, `scripts/migrate_cache_ids.py`,
  `scripts/scan_download_args.py`, `scripts/migrate_listed_etfs.py`,
  `scripts/migrate_meta_db_fk.py`, `scripts/bvbg028_capture_expected_values.py`,
  plus their coupled tests (`tests/test_migrate_meta_db_fk.py`,
  `tests/test_migrate_listed_etfs.py`, `tests/test_download_trials_migration.py`).
  All are recoverable from git history.

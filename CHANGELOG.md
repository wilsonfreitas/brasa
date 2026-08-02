# Changelog

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

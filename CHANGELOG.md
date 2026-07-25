# Changelog

## Unreleased

### Breaking changes

- brasa now requires explicit configuration via `brasa init` or the
  `BRASA_DATA_PATH` environment variable; the implicit `./.brasa-cache`
  default was removed. Data-touching commands fail with an actionable
  error until one of the two is configured.
- The `brasa setup` command was renamed to `brasa init` and gained
  `--data-path PATH` and `--yes` flags; the chosen path is persisted in
  `~/.config/brasa/config.toml`.

### Maintenance

- Removed obsolete one-off scripts whose purpose was fulfilled:
  `scripts/migrate_download_trials_status.py`, `scripts/migrate_cache_ids.py`,
  `scripts/scan_download_args.py`, `scripts/migrate_listed_etfs.py`,
  `scripts/migrate_meta_db_fk.py`, `scripts/bvbg028_capture_expected_values.py`,
  plus their coupled tests (`tests/test_migrate_meta_db_fk.py`,
  `tests/test_migrate_listed_etfs.py`, `tests/test_download_trials_migration.py`).
  All are recoverable from git history.

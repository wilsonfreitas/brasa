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

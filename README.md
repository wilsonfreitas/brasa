# brasa

A template-driven ETL engine: download, parse, transform and query datasets
described by YAML templates. The official templates —
[brasa-templates](https://github.com/wilsonfreitas/brasa-templates) — cover
Brazilian market and economic data (B3, ANBIMA, CVM, BCB).

## Installation

Install from PyPI as `brasa-core` (the import name and the command are `brasa`),
then run `brasa init` to choose a data directory and get the templates:

    pip install brasa-core
    brasa init

Until 0.4.0 the distribution was called `brasa-marketdata`; since 0.5.0 it is
`brasa-core`, and the templates live in brasa-templates instead of the package.

Or directly from GitHub (latest `main`):

    pip install git+https://github.com/wilsonfreitas/brasa.git

With `uv`:

    uv add brasa-core
    uv add git+https://github.com/wilsonfreitas/brasa.git

## Data directory (`brasa init` / `BRASA_DATA_PATH`)

brasa stores everything — raw downloads, parsed parquet, and the metadata DB —
under a single *brasa home*. On a fresh install, configure it once:

    brasa init

This suggests a platform default (e.g. `~/.local/share/brasa` on Linux),
lets you confirm or type another path, and persists the choice in
`~/.config/brasa/config.toml`. It also sets up your templates: it clones the
official [brasa-templates](https://github.com/wilsonfreitas/brasa-templates)
repo into `<data path>/templates` (use `--templates <url|path>` for a fork or
a folder you already have). Get template updates later with
`brasa update-templates`. Non-interactive variants:

    brasa init --data-path /data/brasa   # explicit path, no prompt
    brasa init --yes                     # accept the default, no prompt

The `BRASA_DATA_PATH` environment variable, when set, always takes
precedence over the config file — useful for CI, containers, tests, and
one-off overrides. Commands that read or write data fail with a clear
error until one of the two is configured; template introspection commands
(`list-templates`, `deps`, `graph`) work without any configuration.

Templates can live in your own template folders too (`BRASA_TEMPLATE_PATH`
or `template_path` in the config file). Templates are code — they can run
Python — so only add folders you trust; see
[Template folders](docs/TEMPLATES.md#template-folders).

## Changelog

### Deterministic Download Status Codes

Every download attempt is now classified with a single, unambiguous status code
persisted in the `download_trials` table:

| Symbol | Name       | Trigger                                           |
|--------|------------|---------------------------------------------------|
| `.`    | PASSED     | Successful download                               |
| `F`    | FAILED     | Expected failure (`DownloadException`)             |
| `E`    | ERROR      | Unexpected exception                              |
| `S`    | SKIPPED    | Skipped (cache hit / invalid / duplicated)         |
| `D`    | DUPLICATED | Raw folder already exists                         |
| `I`    | INVALID    | Content validation failure                        |
| `W`    | WARNING    | Success with warnings                             |

**DB migration**: Existing caches are upgraded automatically on startup.
Legacy `downloaded=1` rows become `PASSED`; `downloaded=0` become `FAILED`.
(The standalone migration script has been removed after being applied; it is
recoverable from git history if ever needed.)

See [docs/USER_GUIDE.md](docs/USER_GUIDE.md#download-status-codes) for full details.

### Import Local Files (`import_marketdata` / `brasa import`)

Files with no download URL — a one-off vendor file, a manually-provided upload,
or a corrected file for backfill — can now be imported using the same
validate → gzip → checksum-dedup → parse → store engine as `download`. Only
the acquisition step changes: bytes are read from disk instead of HTTP.

```bash
# Backfill a single date into a template that normally downloads via HTTP
brasa import b3-cotahist-daily --path /data/backfill/COTAHIST_D02012024.TXT --arg refdate=2024-01-02

# Bulk import one file per business day using a date pattern
brasa import my-daily-template --path '/data/prices/%Y-%m-%d.csv' --arg refdate=@2026-06-01:2026-06-30
```

```python
from brasa import import_marketdata

import_marketdata("b3-cotahist-daily", path="/data/backfill/COTAHIST_D02012024.TXT", refdate="2024-01-02")
```

See [docs/CLI.md](docs/CLI.md#import), [docs/API_REFERENCE.md](docs/API_REFERENCE.md#data-import),
and [docs/TEMPLATES.md](docs/TEMPLATES.md#importing-local-files-importer) for full details.

## Publishing to PyPI

brasa is built with [hatchling](https://hatch.pypa.io/) and published manually
under the distribution name `brasa-core` (the PyPI name `brasa` was already
taken by an unrelated project; `import brasa` is unaffected; until 0.4.0 the
name was `brasa-marketdata`). The SQL DDL ships inside the package
(`brasa/files/sql/`); templates do not — they live in brasa-templates.

1. Bump `version` in `pyproject.toml`.
2. Build the wheel and sdist:

   ```bash
   uv build
   ```

   Distributions are written to `dist/`.

3. (Optional) Verify the wheel ships the SQL files and no templates:

   ```bash
   BRASA_BUILD_TEST=1 uv run pytest tests/test_packaging_wheel.py -v
   ```

4. (Optional) Smoke-test in a clean environment:

   ```bash
   python -m venv /tmp/brasa-smoke
   /tmp/brasa-smoke/bin/pip install dist/brasa_core-*.whl
   /tmp/brasa-smoke/bin/brasa --version
   ```

5. Publish:

   ```bash
   # TestPyPI dry-run first (recommended)
   uv publish --publish-url https://test.pypi.org/legacy/ --token "$TEST_PYPI_TOKEN"
   # Production
   uv publish --token "$UV_PUBLISH_TOKEN"
   ```

   A PyPI API token can be supplied via `--token`, the `UV_PUBLISH_TOKEN`
   environment variable, or `~/.pypirc`.

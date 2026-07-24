# brasa

Extract finance market data from brazillian financial institutions: B3, ANBIMA, Tesouro Direto, CVM.

## Installation

Install from PyPI (published as `brasa-marketdata`; the import name is still `brasa`):

    pip install brasa-marketdata

Or directly from GitHub (latest `main`):

    pip install git+https://github.com/wilsonfreitas/brasa.git

With `uv`:

    uv add brasa-marketdata
    uv add git+https://github.com/wilsonfreitas/brasa.git

## Cache location (`BRASA_DATA_PATH`)

brasa stores everything — raw downloads, parsed parquet, and the metadata DB —
under a single *brasa home*, resolved from the `BRASA_DATA_PATH` environment
variable (falling back to `./.brasa-cache`).

A common setup keeps one central home exported globally (e.g. in `~/.env.local`).
For a one-off, project-local dataset, create a separate home and point at it for
the current shell only — a per-shell `export` shadows the global value:

    BRASA_DATA_PATH=./brasa-home uv run python -m brasa.cli setup

`setup` then prints the exact line to activate that home:

    Brasa home ready at /abs/path/to/brasa-home

    To use this home in your shell:
      export BRASA_DATA_PATH="/abs/path/to/brasa-home"

Run that `export` and every `brasa` command in the session uses the project-local
home; open a new shell to return to the central one.

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
A standalone migration script is also available:

```bash
uv run python scripts/migrate_download_trials_status.py
```

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
under the distribution name `brasa-marketdata` (the PyPI name `brasa` was
already taken by an unrelated project; `import brasa` is unaffected).
Templates and SQL DDL are bundled inside the package (`brasa/files/`), so the
built wheel is self-contained.

1. Bump `version` in `pyproject.toml`.
2. Build the wheel and sdist:

   ```bash
   uv build
   ```

   Distributions are written to `dist/`.

3. (Optional) Verify the wheel bundles the data files:

   ```bash
   BRASA_BUILD_TEST=1 uv run pytest tests/test_packaging_wheel.py -v
   ```

4. (Optional) Smoke-test in a clean environment:

   ```bash
   python -m venv /tmp/brasa-smoke
   /tmp/brasa-smoke/bin/pip install dist/brasa_marketdata-*.whl
   /tmp/brasa-smoke/bin/python -c "import brasa; from brasa.engine.template import list_templates; print(len(list_templates()))"
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

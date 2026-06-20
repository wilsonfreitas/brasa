# brasa

Extract finance market data from brazillian financial institutions: B3, ANBIMA, Tesouro Direto, CVM.

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

## Publishing to PyPI

brasa is built with [hatchling](https://hatch.pypa.io/) and published manually.
Templates and SQL DDL are bundled inside the package (`brasa/files/`), so the
built wheel is self-contained.

1. Bump `version` in `pyproject.toml` (the repo ships a `0.0.1` placeholder).
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
   /tmp/brasa-smoke/bin/pip install dist/brasa-*.whl
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

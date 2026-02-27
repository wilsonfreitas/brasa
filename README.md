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

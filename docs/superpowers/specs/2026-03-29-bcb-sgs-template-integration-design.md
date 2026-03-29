# BCB SGS Template Integration Design

**Issue:** WIL-8 — Better integration with python-bcb
**Date:** 2026-03-29
**Status:** Approved

## Goal

Integrate python-bcb's SGS module into brasa's template-driven pipeline so that BCB time series data flows through the standard download -> cache -> parse -> parquet pipeline instead of living in ETL functions. The design should make it straightforward to add OData endpoints (PTAX, Expectativas, etc.) in the future without rearchitecting.

## Scope

- **In scope:** SGS time series via `sgs.get_json()`
- **Out of scope:** OData endpoints (PTAX, Expectativas, TaxaJuros, etc.) — future work

## Constraints

- Same set of arguments cannot be downloaded twice (unless forced) — enforced by CacheManager
- Downloaded content cannot be duplicated — enforced by checksum-based deduplication
- The template does not declare update frequency — the caller controls what date range to fetch
- A future `update` feature will automate incremental fetching, but is out of scope

## Usage

```python
# Initial backfill
download_marketdata('bcb-sgs', code=[4389, 1178, 432], start='2000-01-01', end='2026-03-29')

# Incremental update
download_marketdata('bcb-sgs', code=[4389, 1178, 432], start='2026-03-28', end='2026-03-29')
```

`KwargsIterator` expands `code=[4389, 1178, 432]` into three separate API calls, one per code. Each call is cached independently.

## Components

### 1. Downloader — `BCBSGSDownloader`

Update the existing `BCBSGSDownloader` class in `brasa/downloaders/downloaders.py` to accept `code`, `start`, `end` arguments (replacing the current `code`, `refdate` pattern).

```python
class BCBSGSDownloader:
    def __init__(self, **kwargs):
        self.args = kwargs

    def download(self) -> IO | None:
        try:
            text = sgs.get_json(
                self.args["code"],
                start=self.args["start"],
                end=self.args["end"],
            )
        except Exception:
            return None
        return io.BytesIO(bytes(text, "utf8"))
```

The helper function `bcb_sgs_download` in `brasa/downloaders/helpers.py` requires no changes — it passes kwargs through to the downloader.

### 2. Template — `bcb-sgs`

New file: `templates/bcb/bcb-sgs.yaml`

```yaml
id: bcb-sgs
description: Séries temporais do SGS (Sistema Gerenciador de Séries) do BCB

downloader:
  function: brasa.downloaders.bcb_sgs_download
  validator: brasa.downloaders.validate_json_empty_file
  format: json
  args:
    code: ~
    start: ~
    end: ~

reader:
  locale: pt
  pipeline:
    - step: read_json
    - step: rename_columns
      columns:
        data: refdate
        valor: value
    - step: add_column
      name: code
      from:
        where: download_args
        key: code
    - step: apply_fields
      errors: coerce

writer:
  layer: input
  partitioning: [refdate, code]

fields:
  - name: refdate
    description: Data de referência
    type: date(format='%d/%m/%Y')
  - name: value
    description: Valor da série
    type: numeric(decimal=',')
  - name: code
    description: Código da série SGS
    type: integer
```

Key design decisions:
- **Pipeline-based reader** (modern format, not legacy `reader.function`)
- **`rename_columns`** maps `data` -> `refdate`, `valor` -> `value` from the SGS JSON response
- **`add_column`** injects `code` from download args since the JSON response doesn't include it
- **Partitioned by `[refdate, code]`** for efficient querying by date and series

### 3. Migration

| Asset | Action |
|---|---|
| `templates/bcb/bcb-sgs-data.yaml` | Move to `templates/legacy/bcb-sgs-data.yaml` |
| `bcb-data` template | Refactor to read from `input.bcb-sgs` instead of calling `sgs.get()` directly in `create_bcb_data` ETL |
| `create_bcb_data` in `brasa/etl.py` | Update to query `input.bcb-sgs` parquet data |
| `bcb-currency-data` / PTAX | No changes (out of scope) |
| `bcb-currencies-returns` | No changes (out of scope) |

### 4. Testing

- **Unit test:** Mock `sgs.get_json()` to verify the downloader returns proper `BytesIO` with expected JSON content
- **Integration test** (`@pytest.mark.integration`): Call `download_marketdata('bcb-sgs', code=[4389], start='2025-01-01', end='2025-01-31')` and verify data flows through cache -> parse -> parquet correctly
- **Existing tests:** Update any tests referencing `bcb-sgs-data` to point to legacy path or the new template

## Future Extensions

When OData endpoints are added (PTAX, Expectativas, etc.), each will get:
- Its own downloader class (e.g., `BCBPTAXDownloader`, `BCBExpectativasDownloader`)
- Its own template with endpoint-specific arguments
- No new base class or abstraction needed — follows the existing convention where each download strategy has its own class

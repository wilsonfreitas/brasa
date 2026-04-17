# Plan: Create ETL Template `staging.bcb-sgs`

## Context

A new `input.bcb-sgs` template (`templates/bcb/bcb-sgs.yaml`) was recently created using the modern pipeline format. It downloads SGS time series data and stores it partitioned by `[downloaded_at, code]`. Because each download run creates a new partition, the input layer will accumulate duplicate rows for the same `(refdate, code)` pair across multiple downloads.

An older legacy ETL exists (`create_bcb_data` in `brasa/etl.py`, used by `templates/bcb/bcb-data.yaml`) that does the same transformation but using the old function-based style from the now-deprecated `bcb-sgs-data` input template. The new ETL template should replicate this logic in the modern pipeline YAML format, reading from `input.bcb-sgs`.

## Goal

Create `templates/bcb/bcb-sgs-etl.yaml` that produces `staging.bcb-sgs` with columns:
- `refdate` — date
- `symbol` — string (mapped from `code`)
- `value` — numeric

## Implementation Plan

### New file: `templates/bcb/bcb-sgs-consolidated.yaml`

```yaml
id: bcb-sgs-consolidated
description: Séries temporais do SGS (BCB) deduplicated e com símbolo mapeado

etl:
  pipeline:
    - step: sql_query
      datasets:
        - input.bcb-sgs
      query: |
        SELECT
          refdate,
          CASE code
            WHEN 4389 THEN 'CDI'
            WHEN 1178 THEN 'SELIC'
            WHEN 432  THEN 'SETA'
            WHEN 433  THEN 'IPCA'
            WHEN 189  THEN 'IGPM'
          END AS symbol,
          value
        FROM (
          SELECT *,
            ROW_NUMBER() OVER (
              PARTITION BY refdate, code
              ORDER BY downloaded_at DESC
            ) AS rn
          FROM 'input.bcb-sgs'
          WHERE code IN (4389, 1178, 432, 433, 189)
        )
        WHERE rn = 1

writer:
  layer: staging
  dataset: bcb-sgs
  partitioning: [refdate]

fields:
  - name: refdate
    description: Data de referência
    type: date(format='%Y-%m-%d')
  - name: symbol
    description: Símbolo da série (ex. IPCA, CDI)
    type: character
  - name: value
    description: Valor da série
    type: numeric
```

### Key design decisions

1. **`sql_query` with `ROW_NUMBER() OVER (PARTITION BY refdate, code ORDER BY downloaded_at DESC)`** — the dominant pattern for "keep latest" deduplication in this codebase (used in `brasa-companies.yaml`, `b3-equities-instrument-assets.yaml`, etc.)

2. **`CASE code WHEN ... END`** inside the same SQL query — avoids an extra pipeline step and keeps logic self-contained.

3. **`WHERE code IN (...)`** filter is applied inside the subquery to exclude unknown codes before deduplication, matching the behavior of the legacy `create_bcb_data` function.

4. **`writer.dataset: bcb-sgs`** — the template id is `bcb-sgs-consolidated` (to avoid collision with the input template), but the output dataset name is explicitly set to `bcb-sgs` so the view is `staging.bcb-sgs`.

5. **`partitioning: [refdate]`** — sensible for time-series queries.

## Critical files

- New file: `templates/bcb/bcb-sgs-consolidated.yaml`
- Reference: `templates/bcb/bcb-sgs.yaml` (the input template)
- Reference: `brasa/etl.py:129-152` (`create_bcb_data` — legacy equivalent)
- Reference: `templates/brasa/brasa-companies.yaml` (ROW_NUMBER dedup pattern)

## Verification

1. Run `uv run pytest tests/test_templates.py` to confirm template loads without errors.
2. After downloading some SGS data, run the ETL and query:
   ```python
   from brasa import process_etl
   process_etl("bcb-sgs-consolidated")
   ```
3. Inspect via BrasaDB:
   ```sql
   SELECT symbol, COUNT(*), MIN(refdate), MAX(refdate)
   FROM "staging.bcb-sgs"
   GROUP BY symbol
   ORDER BY symbol
   ```
4. Run full checks: `uv run pytest && uv run ruff check . && uv run pre-commit run --all-files`

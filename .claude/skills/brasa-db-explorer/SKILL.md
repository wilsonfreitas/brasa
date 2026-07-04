---
name: brasa-db-explorer
description: Connect to BrasaDB's DuckDB database and execute SQL queries to explore Brazilian financial market data. Use this skill whenever the user asks to query, explore, analyze, or study data in the brasa database, asks about available datasets or tables, wants to run SQL queries against financial data, wants to create views or combine datasets for a specific application, or mentions DuckDB, BrasaDB, or SQL in the context of this project. Also trigger when the user asks questions that could be answered by querying the data — e.g., "what stocks are in IBOV?", "show me PETR4 prices", "what's the DI1 curve?", "help me create a view with these datasets", "I have datasets A and B, combine them for X".
---

# BrasaDB Explorer

Execute SQL queries against the brasa DuckDB database to explore, analyze, and combine Brazilian financial market data.

## Setup: Creating Views

Views must exist before querying. If queries return empty results or "table not found" errors, create the views first:

```bash
uv run python -c "from brasa import create_all_views; create_all_views()"
```

This only needs to be done once per Python process. If you open a new terminal or restart the kernel, run it again.

If a query returns empty results but no error is raised, first verify your filter values by sampling the table without filters (for example, `SELECT DISTINCT symbol FROM "staging.b3-cotahist" LIMIT 20`) before assuming views are missing.

## Connecting to DuckDB

Don't open the database file by hand. The `BrasaDB` class is the easy path: it owns a single shared connection to `brasa.duckdb` and exposes helper methods for running queries. Because every caller reuses that one connection — already opened with `read_only=False` and re-established automatically if it drops — you never construct the database path yourself, and you sidestep the error DuckDB raises when read-only and read-write connections are mixed in the same session (brasa holds a write connection internally).

The most convenient entry point is the module-level `sql()` helper, which delegates to `BrasaDB.query()`:

```bash
uv run python -c "
from brasa import sql
print(sql('''
<SQL QUERY HERE>
''').df().to_string())
"
```

`sql()` returns a DuckDB relation. Call `.df()` for a pandas DataFrame — chain `.to_string()` so wide or long results print in full — or `.fetchall()` for a list of tuples.

The same functionality is available as methods on the class, which is handy when you want discovery and querying in one place:

- `BrasaDB.query(sql)` — run a query (this is what `sql()` calls)
- `BrasaDB.list_tables()` — list every `layer.dataset` view
- `BrasaDB.create_all_views()` — (re)create all views; see Setup above
- `BrasaDB.get_connection()` — the shared `duckdb.DuckDBPyConnection`, if you need raw access

`BrasaDB` resolves the database path through brasa's `CacheManager`, so it honors the `BRASA_DATA_PATH` environment variable (defaulting to `.brasa-cache/`) without any manual path handling.

## Table Naming Convention

Tables follow the pattern `"layer.dataset-name"` and **must be double-quoted** in SQL because they contain dots and hyphens:

```sql
SELECT * FROM "staging.b3-cotahist" LIMIT 10
```

Datasets are organized in layers. **Default to the `staging` layer** — it holds the cleaned, enriched, analysis-ready tables intended for end users, and it's where you should look first:

- **staging** — transformed and enriched data ready for analysis (prices, returns, index compositions, macro series, corporate events). **Start here.**
- **curated** — a small set of fully unified, analytics-ready datasets built on top of staging (e.g., all returns, all prices).
- **input** — raw parsed data. This is an internal ingestion layer, **not meant for end users**. Only reach for an `input.*` table when a dataset genuinely has no `staging` equivalent (e.g. some B3 registries, PTAX FX, intraday trades); otherwise the staging version is cleaner and is what you should use.

## Choosing the right table

Selection questions arrive as *information needs*, not table names. Match the need to the canonical table below, and heed the caveat — it's usually what makes the tempting alternative wrong. Prefer these over anything you'd find by keyword-matching a table name. For anything not listed, fall back to the Key Datasets Reference and the discovery queries. (This map mirrors the authoritative Status notes in [`docs/datasets.md`](../../../docs/datasets.md).)

### Equity / ETF prices & trading activity

- **Daily stock OHLC (unadjusted)** → `staging.b3-cotahist`. Longest history. Stock prices are **not adjusted** for corporate events.
- **Adjusted stock OHLC** → ⚠️ **does not exist yet.** Don't invent a table. Adjusted *returns* exist (see below); an adjusted price series still has to be built.
- **ETF OHLC** → `staging.b3-cotahist`. ETF close is fine unadjusted (most BR ETFs pay no dividends) **except `NDIV11`, `DIVD11`, `SPYI11`**.
- **Equity-option OHLC** → `staging.b3-cotahist`. Longest history.
- **Volume / nº de negócios / nº de trades (deepest history)** → `staging.b3-cotahist`.
- **"What stocks/assets exist" (registry)** → `input.b3-bvbg028-equities`. Daily registry of everything listed (stocks, fractional…); also `market_capitalisation`.
- **Which stocks trade on the spot market (non-fractional)** → `staging.b3-equities-spot-market`.

### Returns

- **Adjusted stock returns** → `staging.b3-equities-returns`. Built from `b3-bvbg086`'s daily `oscillation`; history **starts 2018**. Pre-2018 you only have unadjusted returns computed from `staging.b3-cotahist`.
- **ETF returns** → `staging.b3-equities-etfs-returns`. Longer history — concatenates `b3-cotahist` (<2018) + `b3-bvbg086` (>2018).

### Indexes

- **What's in IBOV (membership)** → `staging.b3-indexes-composition` (no weights).
- **Index weights — official quarterly target** → `staging.b3-indexes-theoretical-portfolio` (rebalance-date weights, valid for the quarter).
- **Index weights — live/drifted as of a day** → `staging.b3-indexes-current-portfolio`.
- **Index level over time** → `staging.b3-indexes-historical-prices`.

### Futures

- **Futures daily data (>2018)** — prices/OHLC, settlement, open interest, contract metadata → `staging.b3-futures`. It is `b3-bvbg086` + `b3-bvbg028-future_contracts` already joined and cleaned. **Settlement lives in `adjusted_quote`/`adjusted_tax`, not `settlement_value`.**
- **Futures settlement pre-2018** → `input.b3-futures-settlement-prices` (frozen; incomplete — no maturity/OI; OK only for simple standardized contracts like DI1, DOL, DAP).
- **Contract registry (maturity, multiplier)** → `staging.b3-futures-register`.
- Avoid `staging.b3-futures-di1-consolidated` (**deprecated**) and `staging.b3-futures-settlement-prices` (**outdated, ships 2× duplicate rows**).

### Interest-rate & inflation curves

- **DI nominal curve at contract maturities (+ CDI overnight)** → `staging.b3-curves-di1`.
- **DI nominal curve at standardized fixed-term vertices** (e.g. `DI1T252` = 252 business days) → `staging.b3-curves-di1-standard`.
- **Returns of a fixed-tenor DI vertex** → `staging.b3-curves-di1-standard-returns`.
- **Real-rate (inflation) curve** → `staging.b3-futures-dap` for now (standardized `b3-curves-dap*` are planned but **not yet in the DB**). The rate column is `adjusted_tax`.

### Macro, indicators & FX

- **Macro series (CDI, SELIC, IPCA, IGPM)** → `staging.bcb-sgs`. Other SGS series → `input.bcb-sgs` (by numeric `code`).
- **Indicators to use with B3 contracts/pricing/curves** → `staging.b3-economic-indicators` (this is the pricing companion, *not* general macro).
- **FX / PTAX** → `input.bcb-currency` (input-only; `currency` ∈ {USD, EUR, GBP, JPY, CHF, CAD, AUD}).

### Corporate events

- **All events for a stock (one place)** → `staging.brasa-corporate-events` (unified; `event_family` ∈ {CASH, STOCK, SUBSCRIPTION}).
- **Cash dividends & JCP (with yield)** → `staging.b3-cash-dividends-events`; **split/bonus factors** → `staging.b3-stock-events`; **subscriptions** → `staging.b3-subscription-events`.

### Company info, identity & sectors

- **Company-level info** → `staging.brasa-companies`. **No `symbol` column — join via `code_cvm`.**
- **Ticker ↔ company** → `staging.b3-companies-symbols`.
- **Sector / industry classification** → `staging.brasa-industry-sectors` (best source; prefer over the sector columns on `brasa-companies`).

### Funds, options, intraday

- **Listed funds (ETF/FII)** → `staging.b3-listed-funds` (`fund_type` ∈ {ETF, FII, Fixed Income ETF}).
- **Option theoretical price / implied vol** → `input.b3-equity-options`; **vol surface** → `input.b3-equities-volatility-surface` (both input-only).
- **Intraday tick trades** → `input.b3-trades-intraday[-equities/-derivatives]` for deepest history; `staging.b3-trades-intraday` if its 2023+ coverage suffices.

## Discovery Workflow

Use the Key Datasets Reference for common tables listed below. Run discovery queries when the user asks about datasets not listed there, or when a query returns unexpected results.

### 1. List all tables

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_type = 'VIEW'
ORDER BY table_name
```

### 2. Inspect a table's schema

```sql
DESCRIBE "staging.b3-cotahist"
```

### 3. Sample data

```sql
SELECT * FROM "staging.b3-cotahist" LIMIT 5
```

### 4. Check row counts and date ranges

```sql
SELECT
    COUNT(*) as rows,
    MIN(refdate) as first_date,
    MAX(refdate) as last_date
FROM "staging.b3-cotahist"
```

## Key Datasets Reference

These are the most commonly used datasets, with their actual column names verified against the live database. This is a curated subset — for the **complete catalog of every view grouped by topic**, see [`docs/datasets.md`](../../../docs/datasets.md) in the repo, or run the discovery queries above to see every available table and its current schema.

**Prefer `staging.*` tables** — they are the cleaned, user-facing datasets. The `input.*` tables listed below appear only where they expose data that has no `staging` equivalent (e.g. PTAX FX, intraday trades, options/volatility, some B3 registries); when a staging table covers the same data, use it instead. Column names are case-sensitive: the raw `input.b3-company-*` tables use camelCase (e.g. `issuingCompany`, `codeCVM`) while staging tables use snake_case.

### Prices & Trading

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-cotahist` | Daily OHLC + volume for stocks / ETFs / options — deepest history; **unadjusted** | refdate, symbol, open, high, low, average, close, volume, trade_quantity, traded_contracts |
| `staging.b3-equities-spot-market` | Stocks trading on the spot market (non-fractional) | refdate, symbol, isin, corporation_name, close, open |
| `input.b3-bvbg028-equities` | Asset registry — "what stocks exist" (BVBG028) | refdate, symbol, isin, corporation_name, market_capitalisation |
| `input.b3-bvbg028-options_on_equities` | Options-on-equities registry (strikes/maturities) | refdate, symbol, exercise_price, option_type, maturity_date |
| `input.b3-bvbg086` | Market data for all B3 assets — has `oscillation`; **no `settlement_value`** | refdate, symbol, open, high, low, close, oscillation, open_interest, volume, adjusted_quote, adjusted_tax |

### Indexes

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-indexes-composition` | Index compositions | refdate, indexes, symbol, corporation_name, specification_code |
| `staging.b3-indexes-historical-prices` | Historical index prices (long) | refdate, symbol, value |
| `staging.b3-indexes-theoretical-portfolio` | Theoretical portfolio weights | refdate, symbol, weight, index |
| `staging.b3-indexes-current-portfolio` | Current portfolio weights | refdate, symbol, weight, index |

### Returns (staging layer)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-equities-returns` | Equity returns | refdate, symbol, pct_return, log_return |
| `staging.b3-equities-etfs-returns` | ETF returns | refdate, symbol, pct_return, log_return |

### Macro & Rates (BCB / ANBIMA)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.bcb-sgs` | Daily macro series — `symbol` ∈ {CDI, SELIC, IPCA, IGPM, SETA} | refdate, symbol, value |
| `input.bcb-sgs` | Raw SGS series keyed by numeric `code` | refdate, code, value |
| `input.bcb-currency` | PTAX FX rates — `currency` ∈ {USD, EUR, GBP, JPY, CHF, CAD, AUD} | refdate, currency, bid, ask, parity_bid, parity_ask |
| `staging.b3-economic-indicators` | B3 economic indicators (grouped) | refdate, indicator_group, symbol, value |
| `staging.b3-curves-di1` | DI nominal curve at contract maturities (+ CDI overnight) | refdate, symbol, maturity_date, business_days, adjusted_tax |
| `staging.b3-curves-di1-standard` | DI nominal curve at standardized fixed-term vertices (e.g. `DI1T252`) | refdate, symbol, maturity_date, business_days, adjusted_tax |
| `staging.b3-futures-dap` | Real-rate (inflation/DAP) curve — current source (`b3-curves-dap` not yet materialized) | refdate, symbol, maturity_date, adjusted_tax, business_days |
| `input.anbima-index-imab` | ANBIMA IMA fixed-income index | refdate, index_name, index_number, duration_du, pmr |

### Corporate Events

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.brasa-corporate-events` | Unified events — `event_family` ∈ {CASH, STOCK, SUBSCRIPTION} | code_cvm, symbol, event_family, event_type, ex_date, payment_date, value_cash, factor, ratio |
| `staging.b3-cash-dividends-events` | Cash dividends/JCP events | code_cvm, symbol, ex_date, payment_date, value_cash, yield_pct |
| `staging.b3-stock-events` | Stock events (splits, bonus, mergers) | code_cvm, symbol, event_type_raw, factor, ex_date |
| `staging.b3-subscription-events` | Subscription rights | code_cvm, symbol, subscription_price, subscription_date, ex_date |
| `input.b3-cash-dividends` | Raw cash dividends | refdate, trading_name, type_stock, value_cash, last_date_prior_ex |

### Company & Fund Data

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.brasa-companies` | Unified company information (no `symbol` column — join via `code_cvm`) | code_cvm, company_name, trading_name, sector, subsector, segment |
| `staging.b3-companies-profile` | Company profile | code_cvm, trading_name, company_name, cnpj, segment |
| `staging.b3-companies-symbols` | Symbol ↔ company mapping | symbol, isin, code_cvm, share_class |
| `staging.b3-companies-names` | Company name registry | refdate, code_cvm, trading_name, instrument_asset |
| `staging.brasa-industry-sectors` | Industry classification | sector, subsector, gics_sector, icb_sector |
| `input.cvm-companies-registration` | CVM company registry | code_cvm, cnpj_cia, denom_social, setor_ativ, sit |
| `input.b3-company-details` | Detailed company info (camelCase) | refdate, issuingCompany, tradingName, codeCVM, industryClassification |
| `input.b3-company-info-info` | Company general info (camelCase) | refdate, issuingCompany, codeCVM, segment, totalNumberShares |
| `input.b3-company-info-cash_dividends` | Cash dividends (camelCase) | refdate, issuingCompany, paymentDate, rate, lastDatePrior |
| `staging.b3-listed-funds` | Listed funds (ETF, FII) | refdate, symbol, fund_name, fund_type |

> Note: the registry views `input.b3-listed-stock-etfs`, `input.b3-listed-fixed-income-etfs`, `input.b3-listed-cripto-etfs`, and `input.b3-listed-reits` exist but may currently hold no parquet files (queries raise an `IO Error: No files found`). Use `staging.b3-listed-funds` for fund data instead.

### Equities (staging layer)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-equities-instrument-assets` | Instrument asset mapping | refdate, instrument_asset |
| `staging.b3-equities-register` | Equities register | refdate, symbol, isin, corporation_name |
| `staging.b3-equities-spot-market` | Spot market data | refdate, symbol, isin, corporation_name |

### Futures & Options

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-futures` | **Futures daily data (>2018)** — OHLC, settlement (`adjusted_quote`/`adjusted_tax`), OI, contract meta | refdate, symbol, commodity, maturity_date, contract_multiplier, close, adjusted_quote, adjusted_tax, open_interest, volume |
| `staging.b3-futures-register` | Futures contract registry (maturity, multiplier) | refdate, symbol, maturity_date, contract_multiplier |
| `input.b3-futures-settlement-prices` | Settlement **pre-2018** tail (frozen, incomplete — simple standardized contracts only) | refdate, symbol, commodity, price, settlement_value |
| `input.b3-equity-options` | Equity option theoretical prices & implied vol | refdate, symbol, strike, maturity_date, volatility |
| `input.b3-equities-volatility-surface` | Equity volatility surface | refdate, underlying, delta, volatility, maturity_date |

### Intraday Trades (input only — no staging equivalent)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `input.b3-trades-intraday` | Intraday trades (tick) | refdate, symbol, traded_price, traded_quantity, trade_time |
| `input.b3-trades-intraday-equities` | Intraday equity trades | refdate, symbol, traded_price, traded_quantity, trade_time |
| `input.b3-trades-intraday-derivatives` | Intraday derivative trades | refdate, symbol, traded_price, traded_quantity, trade_time |

## Common Query Patterns

### Filter by symbol and date range

```sql
SELECT refdate, symbol, close, volume
FROM "staging.b3-cotahist"
WHERE symbol = 'PETR4'
  AND refdate >= '2024-01-01'
ORDER BY refdate
```

### Aggregate across symbols

```sql
SELECT symbol, COUNT(*) as trading_days, AVG(close) as avg_price
FROM "staging.b3-cotahist"
WHERE refdate >= '2024-01-01'
GROUP BY symbol
ORDER BY avg_price DESC
LIMIT 20
```

### Join datasets

```sql
SELECT
    p.refdate,
    p.symbol,
    p.close,
    p.volume,
    r.log_return
FROM "staging.b3-cotahist" p
JOIN "staging.b3-equities-returns" r
    ON p.refdate = r.refdate AND p.symbol = r.symbol
WHERE p.symbol = 'VALE3'
  AND p.refdate >= '2024-01-01'
ORDER BY p.refdate
```

### Find stocks in an index

```sql
SELECT symbol, corporation_name, specification_code
FROM "staging.b3-indexes-composition"
WHERE indexes = 'IBOV'
  AND refdate = (SELECT MAX(refdate) FROM "staging.b3-indexes-composition")
ORDER BY symbol
```

### Get a macro series (CDI, SELIC, IPCA, ...)

```sql
SELECT refdate, value
FROM "staging.bcb-sgs"
WHERE symbol = 'CDI'
  AND refdate >= '2024-01-01'
ORDER BY refdate
```

### Get FX rates (PTAX)

```sql
SELECT refdate, bid, ask
FROM "input.bcb-currency"
WHERE currency = 'USD'
  AND refdate >= '2024-01-01'
ORDER BY refdate
```

### Find dividends/corporate events for a stock

`staging.brasa-corporate-events` has no price/refdate of trading — use `ex_date` for timing. Filter by `event_family` (`CASH`, `STOCK`, `SUBSCRIPTION`) or a specific `event_type` (e.g. `CASH_DIVIDEND`, `INTEREST_ON_CAPITAL`, `SPLIT`, `BONUS_SHARES`).

```sql
SELECT ex_date, payment_date, event_type, value_cash, yield_pct
FROM "staging.brasa-corporate-events"
WHERE symbol = 'PETR4'
  AND event_family = 'CASH'
ORDER BY ex_date DESC
```

## Creating Derived Views

When the user wants to combine datasets for a specific application, create a persistent view:

```sql
CREATE OR REPLACE VIEW "custom.my-analysis" AS
SELECT
    p.refdate,
    p.symbol,
    p.close,
    p.volume,
    r.log_return,
    ic.indexes
FROM "staging.b3-cotahist" p
LEFT JOIN "staging.b3-equities-returns" r
    ON p.refdate = r.refdate AND p.symbol = r.symbol
LEFT JOIN "staging.b3-indexes-composition" ic
    ON p.symbol = ic.symbol AND p.refdate = ic.refdate
WHERE p.refdate >= '2024-01-01'
```

View names like `"custom.my-analysis"` use a dot as part of the quoted name, not as a schema separator. DuckDB stores this as a quoted object name in the default schema, so no schema creation is needed.

## DuckDB SQL Features

DuckDB supports advanced SQL that is useful for financial analysis:

- **Window functions**: `LAG`, `LEAD`, `ROW_NUMBER`, `RANK`, running averages
- **CTEs**: `WITH` clauses for readable multi-step queries
- **PIVOT/UNPIVOT**: reshape data between wide and long formats
- **Date functions**: `date_trunc`, `date_part`, `date_diff`, interval arithmetic
- **Statistical aggregates**: `stddev`, `variance`, `corr`, `percentile_cont`
- **List/array functions**: `list_agg`, `array_agg` for grouping values
- **QUALIFY clause**: filter window function results directly

### Example: Rolling volatility

```sql
SELECT
    refdate,
    symbol,
    log_return,
    STDDEV(log_return) OVER (
        PARTITION BY symbol
        ORDER BY refdate
        ROWS BETWEEN 20 PRECEDING AND CURRENT ROW
    ) as vol_21d
FROM "staging.b3-equities-returns"
WHERE symbol = 'PETR4'
  AND refdate >= '2024-01-01'
ORDER BY refdate
```

## Guidelines

- Start with discovery (list tables, describe schemas, sample rows) before writing complex queries
- Always quote table names with double quotes due to dots and hyphens
- Use `LIMIT 100` or fewer when exploring unfamiliar tables. Only omit `LIMIT` when the user explicitly requests full output or when row count is known to be small
- Prefer CTEs over subqueries for readability
- When the user's question is ambiguous, explore the data first and show what's available before making assumptions
- For date filtering, `refdate` is the standard date column across most datasets
- When creating views for the user, explain what each join and filter does
- Do not execute `INSERT`, `UPDATE`, or `DELETE` statements against existing brasa-managed tables. Only `CREATE OR REPLACE VIEW` and `SELECT` operations are safe; if the user requests data modification, explain that brasa manages data ingestion separately

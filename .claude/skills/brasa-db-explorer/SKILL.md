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

If a query returns empty results but no error is raised, first verify your filter values by sampling the table without filters (for example, `SELECT DISTINCT symbol FROM "input.b3-cotahist-daily" LIMIT 20`) before assuming views are missing.

## Connecting to DuckDB

The database file lives at `$BRASA_DATA_PATH/db/brasa.duckdb`. Always connect with `read_only=False` — DuckDB raises an error if you try to mix read-only and read-write connections in the same session, and brasa itself holds a write connection internally.

If `os.environ["BRASA_DATA_PATH"]` raises `KeyError`, inform the user that the environment variable is not set and ask them to set it to the directory where brasa data is stored before proceeding.

```bash
uv run python -c "
import duckdb, os
con = duckdb.connect(os.path.join(os.environ['BRASA_DATA_PATH'], 'db', 'brasa.duckdb'), read_only=False)
result = con.sql('''
<SQL QUERY HERE>
''')
print(result)
"
```

For queries that return many rows or wide tables, use `.df().to_string()` for full output:

```bash
uv run python -c "
import duckdb, os
con = duckdb.connect(os.path.join(os.environ['BRASA_DATA_PATH'], 'db', 'brasa.duckdb'), read_only=False)
print(con.sql('''<SQL>''').df().to_string())
"
```

## Table Naming Convention

Tables follow the pattern `"layer.dataset-name"` and **must be double-quoted** in SQL because they contain dots and hyphens:

```sql
SELECT * FROM "input.b3-cotahist-daily" LIMIT 10
```

The three queryable layers are:
- **input** — parsed raw data (e.g., daily stock prices, company info)
- **staging** — transformed/enriched data (e.g., returns, index compositions)
- **curated** — analytics-ready unified datasets (e.g., all returns, all prices)

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
DESCRIBE "input.b3-cotahist-daily"
```

### 3. Sample data

```sql
SELECT * FROM "input.b3-cotahist-daily" LIMIT 5
```

### 4. Check row counts and date ranges

```sql
SELECT
    COUNT(*) as rows,
    MIN(refdate) as first_date,
    MAX(refdate) as last_date
FROM "input.b3-cotahist-daily"
```

## Key Datasets Reference

These are the most commonly used datasets, with their actual column names verified against the live database. This is a curated subset — run the discovery queries above to see every available table and its current schema. Column names are case-sensitive: some `input.b3-company-*` tables use camelCase (e.g. `issuingCompany`, `codeCVM`) while most processed tables use snake_case.

### Prices & Trading (input layer)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `input.b3-cotahist-daily` | Daily stock prices | refdate, symbol, open, high, low, average, close, volume |
| `input.b3-cotahist-yearly` | Yearly historical stock prices | refdate, symbol, open, high, low, average, close, volume |
| `input.b3-futures-settlement-prices` | Futures settlement prices | refdate, symbol, commodity, maturity_code, price, settlement_value, price_change |
| `input.b3-bvbg028-equities` | Equities instrument registry (BVBG028) | refdate, symbol, isin, corporation_name, close, open, market_capitalisation |
| `input.b3-bvbg028-future_contracts` | Future contracts registry (BVBG028) | refdate, symbol, maturity_date, contract_multiplier |
| `input.b3-bvbg028-options_on_equities` | Options on equities registry (BVBG028) | refdate, symbol, exercise_price, option_type, maturity_date |
| `input.b3-bvbg086` | Derivatives market data (settlement, OI) | refdate, symbol, settlement_value, open_interest, volume, close |

### Indexes (input & staging layers)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `input.b3-indexes-composition` | Index compositions (raw) | refdate, indexes, symbol, corporation_name |
| `input.b3-indexes-theoretical-portfolio` | Theoretical portfolio weights | refdate, symbol, weight, index |
| `input.b3-indexes-current-portfolio` | Current portfolio | refdate, symbol, weight, index |
| `staging.b3-indexes-composition` | Index compositions (processed) | refdate, indexes, symbol, corporation_name, specification_code |
| `staging.b3-indexes-historical-prices` | Historical index prices (processed, long) | refdate, symbol, value |
| `staging.b3-indexes-theoretical-portfolio` | Theoretical portfolio (processed) | refdate, symbol, weight, index |
| `staging.b3-indexes-current-portfolio` | Current portfolio (processed) | refdate, symbol, weight, index |

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
| `staging.b3-futures-di1-consolidated` | DI1 (interest-rate) futures, consolidated | refdate, symbol, maturity_code, price, settlement_value |
| `staging.b3-futures-dap` | DAP (inflation) futures with implied tax | refdate, symbol, maturity_date, price, adjusted_tax, business_days |
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
| `staging.b3-cotahist` | Unified cotahist data | refdate, symbol, open, high, low, close, volume |
| `staging.b3-equities-instrument-assets` | Instrument asset mapping | refdate, instrument_asset |
| `staging.b3-equities-register` | Equities register | refdate, symbol, isin, corporation_name |
| `staging.b3-equities-spot-market` | Spot market data | refdate, symbol, isin, corporation_name |

### Futures & Options (staging / input)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-futures-settlement-prices` | Futures settlement prices (processed) | refdate, symbol, commodity, price, settlement_value |
| `staging.b3-futures-register` | Futures register | refdate, symbol, maturity_date, contract_multiplier |
| `input.b3-equity-options` | Equity option theoretical prices & vol | refdate, symbol, strike, maturity_date, volatility |
| `input.b3-equities-volatility-surface` | Equity volatility surface | refdate, underlying, delta, volatility, maturity_date |

### Intraday Trades (input / staging layer)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `input.b3-trades-intraday` | Intraday trades (tick) | refdate, symbol, traded_price, traded_quantity, trade_time |
| `input.b3-trades-intraday-equities` | Intraday equity trades | refdate, symbol, traded_price, traded_quantity, trade_time |
| `input.b3-trades-intraday-derivatives` | Intraday derivative trades | refdate, symbol, traded_price, traded_quantity, trade_time |

## Common Query Patterns

### Filter by symbol and date range

```sql
SELECT refdate, symbol, close, volume
FROM "input.b3-cotahist-daily"
WHERE symbol = 'PETR4'
  AND refdate >= '2024-01-01'
ORDER BY refdate
```

### Aggregate across symbols

```sql
SELECT symbol, COUNT(*) as trading_days, AVG(close) as avg_price
FROM "input.b3-cotahist-daily"
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
FROM "input.b3-cotahist-daily" p
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
FROM "input.b3-cotahist-daily" p
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

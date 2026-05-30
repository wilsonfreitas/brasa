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

These are the most commonly used datasets. Run the discovery queries above to see all available tables and their current schemas.

### Prices & Trading (input layer)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `input.b3-cotahist-daily` | Daily stock prices | symbol, refdate, open, high, low, close, volume |
| `input.b3-cotahist-yearly` | Yearly historical stock prices | symbol, refdate, open, high, low, close, volume |
| `input.b3-futures-settlement-prices` | Futures settlement prices | refdate, symbol, settlement_price |
| `input.b3-bvbg028-equities` | Equities from BVBG028 | refdate, symbol |
| `input.b3-bvbg028-future_contracts` | Future contracts from BVBG028 | refdate, symbol |
| `input.b3-bvbg028-options_on_equities` | Options on equities from BVBG028 | refdate, symbol |
| `input.b3-bvbg086` | Options market data | refdate, symbol |

### Indexes (input & staging layers)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `input.b3-indexes-composition` | Index compositions (raw) | refdate, indexes, symbol |
| `input.b3-indexes-theoretical-portfolio` | Theoretical portfolio weights | refdate, symbol |
| `input.b3-indexes-historical-prices` | Historical index prices | refdate, symbol |
| `input.b3-indexes-current-portfolio` | Current portfolio | symbol |
| `staging.b3-indexes-composition` | Index compositions (processed) | refdate, indexes, symbol |
| `staging.b3-indexes-historical-prices` | Historical index prices (processed) | refdate, symbol |
| `staging.b3-indexes-theoretical-portfolio` | Theoretical portfolio (processed) | refdate, symbol |
| `staging.b3-indexes-current-portfolio` | Current portfolio (processed) | symbol |

### Returns (staging layer)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-equities-returns` | Equity log returns | refdate, symbol, log_return |

### Company & Fund Data

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-companies-names` | Company name registry | code_cvm, trading_name |
| `staging.brasa-companies` | Unified company information | symbol, company_name |
| `staging.brasa-industry-sectors` | Industry classification | sector, subsector, segment |
| `input.b3-company-details` | Detailed company info | issuing_company |
| `input.b3-company-info-info` | Company general info | code_cvm |
| `input.b3-company-info-cash_dividends` | Cash dividends | code_cvm |
| `staging.b3-listed-funds` | Listed funds (ETF, FII) | symbol, fund_type |
| `input.b3-listed-stock-etfs` | Stock ETFs registry | symbol |
| `input.b3-listed-fixed-income-etfs` | Fixed income ETFs | symbol |
| `input.b3-listed-reits` | REITs (FIIs) | symbol |

### Equities (staging layer)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-cotahist` | Unified cotahist data | refdate, symbol, close |
| `staging.b3-equities-instrument-assets` | Instrument asset mapping | instrument_asset |
| `staging.b3-equities-register` | Equities register | symbol |
| `staging.b3-equities-spot-market` | Spot market data | refdate, symbol |

### Futures (staging layer)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `staging.b3-futures-settlement-prices` | Futures settlement prices (processed) | refdate, symbol |
| `staging.b3-futures-register` | Futures register | symbol |
| `staging.b3-futures-dap` | DAP futures | refdate, symbol |

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

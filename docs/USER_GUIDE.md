# Brasa User Guide

## Getting Started

### Installation

Using uv (recommended):
```bash
uv sync
```

Using pip:
```bash
pip install -e .
```

### Initial Setup

```python
from brasa.engine import CacheManager

# Initialize cache directories
man = CacheManager()
```

Or via CLI:
```bash
python -m brasa.cli setup
```

### Configuration

Set custom cache location (optional):
```bash
export BRASA_DATA_PATH=/path/to/data/directory
```

## Basic Workflow

### 1. Download Data

Download market data using templates:

```python
from datetime import datetime
from brasa import download_marketdata
from brasa.util import DateRange

# Define date range
period = DateRange(start=datetime(2024, 1, 1), end=datetime.today(), calendar="B3")

# Download daily price data
download_marketdata("b3-cotahist-daily", refdate=period)

# Download derivatives market data (futures contracts)
download_marketdata("b3-bvbg028", refdate=period)

# Download index composition (no date parameter needed)
download_marketdata("b3-indexes-composition")
```

### 2. Process Data

Convert downloaded files to parquet format:

```python
from brasa import process_marketdata

# Process COTAHIST data
process_marketdata("b3-cotahist-daily")

# Process derivatives market data
process_marketdata("b3-bvbg028")
```

### 3. Run ETL Transformations

Create derived datasets:

```python
from brasa import process_etl

# Consolidate DI1 futures
process_etl("b3-futures-di1-consolidated")

# Create equity returns dataset
process_etl("b3-equities-returns")

# Merge yearly and daily COTAHIST
process_etl("b3-cotahist")
```

### 4. Query Data

Access processed data:

```python
from brasa.queries import get_returns, get_prices, get_symbols
from datetime import datetime

# Get equity returns
returns = get_returns(
    ["PETR4", "VALE3", "ITUB4"],
    start=datetime(2023, 1, 1),
    end=datetime(2024, 12, 31)
)

# Get adjusted prices
prices = get_prices("PETR4", start=datetime(2024, 1, 1))

# Get list of ETF symbols
etf_symbols = get_symbols("etf")
```

## Common Use Cases

### Use Case 1: Equity Analysis

Download and analyze individual stock data:

```python
from datetime import datetime
from brasa import download_marketdata, process_marketdata, process_etl
from brasa.queries import get_returns, get_prices, get_symbols
from brasa.util import DateRange

# 1. Download historical data
period = DateRange(year=2024, calendar="B3")
download_marketdata("b3-cotahist-daily", refdate=period)
process_marketdata("b3-cotahist-daily")

# 2. Create returns dataset
process_etl("b3-equities-returns")

# 3. Get symbols in IBOV
ibov_symbols = get_symbols("equity", index="IBOV")

# 4. Get returns for analysis
returns = get_returns(ibov_symbols, start=datetime(2024, 1, 1))

# 5. Analyze
import pandas as pd
volatility = returns.groupby("symbol")["log_return"].std() * (252 ** 0.5)
print(volatility.sort_values(ascending=False).head(10))
```

### Use Case 2: Interest Rate Curve Analysis

Work with futures-implied rate curves (DAP — IPCA-linked real rates):

> **Note**: `b3-futures-settlement-prices` is frozen (no longer updated), so this
> workflow only covers historical data. The legacy DI1/DOL/etc. ETL chain was moved
> to `templates/legacy/`; a refactor sourcing futures from `b3-bvbg028`/`b3-bvbg086`
> is planned.

```python
from brasa import process_etl
from brasa.queries import get_dataset
from datetime import datetime
import pyarrow.compute as pc

# 1. Consolidate the (historical) futures settlement prices into staging
process_etl("b3-futures-settlement-prices-consolidated")

# 2. Create the DAP futures dataset with implied rates
process_etl("b3-futures-dap")

# 3. Query specific date
date = datetime(2024, 12, 31)
curve = (
    get_dataset("b3-futures-dap")
    .filter(pc.field("refdate") == date)
    .to_table()
    .to_pandas()
)

# 4. Plot curve
import matplotlib.pyplot as plt
plt.figure(figsize=(10, 6))
plt.plot(curve["business_days"], curve["adjusted_tax"] * 100)
plt.xlabel("Business Days")
plt.ylabel("Rate (%)")
plt.title(f"DAP Curve - {date.date()}")
plt.grid(True)
plt.show()
```

### Use Case 3: Portfolio Analysis

Analyze a portfolio of stocks:

```python
from brasa.queries import get_returns, get_prices
from datetime import datetime
import pandas as pd
import numpy as np

# Define portfolio
portfolio = {
    "PETR4": 0.3,  # 30% weight
    "VALE3": 0.3,  # 30% weight
    "ITUB4": 0.2,  # 20% weight
    "BBAS3": 0.2   # 20% weight
}

# Get returns
returns = get_returns(
    list(portfolio.keys()),
    start=datetime(2023, 1, 1),
    end=datetime(2024, 12, 31)
)

# Pivot to wide format
returns_wide = returns.pivot(index="refdate", columns="symbol", values="log_return")

# Calculate portfolio returns
weights = pd.Series(portfolio)
portfolio_returns = (returns_wide * weights).sum(axis=1)

# Calculate statistics
total_return = np.exp(portfolio_returns.sum()) - 1
volatility = portfolio_returns.std() * np.sqrt(252)
sharpe_ratio = portfolio_returns.mean() / portfolio_returns.std() * np.sqrt(252)

print(f"Total Return: {total_return:.2%}")
print(f"Annualized Volatility: {volatility:.2%}")
print(f"Sharpe Ratio: {sharpe_ratio:.2f}")
```

### Use Case 4: Dividend Analysis

Analyze dividend payments:

```python
from brasa import download_marketdata, process_marketdata, process_etl
from brasa.queries import get_dataset, get_symbols
import pyarrow.compute as pc

# 1. Download company data (uncomment if needed)
# companies = get_symbols("company-trading-name")
# download_marketdata("b3-cash-dividends", tradingName=companies)
# process_marketdata("b3-cash-dividends")

# 2. Process corporate actions
process_etl("b3-companies-cash-dividends")

# 3. Query dividends for specific symbol
dividends = (
    get_dataset("b3-companies-cash-dividends")
    .filter(pc.field("symbol") == "PETR4")
    .to_table()
    .to_pandas()
    .sort_values("last_date_prior_ex")
)

print(dividends[["last_date_prior_ex", "value_cash", "corporate_action_label"]])

# Calculate dividend yield
from brasa.queries import get_prices
from datetime import datetime

prices = get_prices("PETR4", start=datetime(2024, 1, 1))
avg_price = prices["close"].mean()
annual_dividends = dividends[dividends["last_date_prior_ex"].dt.year == 2024]["value_cash"].sum()
dividend_yield = annual_dividends / avg_price

print(f"Dividend Yield: {dividend_yield:.2%}")
```

### Use Case 5: Index Tracking

Track index composition and performance:

```python
from brasa import download_marketdata, process_marketdata, process_etl
from brasa.queries import get_symbols, get_returns, get_dataset
from datetime import datetime
import pyarrow.compute as pc

# 1. Download index data
download_marketdata("b3-indexes-composition")
process_marketdata("b3-indexes-composition")

# Download index quotes
from brasa.util import DateRange
period = DateRange(year=2024, calendar="B3")
download_marketdata("b3-bvbg087", refdate=period)
process_marketdata("b3-bvbg087")
process_etl("b3-indexes-returns")

# 2. Get index composition
index_name = "IBOV"
symbols = get_symbols("equity", index=index_name)
print(f"{index_name} has {len(symbols)} stocks")

# 3. Get index returns
index_returns = get_returns(index_name, start=datetime(2024, 1, 1))

# 4. Get constituents returns
stock_returns = get_returns(symbols, start=datetime(2024, 1, 1))

# 5. Compare index vs average constituent
import pandas as pd
avg_constituent = stock_returns.groupby("refdate")["log_return"].mean()
index_only = index_returns.set_index("refdate")["log_return"]

comparison = pd.DataFrame({
    "Index": index_only,
    "Average Constituent": avg_constituent
}).cumsum()

print(comparison.tail())
```

### Use Case 6: Volatility Surface Analysis

Work with options volatility data:

```python
from brasa import download_marketdata, process_marketdata
from brasa.queries import get_dataset
from brasa.util import DateRange
from datetime import datetime
import pyarrow.compute as pc

# 1. Download volatility surface data
period = DateRange(start=datetime(2024, 1, 1), end=datetime.today(), calendar="B3")
download_marketdata("b3-equities-volatility-surface", refdate=period)
process_marketdata("b3-equities-volatility-surface")

# 2. Query volatility for specific stock
symbol = "PETR4"
date = datetime(2024, 12, 31)

vol_surface = (
    get_dataset("b3-equities-volatility-surface")
    .filter(pc.field("symbol") == symbol)
    .filter(pc.field("refdate") == date)
    .to_table()
    .to_pandas()
)

# 3. Analyze implied volatility
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

fig = plt.figure(figsize=(12, 8))
ax = fig.add_subplot(111, projection='3d')
ax.scatter(
    vol_surface["days_to_maturity"],
    vol_surface["moneyness"],
    vol_surface["implied_volatility"]
)
ax.set_xlabel("Days to Maturity")
ax.set_ylabel("Moneyness")
ax.set_zlabel("Implied Volatility")
plt.title(f"Volatility Surface - {symbol} - {date.date()}")
plt.show()
```

## Working with Templates

### Available Templates

List all available templates:

```bash
ls templates/*.yaml
```

Common templates:
- **brasa-companies**: BOLSA-registered companies (consolidated ETL)
- **b3-cotahist-daily**: Daily historical prices
- **b3-futures-settlement-prices**: Futures settlement (frozen — historical data only)
- **b3-indexes-composition**: Index compositions
- **b3-company-info**: Company information
- **b3-cash-dividends**: Cash dividends
- **bcb-data**: BCB economic indicators

### Understanding Template Structure

Each template defines:
1. **Downloader**: How to download data
2. **Reader**: How to parse downloaded files
3. **Writer**: How to store processed data
4. **ETL** (optional): Transformations to apply

Example template inspection:

```python
from brasa.engine import retrieve_template

template = retrieve_template("b3-cotahist-daily")
print(f"ID: {template.id}")
print(f"Description: {template.description}")
print(f"Has Downloader: {template.has_downloader}")
print(f"Has Reader: {template.has_reader}")
print(f"Is ETL: {template.is_etl}")
```

## Data Storage Structure

### Cache Directory Layout

```
.brasa-cache/  (or $BRASA_DATA_PATH)
├── raw/                    # Downloaded raw files
│   ├── b3-cotahist-daily/
│   │   └── {checksum}/
│   │       └── COTAHIST_D*.ZIP
│   └── b3-futures-settlement-prices/
│       └── {checksum}/
│           └── *.txt
├── db/                     # Processed parquet files
│   ├── b3-cotahist-daily/
│   │   └── refdate=2024-01-02/
│   │       └── data.parquet
│   └── b3-equities-returns/
│       └── refdate=2024-01-02/
│           └── data.parquet
├── meta/                   # Metadata tracking
│   └── meta.db            # SQLite database
└── brasa.duckdb           # DuckDB analytical database
```

### Parquet Dataset Organization

Datasets are partitioned by `refdate` for efficient querying:

```python
from brasa.queries import get_dataset
import pyarrow.compute as pc

ds = get_dataset("b3-cotahist-daily")

# Partition pruning automatically applied
# Only reads necessary parquet files
df = ds.filter(pc.field("refdate") == datetime(2024, 1, 2)).to_table().to_pandas()
```

## Performance Tips

### 1. Use Date Filtering

Always filter by date when possible to leverage partitioning:

```python
from brasa.queries import get_dataset
import pyarrow.compute as pc
from datetime import datetime

ds = get_dataset("b3-cotahist-daily")

# Good: Uses partition pruning
df = ds.filter(pc.field("refdate") >= datetime(2024, 1, 1)).to_table().to_pandas()

# Bad: Reads all partitions
df = ds.to_table().to_pandas()
df = df[df["refdate"] >= datetime(2024, 1, 1)]
```

### 2. Select Only Needed Columns

Use `scanner()` to select specific columns:

```python
# Good: Reads only needed columns
df = (
    get_dataset("b3-cotahist-daily")
    .scanner(columns=["refdate", "symbol", "close"])
    .to_table()
    .to_pandas()
)

# Bad: Reads all columns
df = get_dataset("b3-cotahist-daily").to_table().to_pandas()
df = df[["refdate", "symbol", "close"]]
```

### 3. Batch Downloads

Download multiple dates in one call:

```python
from brasa.util import DateRange
from datetime import datetime

# Good: Single call with date range
period = DateRange(start=datetime(2024, 1, 1), end=datetime(2024, 12, 31), calendar="B3")
download_marketdata("b3-cotahist-daily", refdate=period)

# Bad: Multiple individual calls
for date in dates:
    download_marketdata("b3-cotahist-daily", refdate=date)
```

### 4. Cache Reuse

Let brasa's cache work for you:

```python
# First call downloads and caches
download_marketdata("b3-cotahist-daily", refdate=datetime(2024, 1, 2))

# Second call uses cache (instant)
download_marketdata("b3-cotahist-daily", refdate=datetime(2024, 1, 2))

# Force re-download only when needed
download_marketdata("b3-cotahist-daily", refdate=datetime(2024, 1, 2), reprocess=True)
```

### 5. Use DuckDB for Complex Queries

For complex analytical queries, use DuckDB:

```python
from brasa.queries import BrasaDB

con = BrasaDB.get_connection()

# Complex aggregation query
result = con.execute("""
    SELECT
        symbol,
        AVG(close) as avg_price,
        STDDEV(close) as std_price,
        MIN(close) as min_price,
        MAX(close) as max_price
    FROM 'b3-cotahist-daily'
    WHERE refdate >= '2024-01-01'
    GROUP BY symbol
    ORDER BY avg_price DESC
    LIMIT 10
""").df()

print(result)
```

## Troubleshooting

### Common Issues

#### 1. Download Fails

**Problem**: HTTP errors or empty files

**Solution**:
```python
# Check if data is available for that date
# Some data sources only publish on specific dates

# Force re-download
download_marketdata("template-id", refdate=date, reprocess=True)

# Check error messages in metadata
from brasa.engine import CacheManager, CacheMetadata

man = CacheManager()
meta = CacheMetadata("template-id")
meta.download_args = {"refdate": date}
if man.has_meta(meta):
    man.load_meta(meta)
    print(meta.processing_errors)
```

#### 2. Tuning Retry for Unstable Endpoints

**Problem**: An external API (e.g., B3 company details) fails intermittently
due to transient 5xx errors or connection resets.

**Solution**: Add `retry_attempts`, `retry_delay`, and `retry_backoff` to the
`downloader` section of the template YAML:

```yaml
downloader:
  function: brasa.downloaders.b3_url_encoded_download
  url: https://...
  download_delay: 2        # seconds between tasks (unchanged)
  retry_attempts: 2        # 2 extra attempts → 3 total
  retry_delay: 3.0         # 3 s before first retry
  retry_backoff: 2.0       # exponential: 3, 6, 12 …
```

**Key points**:
- `retry_delay` / `retry_backoff` control the sleep between retries.
  `download_delay` is the pacing between distinct download tasks and remains
  unchanged by retry configuration.
- Only transient failures (HTTP 408, 425, 429, 5xx by default) and generic
  `DownloadException` without a status code are retried. Content validation
  failures (`InvalidContentException`, `CorruptedContentException`) and
  duplicates are **never** retried.
- Each retry attempt is recorded as a separate `download_trials` row. The
  last row is the authoritative status for scheduling and reports.
- When all retries are exhausted the final status is `F` (FAILED) or `E`
  (ERROR); no new status code is introduced.

See [docs/TEMPLATES.md](TEMPLATES.md#downloader-retry-policy) for the full
specification.

#### 2. Processing Errors

**Problem**: Data processing fails

**Solution**:
```python
# Reprocess with error visibility
process_marketdata("template-id", reprocess=True)

# Check metadata for errors
# Errors are printed during processing
```

#### 3. Missing Data in Queries

**Problem**: Expected data not found

**Solution**:
```python
# Verify data was downloaded
from brasa.engine import CacheManager, CacheMetadata

man = CacheManager()
meta = CacheMetadata("template-id")
meta.download_args = {"refdate": date}
print(f"Has metadata: {man.has_meta(meta)}")

if man.has_meta(meta):
    man.load_meta(meta)
    print(f"Downloaded files: {meta.downloaded_files}")
    print(f"Processed files: {meta.processed_files}")

# Verify parquet files exist
import os
db_path = man.db_path("template-id")
print(f"Files in {db_path}:")
for root, dirs, files in os.walk(man.cache_path(db_path)):
    for file in files:
        print(os.path.join(root, file))
```

#### 4. Out of Memory

**Problem**: Large datasets cause memory issues

**Solution**:
```python
# Use lazy evaluation with PyArrow
ds = get_dataset("b3-cotahist-daily")

# Filter before converting to pandas
df = (
    ds.filter(pc.field("symbol") == "PETR4")
    .scanner(columns=["refdate", "close"])
    .to_table()
    .to_pandas()
)

# Or use DuckDB for large aggregations
from brasa.queries import BrasaDB
con = BrasaDB.get_connection()
result = con.execute("SELECT symbol, AVG(close) FROM 'b3-cotahist-daily' GROUP BY symbol").df()
```

#### 5. Incorrect Dates

**Problem**: Business days vs calendar days confusion

**Solution**:
```python
# Use appropriate calendar
from brasa.util import DateRange

# B3 trading calendar (excludes weekends and holidays)
period = DateRange(year=2024, calendar="B3")

# ANBIMA banking calendar
period = DateRange(year=2024, calendar="ANBIMA")

# All days (including weekends)
period = DateRange(year=2024, calendar="actual")
```

## Advanced Usage

### Custom ETL Pipelines

Create custom transformations:

```python
from brasa.queries import get_dataset, write_dataset
import pandas as pd
import pyarrow

# 1. Load source data
returns = get_dataset("b3-equities-returns").to_table().to_pandas()
prices = get_dataset("b3-equities-adjusted-prices").to_table().to_pandas()

# 2. Custom transformation
merged = returns.merge(prices, on=["refdate", "symbol"])
merged["value"] = merged["close"] * merged["log_return"]

# 3. Save result
schema = pyarrow.schema([
    pyarrow.field("refdate", pyarrow.timestamp("us")),
    pyarrow.field("symbol", pyarrow.string()),
    pyarrow.field("value", pyarrow.float64())
])
write_dataset(merged[["refdate", "symbol", "value"]], "custom-dataset", schema=schema)
```

### Direct DuckDB Queries

Complex analytics with DuckDB:

```python
from brasa.queries import BrasaDB

con = BrasaDB.get_connection()

# Create temporary table
con.execute("""
    CREATE TEMP TABLE daily_stats AS
    SELECT
        refdate,
        symbol,
        close,
        LAG(close) OVER (PARTITION BY symbol ORDER BY refdate) as prev_close,
        close / LAG(close) OVER (PARTITION BY symbol ORDER BY refdate) - 1 as daily_return
    FROM 'b3-cotahist-daily'
    WHERE symbol IN ('PETR4', 'VALE3', 'ITUB4')
""")

# Query temporary table
result = con.execute("""
    SELECT
        symbol,
        AVG(daily_return) as mean_return,
        STDDEV(daily_return) as volatility,
        MIN(daily_return) as min_return,
        MAX(daily_return) as max_return
    FROM daily_stats
    WHERE daily_return IS NOT NULL
    GROUP BY symbol
""").df()

print(result)
```

### Parallel Processing

Process multiple templates in parallel (external script):

```python
from multiprocessing import Pool
from brasa import process_marketdata

templates = [
    "b3-cotahist-daily",
    "b3-bvbg028",
    "b3-bvbg086",
    "b3-bvbg087"
]

def process_template(template):
    try:
        process_marketdata(template)
        return f"{template}: Success"
    except Exception as e:
        return f"{template}: Failed - {e}"

with Pool(4) as pool:
    results = pool.map(process_template, templates)
    for result in results:
        print(result)
```

## Best Practices

### 1. Always Use Business Day Calendars

```python
from brasa.util import DateRange

# Correct
period = DateRange(year=2024, calendar="B3")

# Wrong (may include weekends/holidays)
period = DateRange(year=2024, calendar="actual")
```

### 2. Download Before Processing

```python
# Correct order
download_marketdata("b3-cotahist-daily", refdate=period)
process_marketdata("b3-cotahist-daily")

# May fail if data not downloaded
process_marketdata("b3-cotahist-daily")
```

### 3. Check Data Availability

```python
from datetime import datetime
from brasa.util import DateRange

# Check if today is business day
cal = Calendar.load("B3")
if cal.isbizday(datetime.today()):
    # Data might be available
    pass
```

### 4. Version Control Templates

If you create custom templates, version control them:
```bash
git add templates/my-custom-template.yaml
git commit -m "Add custom template"
```

### 5. Monitor Cache Size

Periodically check and clean cache:
```python
from brasa.engine import CacheManager
import os

man = CacheManager()
cache_size = sum(
    os.path.getsize(os.path.join(root, f))
    for root, dirs, files in os.walk(man.cache_folder)
    for f in files
) / (1024**3)  # GB

print(f"Cache size: {cache_size:.2f} GB")
```

## Next Steps

- Explore available templates in `templates/` directory
- Review example notebooks in `notebooks/` directory
- Check API reference for detailed function documentation
- Consult architecture documentation for system internals

---

## Download Status Codes

Every download attempt in brasa is classified with exactly one deterministic
status code. These codes appear in CLI progress output, JSON reports, and the
`download_trials` SQLite table.

### Status Code Table

| Symbol | Name       | Meaning                                                        |
|--------|------------|----------------------------------------------------------------|
| `.`    | PASSED     | Download completed successfully.                               |
| `F`    | FAILED     | Expected download failure (e.g., HTTP 4xx/5xx via `DownloadException`). |
| `E`    | ERROR      | Unexpected unhandled exception during download.                |
| `S`    | SKIPPED    | Download was skipped because data already exists in cache (or cache marked invalid/duplicated). |
| `D`    | DUPLICATED | Target raw folder already exists (`DuplicatedFolderException`). Treated as cache-reusable; future attempts are skipped unless `reprocess=True`. |
| `I`    | INVALID    | Downloaded file failed template validation rules (`InvalidContentException`). Raw files are cleaned up; future attempts are skipped unless `reprocess=True`. |
| `W`    | WARNING    | Download succeeded but with non-terminal warnings.             |

### Reading CLI Output

During a batch download, brasa prints pytest-style symbols:

```
Status legend: .(passed) F(failed) E(error) S(skipped) D(duplicated) I(invalid)
Download b3-cotahist ....FSSD.FI [10/10] (3.2s)
```

### JSON Report Fields

When saving reports with `--report report.json`, each result includes:

```json
{
  "extra_info": {
    "download_status_code": ".",
    "download_status_name": "PASSED",
    "download_status_reason": "",
    "http_status": "404"
  }
}
```

The summary section includes `duplicated` and `invalid` counts alongside
the standard `passed`, `failed`, `errors`, `skipped`, and `warnings`.

### Scheduling Rules

- **DUPLICATED (D)**: If the last trial for a cache entry was `D` and the raw
  files still exist on disk, subsequent attempts are automatically skipped (`S`).
  If raw files are missing, a fresh download is triggered.
- **INVALID (I)**: Invalid entries are skipped on future attempts. Use
  `reprocess=True` to force a re-download.

### Database Migration

Existing cache databases are migrated automatically on startup. The
`download_trials` table gains four new columns: `status_code`, `status_name`,
`reason`, and `http_status`. Legacy rows are backfilled:

- `downloaded=1` → `status_code='.'`, `status_name='PASSED'`
- `downloaded=0` → `status_code='F'`, `status_name='FAILED'`

You can also run the migration manually:

```bash
uv run python scripts/migrate_download_trials_status.py [CACHE_PATH]
```

If `CACHE_PATH` is omitted, the script uses `$BRASA_DATA_PATH` or
`./.brasa-cache`.

**Rollback**: The migration only adds columns and backfills NULLs. It does not
remove or modify the legacy `downloaded` column. Reverting requires manually
dropping the new columns via SQLite.

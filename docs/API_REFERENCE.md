# Brasa API Reference

## Main API Functions

### Data Download

#### `download_marketdata(template_name, reprocess=False, **kwargs)`

Download market data from a specified template.

**Parameters**:
- `template_name` (str): Template ID (e.g., "b3-cotahist-daily")
- `reprocess` (bool): If True, re-download even if cached. Default: False
- `**kwargs`: Template-specific parameters (e.g., `refdate`, `symbol`, `index`)

**Returns**: None

**Example**:
```python
from datetime import datetime
from brasa import download_marketdata
from brasa.util import DateRange

# Download daily COTAHIST data
period = DateRange(start=datetime(2024, 1, 1), end=datetime.today(), calendar="B3")
download_marketdata("b3-cotahist-daily", refdate=period)

# Download index theoretical portfolio
download_marketdata("b3-indexes-theoretical-portfolio", index=["IBOV", "IFIX"])

# Re-download (force refresh)
download_marketdata("b3-cotahist-daily", refdate=datetime(2024, 1, 2), reprocess=True)
```

### Data Processing

#### `process_marketdata(template_name, reprocess=False)`

Process downloaded data into parquet format.

**Parameters**:
- `template_name` (str): Template ID
- `reprocess` (bool): If True, reprocess even if already processed. Default: False

**Returns**: None

**Example**:
```python
from brasa import process_marketdata

# Process downloaded COTAHIST files
process_marketdata("b3-cotahist-daily")

# Reprocess all data
process_marketdata("b3-cotahist-daily", reprocess=True)
```

#### `process_etl(template_name)`

Execute ETL transformation defined in template.

**Parameters**:
- `template_name` (str): ETL template ID

**Returns**: None

**Example**:
```python
from brasa import process_etl

# Consolidate DI1 futures
process_etl("b3-futures-di1-consolidated")

# Create equity returns dataset
process_etl("b3-equities-returns")

# Create BCB economic data
process_etl("bcb-data")
```

### Data Retrieval

#### `get_marketdata(template_name, reprocess=False, **kwargs)`

Download, process, and return data in one call.

**Parameters**:
- `template_name` (str): Template ID
- `reprocess` (bool): Force re-download and reprocess. Default: False
- `**kwargs`: Template-specific parameters

**Returns**: `pd.DataFrame` or `dict[str, pd.DataFrame]` or `None`

**Example**:
```python
from brasa import get_marketdata
from datetime import datetime

# Get data (downloads if not cached)
df = get_marketdata("b3-cotahist-daily", refdate=datetime(2024, 1, 2))

# Force refresh
df = get_marketdata("b3-cotahist-daily", refdate=datetime(2024, 1, 2), reprocess=True)
```

## Query API

### Dataset Access

#### `get_dataset(dataset_name, schema=None)`

Get PyArrow dataset for querying.

**Parameters**:
- `dataset_name` (str): Dataset/template ID
- `schema` (pyarrow.Schema, optional): Override schema

**Returns**: `pyarrow.dataset.Dataset`

**Example**:
```python
from brasa.queries import get_dataset
import pyarrow.compute as pc

# Get dataset
ds = get_dataset("b3-cotahist-daily")

# Filter and load
df = ds.filter(pc.field("symbol") == "PETR4").to_table().to_pandas()

# Complex filtering
df = (ds
    .filter(pc.field("refdate") >= datetime(2024, 1, 1))
    .filter(pc.field("symbol").isin(["PETR4", "VALE3"]))
    .scanner(columns=["refdate", "symbol", "close"])
    .to_table()
    .to_pandas()
)
```

#### `write_dataset(df, template_id, schema=None)`

Write DataFrame to parquet dataset.

**Parameters**:
- `df` (pd.DataFrame): Data to write
- `template_id` (str): Target dataset/template ID
- `schema` (pyarrow.Schema, optional): PyArrow schema

**Returns**: None

**Example**:
```python
from brasa.queries import write_dataset
import pandas as pd
import pyarrow

df = pd.DataFrame({
    "refdate": [datetime(2024, 1, 1)],
    "symbol": ["PETR4"],
    "close": [30.50]
})

schema = pyarrow.schema([
    pyarrow.field("refdate", pyarrow.timestamp("us")),
    pyarrow.field("symbol", pyarrow.string()),
    pyarrow.field("close", pyarrow.float64())
])

write_dataset(df, "custom-dataset", schema=schema)
```

### Time Series Functions

#### `get_returns(symbols, start=None, end=None, calendar="B3")`

Get return time series for symbols.

**Parameters**:
- `symbols` (str or list[str]): Symbol(s) to retrieve
- `start` (datetime, optional): Start date. Default: 2000-01-01
- `end` (datetime, optional): End date. Default: today
- `calendar` (str): Calendar for date alignment. Default: "B3"

**Returns**: `pd.DataFrame` with columns: `refdate`, `symbol`, `pct_return`, `log_return`

**Example**:
```python
from brasa.queries import get_returns
from datetime import datetime

# Single symbol
returns = get_returns("PETR4", start=datetime(2024, 1, 1))

# Multiple symbols
returns = get_returns(
    ["PETR4", "VALE3", "ITUB4"],
    start=datetime(2023, 1, 1),
    end=datetime(2024, 12, 31)
)

# Use ANBIMA calendar
returns = get_returns("PETR4", calendar="ANBIMA")
```

#### `get_prices(symbols, start=None, end=None, calendar="B3")`

Get adjusted price time series for symbols.

**Parameters**:
- `symbols` (str or list[str]): Symbol(s) to retrieve
- `start` (datetime, optional): Start date. Default: 2000-01-01
- `end` (datetime, optional): End date. Default: today
- `calendar` (str): Calendar for date alignment. Default: "B3"

**Returns**: `pd.DataFrame` with columns: `refdate`, `symbol`, `open`, `high`, `low`, `close`

**Example**:
```python
from brasa.queries import get_prices
from datetime import datetime

# Get adjusted prices
prices = get_prices("PETR4", start=datetime(2024, 1, 1))

# Multiple symbols
prices = get_prices(["PETR4", "VALE3"], start=datetime(2023, 1, 1))
```

### Symbol Lookup

#### `get_symbols(type, **kwargs)`

Get list of symbols by type.

**Parameters**:
- `type` (str): Symbol type
- `**kwargs`: Additional filters

**Symbol Types**:
- `"etf"`: Listed ETFs
- `"fii"`: Real Estate Investment Funds
- `"fixed-income-etf"` or `"fietf"`: Fixed income ETFs
- `"index"`: Stock indexes
- `"company"`: Listed companies
- `"company-cvm-code"`: CVM codes
- `"company-trading-name"`: Trading names
- `"equity"`: Equity symbols (can filter by sector or index)
- `"industry-sector"`: Sector names
- `"industry-subsector"`: Subsector names
- `"industry-segment"`: Segment names

**Returns**: `list`

**Example**:
```python
from brasa import get_symbols

# Get all ETFs
etfs = get_symbols("etf")

# Get all indexes
indexes = get_symbols("index")

# Get companies
companies = get_symbols("company")

# Get equities by sector
tech_stocks = get_symbols("equity", sector="Tecnologia da Informação")

# Get equities in index
ibov_stocks = get_symbols("equity", index="IBOV")

# Get IBOV stocks for specific end month
ibov_dec = get_symbols("equity", index="IBOV", end_month="2024-12-31")

# Get industry sectors
sectors = get_symbols("industry-sector")
```

#### `get_industry_sectors()`

Get all industry sector classifications.

**Returns**: `pd.DataFrame` with columns: `sector`, `subsector`, `segment`

**Example**:
```python
from brasa.queries import get_industry_sectors

sectors = get_industry_sectors()
print(sectors.head())
```

### Dataset Inspection

#### `describe(dataset_name)`

Print dataset schema and statistics.

**Parameters**:
- `dataset_name` (str): Dataset/template ID

**Returns**: None (prints to console)

**Example**:
```python
from brasa.queries import describe

describe("b3-cotahist-daily")
```

#### `show(dataset_name, n=10)`

Show first n rows of dataset.

**Parameters**:
- `dataset_name` (str): Dataset/template ID
- `n` (int): Number of rows. Default: 10

**Returns**: `pd.DataFrame`

**Example**:
```python
from brasa.queries import show

# Show first 10 rows
df = show("b3-cotahist-daily")

# Show first 100 rows
df = show("b3-cotahist-daily", n=100)
```

## DuckDB API

### BrasaDB Class

#### `BrasaDB.get_connection()`

Get DuckDB connection to brasa database.

**Returns**: `duckdb.DuckDBPyConnection`

**Example**:
```python
from brasa.queries import BrasaDB

con = BrasaDB.get_connection()
df = con.execute("SELECT * FROM 'b3-cotahist-daily' LIMIT 10").df()
```

#### `BrasaDB.create_view(template)`

Create DuckDB view from parquet dataset.

**Parameters**:
- `template` (str): Template/dataset ID

**Returns**: None

**Example**:
```python
from brasa.queries import BrasaDB

# Create view for easier querying
BrasaDB.create_view("b3-cotahist-daily")

con = BrasaDB.get_connection()
df = con.execute("SELECT * FROM 'b3-cotahist-daily' WHERE symbol = 'PETR4'").df()
```

#### `BrasaDB.create_views()`

Create views for all datasets.

**Returns**: None

**Example**:
```python
from brasa.queries import BrasaDB

# Create all views
BrasaDB.create_views()
```

## Utility API

### DateRange

#### Class: `DateRange(start=None, end=None, year=None, month=None, calendar=None)`

Generate business day date ranges.

**Parameters**:
- `start` (datetime, optional): Start date
- `end` (datetime, optional): End date (default: yesterday)
- `year` (int, optional): Year (replaces start/end)
- `month` (int, optional): Month (requires year)
- `calendar` (str, optional): Calendar name ("B3", "ANBIMA", "actual")

**Attributes**:
- `start`: First date in range
- `end`: Last date in range
- `dates`: List of dates in range

**Example**:
```python
from brasa.util import DateRange
from datetime import datetime

# Date range
period = DateRange(
    start=datetime(2024, 1, 1),
    end=datetime(2024, 12, 31),
    calendar="B3"
)

# Whole year
period = DateRange(year=2024, calendar="B3")

# Specific month
period = DateRange(year=2024, month=1, calendar="B3")

# Open-ended (to yesterday)
period = DateRange(start=datetime(2024, 1, 1), calendar="B3")

# Iterate
for date in period:
    print(date)

# Length
print(len(period))  # Number of business days
```

### DateRangeParser

#### Class: `DateRangeParser(calendar)`

Parse date range strings.

**Parameters**:
- `calendar` (str): Calendar name

**Methods**:
- `parse(text)`: Parse string to DateRange or list[datetime]

**Patterns**:
- `"2024"`: Full year
- `"2024:"`: From 2024 to today
- `"2024:2025"`: Year range
- `"2024-01"`: Single month
- `"2024-01:"`: From month to today
- `"2024-01-15"`: Single date
- `"2024-01-15:"`: From date to today
- `"2024-01-15:2024-12-31"`: Date range

**Example**:
```python
from brasa.util import DateRangeParser

parser = DateRangeParser("B3")

# Parse year
period = parser.parse("2024")

# Parse month
period = parser.parse("2024-01")

# Parse open range
period = parser.parse("2024-01:")

# Parse date range
period = parser.parse("2024-01-01:2024-12-31")

# Single date returns list
dates = parser.parse("2024-01-15")
```

### KwargsIterator

#### Class: `KwargsIterator(kwargs)`

Iterate over parameter combinations.

**Parameters**:
- `kwargs` (dict): Dictionary with scalar or list values

**Example**:
```python
from brasa.util import KwargsIterator

# Expand parameter combinations
kwargs = {
    "symbol": ["PETR4", "VALE3", "ITUB4"],
    "market": 10
}

for args in KwargsIterator(kwargs):
    print(args)
    # Output:
    # {"symbol": "PETR4", "market": 10}
    # {"symbol": "VALE3", "market": 10}
    # {"symbol": "ITUB4", "market": 10}

# Get total combinations
print(len(KwargsIterator(kwargs)))  # 3
```

## Cache Management API

### CacheManager

#### `retrieve_template(template_name)`

Load template configuration.

**Parameters**:
- `template_name` (str): Template ID

**Returns**: `MarketDataTemplate`

**Example**:
```python
from brasa.engine import retrieve_template

template = retrieve_template("b3-cotahist-daily")
print(template.description)
print(template.has_downloader)
```

#### CacheManager (Singleton)

Access via instantiation:

```python
from brasa.engine import CacheManager

man = CacheManager()

# Get cache paths
cache_folder = man.cache_folder
db_path = man.db_path("b3-cotahist-daily")

# Database connection
conn = man.meta_db_connection

# Check if data cached
from brasa.engine import CacheMetadata
meta = CacheMetadata("b3-cotahist-daily")
meta.download_args = {"refdate": datetime(2024, 1, 2)}
has_data = man.has_meta(meta)
```

## CLI API

### Command Line Interface

```bash
# Setup brasa (creates cache directories)
python -m brasa.cli setup

# Download market data
python -m brasa.cli download b3-cotahist-daily -d 2024-01-01 2024-12-31
python -m brasa.cli download b3-cotahist-daily -d 2024
python -m brasa.cli download b3-cotahist-daily -d 2024-01

# Process market data
python -m brasa.cli process b3-cotahist-daily
python -m brasa.cli process b3-futures-di1-consolidated  # ETL templates

# Create DuckDB views
python -m brasa.cli create-views
python -m brasa.cli create-view b3-cotahist-daily

# Query data
python -m brasa.cli query "SELECT * FROM 'b3-cotahist-daily' LIMIT 10"
python -m brasa.cli query "SELECT * FROM 'b3-cotahist-daily'" -o output.csv
python -m brasa.cli query "SELECT * FROM 'b3-cotahist-daily'" -o output.parquet
```

**CLI Arguments**:
- `-d, --date, --date-range`: Date or date range
- `--calendar`: Calendar to use (B3, ANBIMA, actual)
- `-o, --output`: Output file for queries (csv, json, parquet, orc, xlsx)

## Template IDs Reference

### B3 Data Sources

**Market Data**:
- `b3-cotahist-daily`: Daily historical prices
- `b3-cotahist-monthly`: Monthly historical prices
- `b3-cotahist-yearly`: Yearly historical prices
- `b3-bvbg028`: Futures and derivatives
- `b3-bvbg086`: Equity quotes
- `b3-bvbg087`: Index data
- `b3-futures-settlement-prices`: Futures settlement (frozen — historical data only)
- `b3-trades-intraday`: Intraday trades

**Options & Volatility**:
- `b3-equities-volatility-surface`: Volatility surfaces
- `b3-equity-options`: Options chains
- `b3-companies-options`: Company options

**Corporate Actions**:
- `b3-company-info`: Company information
- `b3-company-details`: Detailed company data
- `b3-cash-dividends`: Cash dividend events

**Indexes**:
- `b3-indexes-composition`: Index compositions
- `b3-indexes-theoretical-portfolio`: Theoretical portfolios

**Funds**:
- `b3-listed-stock-etfs`: Listed stock ETFs
- `b3-listed-fixed-income-etfs`: Fixed income ETFs
- `b3-listed-reits`: Real estate funds

**Lending**:
- `b3-lending-trades`: Lending trades
- `b3-lending-open-position`: Open positions
- `b3-loan-balance`: Loan balances

**OTC**:
- `b3-otc-trade-information`: OTC trade data

**Economic Indicators**:
- `b3-economic-indicators-price`: Price-based indicators
- `b3-economic-indicators-fwf`: Fixed-width indicators

### ETL Datasets

**Futures**:
- `b3-futures`: Futures contracts with prices and settlement rates (bvbg028 + bvbg086)
- `b3-futures-settlement-prices-consolidated`: Settlement prices repartitioned by commodity
- `b3-futures-di1-consolidated`: DI1 futures consolidated
- `b3-futures-dap`: DAP futures
- `b3-futures-dap-first-generic`: DAP front-month contract

> The legacy futures ETLs (`b3-futures-{di1,ddi,dol,wdo,win,frc}` and their
> first-generic/adjusted chains) were moved to `brasa/files/templates/legacy/` and are no
> longer available.

**Curves**:
- `b3-curves-di1`: DI1 prefixed yield curve (b3-futures + b3-economic-indicators)
- `b3-curves-di1-standard`: Interpolated DI1 curve
- `b3-curves-di1-standard-returns`: DI1 curve returns
- `b3-curves-dap`: DAP term structure
- `b3-curves-dap-standard`: Interpolated DAP curve
- `b3-curves-dap-standard-returns`: DAP curve returns

**Equities**:
- `b3-cotahist`: Merged COTAHIST (yearly + daily)
- `b3-equities-register`: Equity registration data
- `b3-equities-spot-market`: Spot market equities
- `b3-equities-returns`: Equity returns
- `b3-equities-adjusted-prices`: Corporate action adjusted prices

**Companies**:
- `b3-companies-info`: Company info (processed)
- `b3-companies-details`: Company details (processed)
- `b3-companies-properties`: Merged company properties
- `b3-equity-symbols-properties`: Symbol metadata
- `b3-companies-cash-dividends`: Cash dividends (processed)
- `b3-companies-stock-dividends`: Stock dividends
- `b3-companies-subscriptions`: Subscription rights

**Funds**:
- `b3-listed-funds`: All listed funds (ETF + FII)

**Indexes**:
- `b3-indexes-returns`: Index returns
- `b3-indexes-adjusted-prices`: Index adjusted prices
- `b3-indexes-theoretical-portfolio-with-sectors`: Portfolio with sectors

### BCB Data Sources

- `bcb-data`: Economic indicators (CDI, SELIC, IPCA, IGPM)
- `bcb-currency-data`: Exchange rates
- `bcb-sgs-data`: SGS time series

### ANBIMA Data Sources

- Various bond and fixed income templates

### Custom Datasets

- `brasa-returns`: Combined returns
- `brasa-prices`: Combined prices
- `brasa-ohlc-prices`: OHLC prices

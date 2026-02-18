# Brasa Configuration Guide

## Overview

Brasa's behavior can be configured through:
1. Environment variables
2. YAML template files
3. Python code parameters
4. Calendar settings

> **📚 For Template Configuration Details:** See [TEMPLATES.md](TEMPLATES.md) for comprehensive guidance on pipeline-based templates (the **recommended modern approach** using `reader.pipeline`, `etl.pipeline`, and typed `fields`/`datasets`). This guide documents the legacy function-based approach. Both approaches are supported, but new templates should use the pipeline approach.

## Environment Variables

### BRASA_DATA_PATH

Controls where brasa stores cached data.

**Default**: `.brasa-cache` in current working directory. But always check for the environment variable BRASA_DATA_PATH.

**Usage**:
```bash
# Linux/Mac
export BRASA_DATA_PATH=/data/brasa

# Windows
set BRASA_DATA_PATH=D:\brasa

# Python
import os
os.environ["BRASA_DATA_PATH"] = "/data/brasa"
```

**Directory Structure**:
```
$BRASA_DATA_PATH/
├── raw/           # Downloaded files
├── db/            # Parquet datasets
├── meta/          # Metadata SQLite DB
└── brasa.duckdb   # DuckDB database
```

## Template Configuration

Templates are YAML files in the `templates/` directory that define how to download, parse, and process data.

### Template Structure

```yaml
id: unique-template-id
description: Human-readable description

downloader:
  function: brasa.downloaders.{function_name}
  url: https://example.com/data
  format: zip|txt|json|html
  encoding: utf-8
  verify_ssl: true|false
  extra-key: date|datetime|null
  download_delay: 5    # Delay between downloads (for rate limiting)
  validator: brasa.downloaders.{validator_function}
  args:
    param1: ~           # Required parameter (must be provided)
    param2: default     # Optional with default value

reader:
  encoding: utf-8
  pipeline:
    - step: step1

writer:
  partitioning:
    - refdate           # Partition by date
    - symbol           # Additional partition (optional)

fields:                 # For simple file formats
  - name: field_name
    description: Field description
    type: numeric|character|date(format="%Y%m%d")|posixct

datasets:                  # For multiple datasets
  dataset_name:
    tag: tag
    fields:
      - name: field_name
        description: Description
        type: numeric|character|date

etl:                    # For ETL-only templates
  function: brasa.etl.{function_name}
  param1: value1
  param2: value2
```

### Configuration Options

#### Downloader Section

**function**: Python function to execute download
- `brasa.downloaders.simple_download`: Basic HTTP GET
- `brasa.downloaders.datetime_download`: URL with date formatting
- `brasa.downloaders.b3_url_encoded_download`: B3 API with base64 params
- `brasa.downloaders.b3_paged_url_encoded_download`: B3 paged API
- `brasa.downloaders.settlement_prices_download`: Futures settlement
- `brasa.downloaders.bcb_sgs_download`: BCB SGS API

**url**: Download URL (can include date format codes for datetime_download)
```yaml
url: https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D%d%m%Y.ZIP
# %d = day, %m = month, %Y = year
```

**format**: Expected file format
- `zip`: ZIP archive (will be extracted)
- `txt`: Text file
- `json`: JSON file
- `html`: HTML file
- `xml`: XML file

**encoding**: Character encoding (default: utf-8)
- `utf-8`: UTF-8
- `latin1`: Latin-1 / ISO-8859-1
- `cp1252`: Windows-1252

**verify_ssl**: SSL certificate verification (default: true)
```yaml
verify_ssl: false  # For sites with certificate issues
```

**extra-key**: Additional cache key component
- `date`: Use current date (YYYY-MM-DD)
- `datetime`: Use current datetime
- `null`: No extra key

**validator**: Function to validate downloaded file
- `brasa.downloaders.validate_empty_file`: Check file not empty
- `brasa.downloaders.validate_json_empty_file`: Check JSON not empty

**download_delay**: Delay in seconds between consecutive downloads (default: 0)
```yaml
download_delay: 5  # Wait 5 seconds between downloads
```
This option is useful for APIs with rate limiting. When downloading multiple items
(e.g., company info for all listed companies), the delay prevents hitting rate limits.
The delay is only applied between downloads, not before the first one.

**args**: Download function arguments
```yaml
args:
  refdate: ~          # Required, must be passed by caller
  symbol: PETR4       # Optional with default
  market: 10          # Fixed value
```

#### Reader Section

**function**: Python function to parse file
- `brasa.readers.read_b3_cotahist`: COTAHIST parser
- `brasa.readers.csv.read_csv`: CSV parser
- Custom parser function

**encoding**: File encoding for reading

**output-filename-format**: Date format for output files
```yaml
output-filename-format: "%Y-%m-%d"      # 2024-01-15
output-filename-format: "%Y%m%d"        # 20240115
output-filename-format: "%Y-%m-%d_%H%M" # 2024-01-15_1430
```

**multi**: Multiple output datasets
```yaml
multi:
  HistoricalPrices: prices
  Trades: trades
  Indices: indices
```

#### Writer Section

**partitioning**: Columns to partition by
```yaml
partitioning:
  - refdate          # Partition by date (recommended)
  - symbol           # Sub-partition by symbol
```

Benefits:
- Faster queries with date filtering
- Parallel processing
- Smaller file sizes

#### Fields Section

For simple file formats without parts:

```yaml
fields:
  - name: symbol
    description: Stock symbol
    width: 12         # Fixed-width only
    handler:
      type: character

  - name: close_price
    description: Closing price
    width: 13
    handler:
      type: numeric
      format: pt-br   # Brazilian number format (1.234,56)

  - name: trade_date
    description: Trading date
    width: 8
    handler:
      type: date
      format: "%Y%m%d"
```

**Field Handler Types**:

1. **character**: String data
   ```yaml
   handler:
     type: character
   ```

2. **numeric**: Numeric data
   ```yaml
   handler:
     type: numeric
     format: pt-br    # Optional: Brazilian format
   ```

3. **date**: Date/datetime data
   ```yaml
   handler:
     type: date
     format: "%Y%m%d"   # strptime format
   ```

   Common formats:
   - `%Y%m%d`: 20240115
   - `%Y-%m-%d`: 2024-01-15
   - `%d/%m/%Y`: 15/01/2024
   - `%Y%m%d%H%M%S`: 20240115143000

4. **posixct**: Timestamp
   ```yaml
   handler:
     type: posixct
     format: "%Y-%m-%d %H:%M:%S"
   ```

#### Parts Section

For multi-part fixed-width files (MFWF):

```yaml
parts:
  Header:
    pattern: ^00      # Lines starting with 00
    fields:
      - name: record_type
        width: 2
        handler:
          type: character
      - name: file_date
        width: 8
        handler:
          type: date
          format: "%Y%m%d"

  Data:
    pattern: ^01      # Lines starting with 01
    fields:
      - name: record_type
        width: 2
        handler:
          type: character
      - name: symbol
        width: 12
        handler:
          type: character
      - name: price
        width: 13
        handler:
          type: numeric
          format: pt-br

  Trailer:
    pattern: ^99      # Lines starting with 99
    fields:
      - name: record_type
        width: 2
        handler:
          type: character
      - name: total_records
        width: 11
        handler:
          type: numeric
```

**pattern**: Regular expression to match line type
- `^00`: Lines starting with "00"
- `^01`: Lines starting with "01"
- `.*HEADER.*`: Lines containing "HEADER"

#### ETL Section

For templates that only transform existing data:

```yaml
etl:
  function: brasa.etl.create_b3_rate_futures
  futures_dataset: b3-futures-settlement-prices
  maturity_day: first day
  compounding: discrete
  commodity: DI1
```

**function**: ETL transformation function

**Additional Parameters**: Passed to ETL function
- Custom parameters for specific transformations
- Reference to source datasets
- Calculation settings

### Template Examples

#### Example 1: Simple Daily Download

```yaml
id: b3-cotahist-daily
filename: COTAHIST
filetype: MFWF
description: Daily historical equity prices

downloader:
  verify_ssl: false
  function: brasa.downloaders.datetime_download
  url: https://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_D%d%m%Y.ZIP
  format: zip
  args:
    refdate: ~

reader:
  function: brasa.readers.read_b3_cotahist

writer:
  partitioning:
    - refdate

parts:
  Header:
    pattern: ^00
    fields:
      - name: tipo_registro
        width: 2
        handler:
          type: character
      - name: data_geracao_arquivo
        width: 8
        handler:
          type: date
          format: "%Y%m%d"

  HistoricalPrices:
    pattern: ^01
    fields:
      - name: tipo_registro
        width: 2
        handler:
          type: character
      - name: refdate
        width: 8
        handler:
          type: date
          format: "%Y%m%d"
      - name: symbol
        width: 12
        handler:
          type: character
      - name: close
        width: 13
        handler:
          type: numeric
          format: pt-br
```

#### Example 2: API Download

```yaml
id: b3-company-info
description: Company information from B3 API

downloader:
  function: brasa.downloaders.b3_url_encoded_download
  url: https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany
  format: json
  validator: brasa.downloaders.validate_json_empty_file
  args:
    issuingCompany: ~
    language: en-us

reader:
  function: brasa.parsers.b3.company_info_parser
  output-filename-format: "%Y-%m-%d"

writer:
  partitioning:
    - refdate
```

#### Example 3: ETL with In-Memory Query

```yaml
id: brasa-companies
description: BOLSA-registered companies consolidated dataset from CVM and B3

etl:
  pipeline:
    - step: run_query
      datasets:
        - input.cvm-companies-registration
        - input.b3-company-details
      query: |
        SELECT
          cvm.code_cvm,
          COALESCE(b3.companyName, cvm.denom_social) as company_name,
          b3.issuingCompany as asset_name,
          cvm.sit as company_status
        FROM 'input.cvm-companies-registration' cvm
        LEFT JOIN 'input.b3-company-details' b3
          ON cvm.code_cvm = CAST(b3.codeCVM AS VARCHAR)
        WHERE cvm.tp_merc = 'BOLSA'

writer:
  layer: staging
```

#### Example 4: Legacy ETL Function

```yaml
id: b3-futures-di1
description: DI1 futures curve

etl:
  function: brasa.etl.create_b3_rate_futures
  futures_dataset: b3-futures-settlement-prices
  maturity_day: first day
  compounding: discrete
  commodity: DI1
```

#### Example 5: Multi-Output Reader

```yaml
id: b3-bvbg028
description: Derivatives market data

downloader:
  function: brasa.downloaders.datetime_download
  url: https://www.b3.com.br/pesquisapregao/download?filelist=BVBG.028.01_%Y%m%d.gz
  format: txt
  args:
    refdate: ~

reader:
  function: brasa.parsers.b3.bvbg028_parser
  multi:
    futures: futures-data
    options: options-data

writer:
  partitioning:
    - refdate
```

## Calendar Configuration

### Available Calendars

Brasa uses the `bizdays` library for business day calendars:

**B3**: Brazilian stock exchange calendar
- Trading days only
- Excludes Brazilian holidays
- Monday-Friday except holidays

**ANBIMA**: Brazilian banking calendar
- Banking business days
- Similar to B3 but may differ on some days

**actual**: Actual calendar
- All days including weekends
- No holiday exclusions

### Using Calendars

```python
from bizdays import Calendar

# Load calendar
cal = Calendar.load("B3")

# Check if business day
cal.isbizday(datetime(2024, 1, 15))  # Returns bool

# Get next/previous business day
cal.following(datetime(2024, 1, 14))  # Next business day
cal.preceding(datetime(2024, 1, 14))  # Previous business day

# Offset by business days
cal.offset(datetime(2024, 1, 15), 5)  # 5 business days later

# Generate sequence
dates = cal.seq(datetime(2024, 1, 1), datetime(2024, 12, 31))

# Count business days
n_days = cal.bizdays(datetime(2024, 1, 1), datetime(2024, 12, 31))
```

### Calendar in DateRange

```python
from brasa.util import DateRange

# Use B3 calendar (default for Brazilian market data)
period = DateRange(year=2024, calendar="B3")

# Use ANBIMA for fixed income
period = DateRange(year=2024, calendar="ANBIMA")

# Use actual for daily data
period = DateRange(year=2024, calendar="actual")
```

## DuckDB Configuration

### Connection Settings

DuckDB database location: `{BRASA_DATA_PATH}/brasa.duckdb`

Access via:
```python
from brasa.queries import BrasaDB

con = BrasaDB.get_connection()
```

### DuckDB Settings

Configure DuckDB:
```python
con = BrasaDB.get_connection()

# Set memory limit
con.execute("SET memory_limit='4GB'")

# Set thread count
con.execute("SET threads=4")

# Enable progress bar
con.execute("SET enable_progress_bar=true")
```

### Creating Views

Views provide SQL access to parquet datasets:

```python
from brasa.queries import BrasaDB

# Create single view
BrasaDB.create_view("b3-cotahist-daily")

# Create all views
BrasaDB.create_views()

# Query view
con = BrasaDB.get_connection()
df = con.execute("SELECT * FROM 'b3-cotahist-daily' LIMIT 10").df()
```

## PyArrow Configuration

### Schema Definition

For ETL outputs, define schema:

```python
import pyarrow

schema = pyarrow.schema([
    pyarrow.field("refdate", pyarrow.timestamp("us")),
    pyarrow.field("symbol", pyarrow.string()),
    pyarrow.field("value", pyarrow.float64())
])

write_dataset(df, "dataset-name", schema=schema)
```

### Common PyArrow Types

- `pyarrow.string()`: String/text
- `pyarrow.int32()`: 32-bit integer
- `pyarrow.int64()`: 64-bit integer
- `pyarrow.float32()`: 32-bit float
- `pyarrow.float64()`: 64-bit float
- `pyarrow.bool_()`: Boolean
- `pyarrow.date32()`: Date (days since epoch)
- `pyarrow.timestamp("us")`: Timestamp (microseconds)
- `pyarrow.timestamp("ns")`: Timestamp (nanoseconds)

## Performance Tuning

### Cache Settings

**Location**: Control via `BRASA_DATA_PATH`
- Use fast SSD for better performance
- Ensure sufficient disk space (datasets can be large)

**Cleanup**: Periodically remove old cache
```python
from brasa.engine import CacheManager

man = CacheManager()

# Remove specific template data
# (Manually delete from cache_path)

# Check cache size
import os
cache_size = sum(
    os.path.getsize(os.path.join(root, f))
    for root, dirs, files in os.walk(man.cache_folder)
    for f in files
)
print(f"Cache size: {cache_size / 1024**3:.2f} GB")
```

### Download Optimization

**Batch downloads**:
```python
# Good: Single call with date range
period = DateRange(year=2024, calendar="B3")
download_marketdata("b3-cotahist-daily", refdate=period)

# Bad: Individual calls
for date in dates:
    download_marketdata("b3-cotahist-daily", refdate=date)
```

**Parallel processing**:
```python
from multiprocessing import Pool

templates = ["template1", "template2", "template3"]

with Pool(4) as pool:
    pool.map(process_marketdata, templates)
```

### Query Optimization

**Use partition pruning**:
```python
# Good: Filters on partition column
ds.filter(pc.field("refdate") >= datetime(2024, 1, 1))

# Bad: Filters after loading
df = ds.to_table().to_pandas()
df = df[df["refdate"] >= datetime(2024, 1, 1)]
```

**Select specific columns**:
```python
# Good: Select columns early
ds.scanner(columns=["refdate", "symbol", "close"])

# Bad: Load all columns
ds.to_table().to_pandas()[["refdate", "symbol", "close"]]
```

## Logging Configuration

Enable logging for debugging:

```python
import logging

# Configure brasa logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# Debug level for more details
logging.getLogger("brasa").setLevel(logging.DEBUG)
```

## Custom Configuration File

Create a configuration file for your project:

```python
# config.py
import os
from datetime import datetime
from brasa.util import DateRange

# Environment
os.environ["BRASA_DATA_PATH"] = "/data/brasa"

# Common settings
DEFAULT_CALENDAR = "B3"
DEFAULT_START_DATE = datetime(2020, 1, 1)

# Helper functions
def get_recent_period(days=30):
    end = datetime.today()
    start = end - timedelta(days=days)
    return DateRange(start=start, end=end, calendar=DEFAULT_CALENDAR)

# Template lists
EQUITY_TEMPLATES = [
    "b3-cotahist-daily",
    "b3-bvbg086",
]

FUTURES_TEMPLATES = [
    "b3-futures-settlement-prices",
    "b3-bvbg028",
]

# Use in scripts
from config import get_recent_period, EQUITY_TEMPLATES
from brasa import download_marketdata

period = get_recent_period(60)
for template in EQUITY_TEMPLATES:
    download_marketdata(template, refdate=period)
```

## Best Practices

1. **Set BRASA_DATA_PATH**: Don't use default `.brasa-cache` in production
2. **Use appropriate calendars**: B3 for equities, ANBIMA for fixed income
3. **Partition by date**: Always include `refdate` in partitioning
4. **Define schemas**: Explicit schemas prevent type issues
5. **Batch operations**: Download/process multiple items together
6. **Cache wisely**: Let cache work, but clean periodically
7. **Filter early**: Apply filters before loading to pandas
8. **Version templates**: Keep custom templates in version control

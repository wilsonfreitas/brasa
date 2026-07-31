# Brasa Configuration Guide

## Overview

Brasa's behavior can be configured through:
1. Environment variables
2. YAML template files
3. Python code parameters
4. Calendar settings

> **📚 For Template Configuration Details:** See [TEMPLATES.md](TEMPLATES.md) for comprehensive guidance on pipeline-based templates (the **recommended modern approach** using `reader.pipeline`, `etl.pipeline`, and typed `fields`/`datasets`). Legacy function-based templates are no longer supported; all templates use the pipeline approach.

## Environment Variables

### BRASA_DATA_PATH

Controls where brasa stores cached data.

**Resolution order**: (1) the `BRASA_DATA_PATH` environment variable, if set; (2) the `data_path` entry in `~/.config/brasa/config.toml`, written by `brasa init`. There is no implicit fallback — data-touching commands raise `BrasaNotConfiguredError` (the CLI prints an actionable message pointing to `brasa init`) until one of the two is configured.

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

Template structure, downloader/reader/writer/fields configuration, and worked examples are documented in [TEMPLATES.md](TEMPLATES.md). The legacy function-based template format previously described here was removed.

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

1. **Configure the data path**: run `brasa init` once (or set `BRASA_DATA_PATH` in CI/containers)
2. **Use appropriate calendars**: B3 for equities, ANBIMA for fixed income
3. **Partition by date**: Always include `refdate` in partitioning
4. **Define schemas**: Explicit schemas prevent type issues
5. **Batch operations**: Download/process multiple items together
6. **Cache wisely**: Let cache work, but clean periodically
7. **Filter early**: Apply filters before loading to pandas
8. **Version templates**: Keep custom templates in version control

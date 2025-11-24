# Brasa Architecture Documentation

## System Architecture

### Overview

Brasa follows a modular, template-driven architecture that separates concerns across distinct layers:

```
┌─────────────────────────────────────────────────────────────┐
│                    User Interface Layer                     │
│  - CLI (brasa.cli)                                          │
│  - Python API (brasa.__init__)                              │
│  - Jupyter Notebooks                                        │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    Template Layer                            │
│  - YAML configuration files (templates/*.yaml)              │
│  - MarketDataTemplate (engine.py)                           │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    Engine Layer                              │
│  - CacheManager: Metadata & file management                 │
│  - MarketDataDownloader: Download orchestration             │
│  - MarketDataReader: Parse raw data                         │
│  - MarketDataWriter: Write processed data                   │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                 Download & Parse Layer                       │
│  - downloaders/*.py: HTTP/API clients                       │
│  - parsers/b3/*.py: B3 file parsers                         │
│  - parsers/anbima/*.py: ANBIMA parsers                      │
│  - readers/*.py: Generic readers                            │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    Storage Layer                             │
│  - Cache: .brasa-cache/ (configurable via BRASA_DATA_PATH)  │
│    - raw/: Downloaded files                                 │
│    - db/: Parquet datasets                                  │
│    - meta/: SQLite metadata DB                              │
│  - DuckDB: brasa.duckdb analytical database                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    ETL Layer                                 │
│  - etl.py: Transformation functions                         │
│  - Derived datasets creation                                │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    Query Layer                               │
│  - queries.py: High-level data access                       │
│  - BrasaDB: DuckDB connection manager                       │
│  - Helper functions: get_returns, get_prices, get_symbols   │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. CacheManager (Singleton)

**Location**: `brasa/engine.py`

**Purpose**: Centralized management of all cached data, metadata, and file system operations.

**Key Responsibilities**:
- Initialize cache directory structure
- Manage SQLite metadata database
- Track downloaded and processed files
- Generate checksums for deduplication
- Handle file path resolution

**Directory Structure**:
```
.brasa-cache/  (or $BRASA_DATA_PATH)
├── raw/          # Downloaded files organized by template
│   └── {template_id}/
│       └── {checksum}/
│           └── downloaded_files
├── db/           # Processed parquet datasets
│   └── {template_id}/
│       └── *.parquet
├── meta/         # Metadata and tracking
│   └── meta.db   # SQLite database
└── brasa.duckdb  # DuckDB analytical database
```

**Key Methods**:
- `cache_path(fname)`: Convert relative to absolute cache paths
- `db_path(name)`: Get database storage path
- `has_meta(meta)`: Check if metadata exists
- `save_meta(meta)`: Persist cache metadata
- `load_meta(meta)`: Load cached metadata
- `download_marketdata(meta)`: Execute download
- `read_marketdata(meta)`: Parse and store data

### 2. MarketDataTemplate

**Location**: `brasa/engine.py`

**Purpose**: Load and represent YAML template configurations.

**Structure**:
```python
class MarketDataTemplate:
    id: str                          # Unique template identifier
    description: str                 # Human-readable description
    has_downloader: bool             # Has download configuration
    has_reader: bool                 # Has read configuration
    has_parts: bool                  # Multi-part file format
    is_etl: bool                     # Is ETL-only (no download)
    downloader: MarketDataDownloader # Download configuration
    reader: MarketDataReader         # Read configuration
    writer: MarketDataWriter         # Write configuration
    fields: TemplateFields           # Field definitions
    parts: list                      # Multi-part definitions
    etl: MarketDataETL              # ETL configuration
```

**Template YAML Structure**:
```yaml
id: template-name
description: Human-readable description
filename: BASE_FILENAME
filetype: MFWF|CSV|JSON|HTML|XML
locale: en|pt_BR

downloader:
  function: brasa.downloaders.{function_name}
  url: https://...
  format: zip|txt|json|html
  encoding: utf-8
  verify_ssl: true|false
  args:
    param1: ~  # Required parameter
    param2: default_value

reader:
  function: brasa.readers.{function_name}
  encoding: utf-8
  output-filename-format: "%Y-%m-%d"
  multi:  # For multi-part outputs
    part1: output_name1
    part2: output_name2

writer:
  partitioning:
    - refdate

fields:  # For simple files
  - name: field_name
    description: Field description
    width: 10
    handler:
      type: numeric|character|date
      format: "%Y%m%d"

parts:  # For multi-part files (MFWF)
  PartName:
    pattern: ^regex_pattern
    fields:
      - name: field_name
        ...

etl:  # For ETL-only templates
  function: brasa.etl.{function_name}
  param1: value1
  param2: value2
```

### 3. CacheMetadata

**Location**: `brasa/engine.py`

**Purpose**: Track metadata for each download/process operation.

**Attributes**:
```python
template: str                        # Template ID
timestamp: datetime                  # When downloaded
response: Any                        # HTTP response metadata
download_checksum: str               # Hash of downloaded files
download_args: dict                  # Arguments used for download
downloaded_files: list[str]          # Paths to downloaded files
processed_files: dict[str, str]      # Processed file mappings
extra_key: str                       # Additional cache key component
processing_errors: str               # Error messages if any
```

**Checksum Calculation**:
```python
id = MD5(template_id + sorted(download_args) + extra_key)
```

This ensures each unique combination of parameters creates a separate cache entry.

### 4. Downloader Layer

**Location**: `brasa/downloaders/`

**Components**:

1. **downloaders.py**: Core downloader classes
   - `SimpleDownloader`: Basic HTTP GET
   - `DatetimeDownloader`: URL with date formatting
   - `B3URLEncodedDownloader`: Base64-encoded parameters
   - `B3PagedURLEncodedDownloader`: Paginated API calls
   - `B3FilesURLDownloader`: File listings
   - `SettlementPricesDownloader`: Futures settlement
   - `BCBSGSDownloader`: Central Bank SGS API

2. **helpers.py**: Wrapper functions
   - `simple_download()`
   - `datetime_download()`
   - `b3_url_encoded_download()`
   - `b3_paged_url_encoded_download()`
   - `settlement_prices_download()`
   - `bcb_sgs_download()`
   - `validate_empty_file()`: File validation
   - `validate_json_empty_file()`: JSON validation

**Download Flow**:
```
1. Template specifies downloader.function
2. MarketDataDownloader.download(**kwargs) called
3. Wrapper function creates appropriate Downloader instance
4. Downloader.download() executes HTTP request
5. Returns (file_object, response_headers)
6. File saved to cache with checksum
7. Metadata updated in SQLite
```

### 5. Parser Layer

**Location**: `brasa/parsers/`

**Structure**:
```
parsers/
├── b3/                    # B3-specific parsers
│   ├── bvbg028.py        # Futures/derivatives
│   ├── bvbg086.py        # Equity quotes
│   ├── bvbg087.py        # Index data
│   ├── cotahist.py       # Historical prices
│   ├── cdi.py            # CDI rates
│   ├── futures_settlement_prices.py
│   ├── indic.py          # Economic indicators
│   ├── stock_indexes.py  # Index compositions
│   └── taxaswap.py       # Swap rates
├── anbima/               # ANBIMA parsers
│   ├── debentures.py
│   └── tpf.py           # Treasury bonds
├── cvm.py               # CVM data
├── td.py                # Tesouro Direto
├── fwf.py               # Fixed-width file parser
└── util.py              # Parser utilities
```

**Parser Pattern**:
Each parser typically implements a class with:
- `__init__(template_config)`: Initialize with template
- `parse(file_object)`: Main parsing method
- Returns: `pd.DataFrame` or `dict[str, pd.DataFrame]`

### 6. Reader Layer

**Location**: `brasa/readers/`

**Components**:

1. **csv.py**: CSV file readers
2. **helpers.py**: Generic reader utilities

**Key Function**: `read_b3_cotahist(meta: CacheMetadata)`
- Parses multi-part fixed-width format (MFWF)
- Uses template field definitions
- Returns structured DataFrame

### 7. ETL Layer

**Location**: `brasa/etl.py`

**Purpose**: Transform raw data into analytical datasets.

**Key Functions** (988 lines total):

**Interest Rate Futures**:
- `create_b3_rate_futures()`: DI1, DDI curves
- `create_b3_price_futures()`: Commodity futures
- `create_b3_price_futures_adjusted()`: Adjusted prices
- `create_b3_futures_first_generic()`: Front-month contracts

**Curves**:
- `create_b3_curves_di1()`: DI1 interest rate curves
- `create_b3_curves()`: Generic curve creation
- `create_b3_curves_standard_terms()`: Interpolated curves
- `create_rate_returns()`: Curve returns calculation

**Equities**:
- `create_cotahist_dataset()`: Merge yearly/daily COTAHIST
- `create_equities_spot_market_dataset()`: Filter spot market
- `create_equities_returns()`: Calculate returns
- `create_adjusted_prices()`: Corporate action adjustments

**Company Data**:
- `create_b3_companies_details()`: Company metadata
- `create_b3_companies_info()`: Stock information
- `create_b3_companies_properties()`: Merged properties
- `create_b3_equity_symbols_properties()`: Symbol mappings
- `create_b3_companies_cash_dividends()`: Dividend events
- `create_b3_companies_stock_dividends()`: Stock dividends
- `create_b3_companies_subscriptions()`: Subscription rights

**Funds**:
- `create_b3_listed_funds()`: ETFs and FIIs
- `create_etf_returns_before_20180101()`: Historical ETF returns

**Economic Data**:
- `create_bcb_data()`: CDI, SELIC, IPCA, IGPM from BCB
- `create_bcb_currency_data()`: Exchange rates via PTAX

**Utilities**:
- `copy_dataset_and_drop_duplicates()`: Dataset deduplication
- `concat_datasets()`: Merge multiple datasets
- `rename_symbols_in_equities_returns()`: Symbol renaming
- `execute_query()`: SQL-based transformations

### 8. Query Layer

**Location**: `brasa/queries.py`

**BrasaDB Class**: Connection manager for DuckDB

**Methods**:
- `get_connection()`: Get/create DuckDB connection
- `create_view(template)`: Create view from parquet
- `create_views()`: Create all views

**Query Functions**:

```python
# Returns time series data
get_returns(symbols, start, end, calendar) -> pd.DataFrame

# Get price data
get_prices(symbols, start, end, calendar) -> pd.DataFrame

# Access parquet datasets
get_dataset(dataset_name) -> pyarrow.dataset.Dataset

# Write processed data
write_dataset(df, template_id, schema=None) -> None

# Get symbol lists
get_symbols(type, **kwargs) -> list
# Types: 'etf', 'fii', 'index', 'company', 'equity', etc.

# Get industry classification
get_industry_sectors() -> pd.DataFrame

# Dataset inspection
describe(dataset_name) -> None
show(dataset_name, n=10) -> pd.DataFrame
```

### 9. Utility Layer

**Location**: `brasa/util.py`

**Key Classes**:

1. **DateRange**: Business day date range generator
   ```python
   # By year
   DateRange(year=2024, calendar="B3")
   
   # By date range
   DateRange(start=datetime(2024,1,1), end=datetime.today(), calendar="B3")
   
   # By month
   DateRange(year=2024, month=1, calendar="B3")
   ```

2. **DateRangeParser**: Parse date range strings
   ```python
   parser = DateRangeParser("B3")
   parser.parse("2024")           # Whole year
   parser.parse("2024-01")        # Single month
   parser.parse("2024:")          # From 2024 to today
   parser.parse("2024-01-01:")    # From date to today
   parser.parse("2024:2025")      # Year range
   ```

3. **KwargsIterator**: Iterate over parameter combinations
   ```python
   kwargs = {"symbol": ["PETR4", "VALE3"], "market": 10}
   for args in KwargsIterator(kwargs):
       # Generates: {"symbol": "PETR4", "market": 10}
       #            {"symbol": "VALE3", "market": 10}
   ```

**Utility Functions**:
- `generate_checksum_for_template()`: Create cache key
- `generate_checksum_from_file()`: File content hash
- `unzip_file_to()`: Extract ZIP files
- `unzip_recursive()`: Recursive ZIP extraction
- `unzip_and_get_content()`: Extract and read content

## Data Flow

### Download → Process Flow

```
1. User Request
   ↓
   download_marketdata("template-id", refdate=dates)
   
2. Template Loading
   ↓
   retrieve_template() → MarketDataTemplate
   
3. Parameter Iteration
   ↓
   KwargsIterator expands parameters
   
4. For each parameter set:
   a. Create CacheMetadata
   b. Check if already cached (has_meta)
   c. If not cached or reprocess=True:
      - MarketDataDownloader.download(**args)
      - Save files to .brasa-cache/raw/{template}/{checksum}/
      - Update metadata in SQLite
      
5. Process Request
   ↓
   process_marketdata("template-id", reprocess=False)
   
6. For each cached download:
   a. Load metadata from SQLite
   b. MarketDataReader.read(meta)
   c. Parse files to DataFrame(s)
   d. Apply field handlers (date parsing, numeric conversion)
   e. Write to .brasa-cache/db/{template}/*.parquet
   f. Update processed_files in metadata
```

### ETL Flow

```
1. User Request
   ↓
   process_etl("template-id")
   
2. Template Loading
   ↓
   retrieve_template() → MarketDataTemplate with etl config
   
3. ETL Execution
   ↓
   a. Load source dataset(s) via get_dataset()
   b. Execute transformation function
   c. Write results via write_dataset()
   
4. Optional View Creation
   ↓
   BrasaDB.create_view("template-id")
```

### Query Flow

```
1. User Query
   ↓
   df = get_returns(["PETR4", "VALE3"], start, end)
   
2. Dataset Access
   ↓
   ds = get_dataset("b3-equities-returns")
   
3. PyArrow Filtering
   ↓
   ds.filter(pc.field("symbol").isin(["PETR4", "VALE3"]))
     .filter(pc.field("refdate") >= start)
     .filter(pc.field("refdate") <= end)
   
4. Conversion
   ↓
   table = ds.to_table()
   df = table.to_pandas()
   
5. Post-processing
   ↓
   Pivot, join with calendar, fill missing dates
```

## Configuration

### Environment Variables

- **BRASA_DATA_PATH**: Override default cache location
  - Default: `.brasa-cache` in current working directory
  - Example: `export BRASA_DATA_PATH=/data/brasa`

### Calendars (via bizdays)

- **B3**: Brazilian stock exchange calendar
- **ANBIMA**: Brazilian banking calendar
- **actual**: Actual calendar (all days)

## Error Handling

### Download Errors
- `DownloadException`: Raised when download fails
- Files validated via `validate_empty_file()` or `validate_json_empty_file()`
- Errors stored in `CacheMetadata.processing_errors`

### Processing Errors
- Exceptions caught and logged
- Metadata marked with error message
- Can reprocess with `reprocess=True`

### Cache Integrity
- Checksums verify file integrity
- Metadata tracks processing state
- Can remove corrupted cache with `CacheManager.remove_meta()`

## Performance Considerations

### Caching Strategy
- Download once, process many times
- Metadata prevents redundant downloads
- Parquet format enables fast queries

### Parallel Processing
- KwargsIterator enables batch processing
- Progress bars track long operations
- Background downloads supported

### Storage Optimization
- Parquet columnar format
- Partitioning by date for pruning
- DuckDB zero-copy access to Parquet

## Extensibility

### Adding New Data Sources

1. Create YAML template in `templates/`
2. Implement downloader function if needed
3. Implement parser function if needed
4. Optional: Add ETL function for transformations

### Custom Downloaders

Implement function signature:
```python
def custom_download(
    md_downloader: MarketDataDownloader, 
    **kwargs
) -> tuple[IO | None, dict[str, str]]:
    # Return (file_object, response_headers)
```

### Custom Parsers

Implement function signature:
```python
def custom_reader(
    meta: CacheMetadata
) -> pd.DataFrame | dict[str, pd.DataFrame]:
    # Return parsed data
```

### Custom ETL

Implement function signature:
```python
def custom_etl(handler: MarketDataETL) -> None:
    # Load source data
    # Transform
    # Write via write_dataset()
```

# Brasa Documentation Index

Welcome to the Brasa project documentation. This index provides a comprehensive guide to understanding and using the Brasa library for Brazilian financial market data.

## Documentation Overview

### 1. [Project Overview](PROJECT_OVERVIEW.md)
**Start here** to understand what Brasa is and what it does.

**Contents**:
- Project purpose and scope
- Key features and capabilities
- Core technologies and dependencies
- Supported data sources (B3, ANBIMA, BCB, CVM)
- Data types available (equities, futures, options, bonds, indexes)
- Use cases and applications

**Audience**: Everyone - developers, analysts, researchers

---

### 2. [User Guide](USER_GUIDE.md)
**Practical guide** for using Brasa in your projects.

**Contents**:
- Getting started and installation
- Basic workflow (download → process → query)
- Common use cases with examples:
  - Equity analysis
  - Interest rate curves
  - Portfolio analysis
  - Dividend tracking
  - Index composition
  - Volatility surfaces
- Working with templates
- Performance optimization tips
- Troubleshooting common issues
- Advanced usage patterns
- Best practices

**Audience**: Data analysts, quantitative researchers, developers

---

### 3. [API Reference](API_REFERENCE.md)
**Complete API documentation** for all public functions and classes.

**Contents**:
- Main API functions:
  - `download_marketdata()`
  - `process_marketdata()`
  - `process_etl()`
  - `get_marketdata()`
- Query API:
  - `get_dataset()`
  - `write_dataset()`
  - `get_returns()`
  - `get_prices()`
  - `get_symbols()`
  - `get_industry_sectors()`
- DuckDB integration (BrasaDB)
- Utility classes (DateRange, KwargsIterator)
- Cache management
- CLI interface
- Template IDs reference

**Audience**: Developers, power users

---

### 4. [Architecture](ARCHITECTURE.md)
**Deep dive** into Brasa's internal architecture and design.

**Contents**:
- System architecture overview
- Core components in detail:
  - CacheManager (singleton pattern)
  - MarketDataTemplate
  - CacheMetadata
  - Downloader layer
  - Parser layer
  - Reader layer
  - ETL layer
  - Query layer
  - Utility layer
- Data flow diagrams:
  - Download → Process flow
  - ETL flow
  - Query flow
- Storage structure and organization
- Error handling strategies
- Performance considerations
- Extensibility points

**Audience**: Developers, contributors, system architects

---

### 5. [Configuration Guide](CONFIGURATION.md)
**Complete reference** for configuring Brasa.

**Contents**:
- Environment variables (BRASA_DATA_PATH)
- Template configuration:
  - YAML structure
  - Downloader options
  - Reader configuration
  - Writer settings
  - Field handlers
  - Multi-part files
  - ETL templates
- Template examples
- Calendar configuration (B3, ANBIMA, actual)
- DuckDB settings
- PyArrow schema definitions
- Performance tuning
- Logging configuration
- Custom configuration patterns
- Best practices

**Audience**: Developers, system administrators

---

## Quick Start

### For First-Time Users

1. Read [Project Overview](PROJECT_OVERVIEW.md) to understand what Brasa does
2. Follow [User Guide - Getting Started](USER_GUIDE.md#getting-started) to install and setup
3. Try the [basic workflow example](USER_GUIDE.md#basic-workflow)
4. Explore [common use cases](USER_GUIDE.md#common-use-cases) relevant to your needs

### For Developers

1. Review [Architecture](ARCHITECTURE.md) to understand system design
2. Consult [API Reference](API_REFERENCE.md) for function signatures
3. Check [Configuration Guide](CONFIGURATION.md) for template creation
4. Follow [best practices](USER_GUIDE.md#best-practices)

### For Analysts/Researchers

1. Start with [User Guide](USER_GUIDE.md)
2. Review [use cases](USER_GUIDE.md#common-use-cases) similar to your work
3. Refer to [API Reference](API_REFERENCE.md) for query functions
4. Check [troubleshooting](USER_GUIDE.md#troubleshooting) when needed

---

## Key Concepts

### Templates
YAML configuration files that define:
- How to download data (downloader)
- How to parse files (reader)
- How to store data (writer)
- How to transform data (ETL)

See: [Configuration Guide - Template Configuration](CONFIGURATION.md#template-configuration)

### Cache System
Intelligent caching prevents redundant downloads:
- Downloaded files stored in `raw/`
- Processed data in `db/` as parquet
- Metadata tracked in SQLite
- Checksums ensure uniqueness

See: [Architecture - CacheManager](ARCHITECTURE.md#1-cachemanager-singleton)

### ETL Pipelines
Transform raw data into analytical datasets:
- Interest rate curves
- Adjusted prices
- Returns calculation
- Corporate actions processing

See: [Architecture - ETL Layer](ARCHITECTURE.md#7-etl-layer)

### Query Layer
Multiple ways to access data:
- PyArrow datasets (efficient filtering)
- Helper functions (returns, prices, symbols)
- DuckDB SQL queries (complex analytics)

See: [API Reference - Query API](API_REFERENCE.md#query-api)

---

## Data Sources Covered

### B3 (Brazilian Stock Exchange)
- **Equities**: COTAHIST, real-time quotes, corporate actions
- **Futures**: Settlement prices, interest rate curves (DI1, DAP)
- **Options**: Volatility surfaces, options chains
- **Indexes**: Compositions, theoretical portfolios
- **Funds**: ETFs, REITs (FIIs)
- **OTC**: Trade information
- **Lending**: Lending trades and positions

### ANBIMA
- **Fixed Income**: Treasury bonds (TPF), debentures
- **Curves**: Interest rate term structures
- **Indices**: Fixed income indices

### BCB (Central Bank of Brazil)
- **Economic Indicators**: CDI, SELIC, IPCA, IGPM
- **Exchange Rates**: PTAX, multiple currencies
- **SGS System**: Time series data

### CVM
- **Company Data**: Registration, financial statements
- **Corporate Events**: Announcements, filings

---

## Common Workflows

### Equity Research Workflow
```
1. Download COTAHIST → b3-cotahist-daily
2. Process to parquet → process_marketdata
3. Create returns → process_etl("b3-equities-returns")
4. Query data → get_returns(symbols)
5. Analyze → pandas/numpy operations
```

### Fixed Income Workflow
```
1. Consolidate futures → process_etl("b3-futures-settlement-prices-consolidated")
2. Consolidate DI1 → process_etl("b3-futures-di1-consolidated")
3. Analyze → get_dataset("b3-futures-di1-consolidated")
```

> **Note**: `b3-futures-settlement-prices` is frozen (no longer updated; historical
> data only) and the legacy `b3-futures-*` ETL chain was moved to `templates/legacy/`.
> A refactor sourcing futures from `b3-bvbg028`/`b3-bvbg086` is planned.

### Portfolio Analysis Workflow
```
1. Get symbols → get_symbols("equity", index="IBOV")
2. Get returns → get_returns(symbols, start, end)
3. Get prices → get_prices(symbols, start, end)
4. Get fundamentals → query company data
5. Analyze → calculate metrics, optimize portfolio
```

---

## Example Code Snippets

### Download and Process
```python
from datetime import datetime
from brasa import download_marketdata, process_marketdata
from brasa.util import DateRange

period = DateRange(year=2024, calendar="B3")
download_marketdata("b3-cotahist-daily", refdate=period)
process_marketdata("b3-cotahist-daily")
```

### Query Returns
```python
from brasa.queries import get_returns
from datetime import datetime

returns = get_returns(
    ["PETR4", "VALE3"],
    start=datetime(2024, 1, 1)
)
```

### DuckDB Query
```python
from brasa.queries import BrasaDB

con = BrasaDB.get_connection()
result = con.execute("""
    SELECT symbol, AVG(close) as avg_price
    FROM 'b3-cotahist-daily'
    WHERE refdate >= '2024-01-01'
    GROUP BY symbol
    ORDER BY avg_price DESC
    LIMIT 10
""").df()
```

---

## Template Reference

### Most Used Templates

**Equity Data**:
- `b3-cotahist-daily`: Daily historical prices
- `b3-equities-returns`: Equity returns (ETL)
- `b3-equities-adjusted-prices`: Adjusted prices (ETL)

**Futures**:
- `b3-futures-settlement-prices`: Futures settlement (frozen — historical data only)
- `b3-futures-di1-consolidated`: DI1 futures consolidated (ETL)
- `b3-futures-dap`: DAP inflation futures (ETL)

**Indexes**:
- `b3-indexes-composition`: Index compositions
- `b3-indexes-theoretical-portfolio`: Theoretical weights
- `b3-indexes-returns`: Index returns (ETL)

**Company Data**:
- `brasa-companies`: BOLSA-registered companies (consolidated CVM + B3, ETL)
- `brasa-industry-sectors`: Sector taxonomy lookup table — maps B3 `sector`/`subsector` to GICS, ICB, and normalized English sector/subsector names (ETL, source: `staging.brasa-companies`)
- `b3-company-info`: Company information
- `b3-cash-dividends`: Dividend events
- `b3-companies-properties`: Company metadata (ETL)

See: [API Reference - Template IDs](API_REFERENCE.md#template-ids-reference)

---

## Troubleshooting

### Quick Fixes

**Download fails**: Check date is business day, verify internet connection
**Processing errors**: Check metadata for error messages
**Missing data**: Verify download completed, check processed files exist
**Memory issues**: Use PyArrow filtering, select only needed columns
**Slow queries**: Add date filters, use partition pruning

See: [User Guide - Troubleshooting](USER_GUIDE.md#troubleshooting)

---

## Contributing

To extend Brasa:

1. **Add new data source**:
   - Create YAML template
   - Implement downloader function (if needed)
   - Implement parser function (if needed)
   - Test with sample data

2. **Add ETL transformation**:
   - Create template with ETL section
   - Implement transformation function in `etl.py`
   - Define output schema
   - Test with existing data

3. **Improve documentation**:
   - Add examples to User Guide
   - Document new templates in Configuration
   - Update API Reference

See: [Architecture - Extensibility](ARCHITECTURE.md#extensibility)

---

## Additional Resources

### Project Files
- `brasa/__init__.py`: Main API exports
- `brasa/engine.py`: Core engine (873 lines)
- `brasa/etl.py`: ETL transformations (988 lines)
- `brasa/queries.py`: Query layer (329 lines)
- `brasa/util.py`: Utilities (205 lines)
- `templates/*.yaml`: 100+ template definitions

### Example Scripts
- `cli.py`: Download current data
- `cli-full.py`: Full data pipeline
- `cli-companies.py`: Company data focus

### Notebooks
Located in `notebooks/` directory:
- Equity analysis examples
- Futures curve analysis
- Portfolio optimization
- Data exploration

### External Documentation
- [B3 Data Documentation](https://www.b3.com.br/)
- [ANBIMA Data Services](https://www.anbima.com.br/)
- [BCB Statistics](https://www.bcb.gov.br/)
- [DuckDB Documentation](https://duckdb.org/)
- [PyArrow Documentation](https://arrow.apache.org/docs/python/)

---

## Version Information

**Current Version**: 0.0.1

**Python Compatibility**: 3.10+

**Last Updated**: 2025-01-19

---

## Getting Help

1. **Check documentation**: Start with relevant section above
2. **Review examples**: Look at notebooks and example scripts
3. **Inspect templates**: See how existing templates work
4. **Check code**: Source code is well-documented
5. **Debug**: Enable logging to see what's happening

---

## License

MIT License - See LICENSE file for details

---

## Document Navigation

- **[← Back to README](../README.md)**
- **[Project Overview →](PROJECT_OVERVIEW.md)**
- **[User Guide →](USER_GUIDE.md)**
- **[API Reference →](API_REFERENCE.md)**
- **[Architecture →](ARCHITECTURE.md)**
- **[Configuration →](CONFIGURATION.md)**

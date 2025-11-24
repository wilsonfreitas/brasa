# Brasa Project Overview

## Purpose

**Brasa** is a Python library designed to extract, process, and manage financial market data from Brazilian financial institutions including:
- **B3** (Brasil, Bolsa, Balcão) - Brazilian Stock Exchange
- **ANBIMA** (Associação Brasileira das Entidades dos Mercados Financeiro e de Capitais)
- **Tesouro Direto** - Brazilian Treasury
- **CVM** (Comissão de Valores Mobiliários) - Brazilian Securities Commission
- **BCB** (Banco Central do Brasil) - Central Bank of Brazil

The library provides a structured framework for downloading raw data, parsing it into usable formats, transforming it through ETL processes, and querying the processed data.

## Project Metadata

- **Name**: brasa
- **Version**: 0.0.1
- **License**: MIT
- **Author**: Wilson Freitas (wilson.freitas@gmail.com)
- **Python Version**: ^3.10
- **Repository**: wilsonfreitas/brasa
- **Branch**: release/stable-local

## Core Features

1. **Data Download**: Automated downloading of market data from various Brazilian financial institutions
2. **Data Parsing**: Parsing of fixed-width files, CSV, JSON, HTML, and other formats
3. **Data Processing**: ETL (Extract, Transform, Load) operations to clean and structure data
4. **Data Storage**: Parquet-based storage with partitioning support
5. **Data Querying**: DuckDB-based querying interface for efficient data analysis
6. **Caching System**: Intelligent caching to avoid redundant downloads
7. **Template-based Configuration**: YAML templates define how each data source is handled

## Key Technologies

### Core Dependencies
- **pandas** (^2.0.0): Data manipulation and analysis
- **numpy** (^2.0.0): Numerical computing
- **pyarrow** (^19.0.0): Apache Arrow for efficient data storage
- **duckdb** (^1.2.0): In-process analytical database
- **lxml** (^4.9.2): XML/HTML parsing
- **bizdays** (^1.0.15): Business days calendar operations
- **pyyaml** (^6.0): YAML configuration files
- **progressbar2** (^4.3.2): Progress tracking
- **python-bcb** (^0.3.2): Central Bank of Brazil data access

### Dev Dependencies
- **pytest** (^7.1.3): Testing framework
- **mypy** (^1.9.0): Static type checking
- **Sphinx** (^5.1.1): Documentation generation

## Architecture Summary

The project follows a layered architecture:

1. **Template Layer**: YAML configuration files defining data sources
2. **Download Layer**: Handles HTTP requests and file downloads
3. **Parser Layer**: Converts raw data into structured formats
4. **Storage Layer**: Manages caching and persistent storage
5. **Query Layer**: Provides data access interfaces
6. **ETL Layer**: Transforms and enriches data

## Use Cases

- **Quantitative Analysis**: Historical price data for equities, futures, options
- **Risk Management**: Volatility surfaces, interest rate curves
- **Portfolio Management**: Index compositions, corporate actions
- **Research**: Company fundamentals, economic indicators
- **Backtesting**: Trading strategies using historical data

## Data Types Supported

- **Equities**: Historical prices (COTAHIST), real-time quotes, corporate actions
- **Futures**: Settlement prices, interest rate curves (DI1, DAP), commodity futures (DOL, WDO, WIN)
- **Options**: Volatility surfaces, options chains
- **Fixed Income**: Government bonds, corporate bonds, interest rate swaps
- **Indexes**: Index compositions, theoretical portfolios, returns
- **ETFs & FIIs**: Listed funds, NAV data
- **Economic Indicators**: CDI, SELIC, IPCA, IGPM, exchange rates
- **Corporate Data**: Company information, dividends, subscriptions

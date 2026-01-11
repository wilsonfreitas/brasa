"""Engine module for market data operations.

This package provides the core functionality for downloading, processing,
and caching market data from various sources. It follows a modular architecture
with high cohesion and low coupling:

- core: Base utilities and patterns (Singleton, JSON helpers)
- parsers: Field parsing and type conversion
- template: Template loading and configuration
- cache: Cache management and metadata persistence
- download: Download operations
- processing: Data transformation and storage
- api: Public API functions
- exceptions: Custom exception classes

Public API:
    get_marketdata: Retrieve market data with caching
    download_marketdata: Batch download market data
    process_marketdata: Process downloaded data to parquet
    process_etl: Run ETL processes
    retrieve_template: Load a template by name
"""

# Public API functions
from .api import (
    download_marketdata,
    get_marketdata,
    process_etl,
    process_marketdata,
)

# Cache classes
from .cache import CacheManager, CacheMetadata

# Core utilities
from .core import (
    Singleton,
    json_convert_from_object,
    json_convert_to_object,
    load_function_by_name,
)

# Download internal function (for backwards compatibility)
from .download import _download_marketdata

# Exceptions
from .exceptions import DownloadException, DuplicatedFolderException

# Layers
from .layers import DEFAULT_ETL_LAYER, DEFAULT_LAYER, LAYER_ORDER, DataLayer

# Parsers
from .parsers import (
    CharacterFieldHandler,
    DateFieldHandler,
    FieldHandler,
    FieldHandlerFactory,
    NumericFieldHandler,
    NumericParser,
    PtBRNumericParser,
    TemplateField,
    TemplateFields,
)

# Processing functions (including internal for backwards compatibility)
from .processing import (
    _read_marketdata,
    get_fname_part,
    save_parquet_file,
    save_partitioned_parquet_file,
)

# Template classes and functions
from .template import (
    MarketDataDownloader,
    MarketDataETL,
    MarketDataReader,
    MarketDataTemplate,
    MarketDataWriter,
    TemplatePart,
    retrieve_template,
)

__all__ = [
    "DEFAULT_ETL_LAYER",
    "DEFAULT_LAYER",
    "LAYER_ORDER",
    "CacheManager",
    "CacheMetadata",
    "CharacterFieldHandler",
    "DataLayer",
    "DateFieldHandler",
    "DownloadException",
    "DuplicatedFolderException",
    "FieldHandler",
    "FieldHandlerFactory",
    "MarketDataDownloader",
    "MarketDataETL",
    "MarketDataReader",
    "MarketDataTemplate",
    "MarketDataWriter",
    "NumericFieldHandler",
    "NumericParser",
    "PtBRNumericParser",
    "Singleton",
    "TemplateField",
    "TemplateFields",
    "TemplatePart",
    "_download_marketdata",
    "_read_marketdata",
    "download_marketdata",
    "get_fname_part",
    "get_marketdata",
    "json_convert_from_object",
    "json_convert_to_object",
    "load_function_by_name",
    "process_etl",
    "process_marketdata",
    "retrieve_template",
    "save_parquet_file",
    "save_partitioned_parquet_file",
]

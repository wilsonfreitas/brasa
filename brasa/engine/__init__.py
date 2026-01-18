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

# Catalog classes
from .catalog import (
    DatasetCatalog,
    DatasetInfo,
    MigrationReport,
    sync_catalog_from_disk,
)

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

# Reporting classes
from .reporting import (
    ProgressDisplay,
    TaskReport,
    TaskResult,
    TaskStatus,
    Verbosity,
    capture_warnings,
    create_task_result_from_exception,
    create_task_result_skipped,
    create_task_result_success,
)

# Template classes and functions
from .template import (
    MarketDataDownloader,
    MarketDataETL,
    MarketDataReader,
    MarketDataTemplate,
    MarketDataWriter,
    TemplatePart,
    clear_template_cache,
    list_templates,
    reload_template,
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
    "DatasetCatalog",
    "DatasetInfo",
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
    "MigrationReport",
    "NumericFieldHandler",
    "NumericParser",
    "ProgressDisplay",
    "PtBRNumericParser",
    "Singleton",
    "TaskReport",
    "TaskResult",
    "TaskStatus",
    "TemplateField",
    "TemplateFields",
    "TemplatePart",
    "Verbosity",
    "_download_marketdata",
    "_read_marketdata",
    "capture_warnings",
    "clear_template_cache",
    "create_task_result_from_exception",
    "create_task_result_skipped",
    "create_task_result_success",
    "download_marketdata",
    "get_fname_part",
    "get_marketdata",
    "json_convert_from_object",
    "json_convert_to_object",
    "list_templates",
    "load_function_by_name",
    "process_etl",
    "process_marketdata",
    "reload_template",
    "retrieve_template",
    "save_parquet_file",
    "save_partitioned_parquet_file",
    "sync_catalog_from_disk",
]

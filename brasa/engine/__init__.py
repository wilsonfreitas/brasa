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
# Migrations
from .api import (
    download_marketdata,
    get_dependency_graph,
    get_execution_plan,
    get_marketdata,
    import_marketdata,
    process_etl,
    process_marketdata,
)

# Cache classes
from .cache import CacheManager, CacheMetadata, DownloadResult

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

# Dependency graph
from .dependency_graph import (
    CyclicDependencyError,
    DatasetOutput,
    ExecutionPlan,
    ExecutionStep,
    TemplateDependencyGraph,
)

# Download internal function (for backwards compatibility)
from .download import _download_marketdata

# Download plan
from .download_plan import (
    DownloadPlan,
    DownloadPlanDefaults,
    DownloadPlanReport,
    DownloadPlanTask,
    execute_download_plan,
    resolve_plan_args,
)

# Exceptions
from .exceptions import (
    CorruptedContentException,
    DownloadException,
    DuplicatedFolderException,
    InvalidContentException,
)

# Layers
from .layers import DEFAULT_ETL_LAYER, DEFAULT_LAYER, LAYER_ORDER, DataLayer

# Orchestrator
from .orchestrator import OrchestratorReport, PipelineOrchestrator, RunAllReport

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

# Pipeline map
from .pipeline_map import TemplateStatus, build_pipeline_map

# Processing functions (including internal for backwards compatibility)
from .processing import (
    _read_marketdata,
    get_fname_part,
    save_parquet_file,
    save_partitioned_parquet_file,
)

# Reporting classes
from .reporting import (
    DownloadAttemptStatus,
    ProgressDisplay,
    TaskReport,
    TaskResult,
    TaskStatus,
    Verbosity,
    capture_warnings,
    create_task_result_from_exception,
    create_task_result_skipped,
    create_task_result_success,
    map_exception_to_download_status,
    to_task_status,
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
    "CorruptedContentException",
    "CyclicDependencyError",
    "DataLayer",
    "DatasetCatalog",
    "DatasetInfo",
    "DatasetOutput",
    "DateFieldHandler",
    "DownloadAttemptStatus",
    "DownloadException",
    "DownloadPlan",
    "DownloadPlanDefaults",
    "DownloadPlanReport",
    "DownloadPlanTask",
    "DownloadResult",
    "DuplicatedFolderException",
    "ExecutionPlan",
    "ExecutionStep",
    "FieldHandler",
    "FieldHandlerFactory",
    "InvalidContentException",
    "MarketDataDownloader",
    "MarketDataETL",
    "MarketDataReader",
    "MarketDataTemplate",
    "MarketDataWriter",
    "MigrationReport",
    "NumericFieldHandler",
    "NumericParser",
    "OrchestratorReport",
    "PipelineOrchestrator",
    "ProgressDisplay",
    "PtBRNumericParser",
    "RunAllReport",
    "Singleton",
    "TaskReport",
    "TaskResult",
    "TaskStatus",
    "TemplateDependencyGraph",
    "TemplateField",
    "TemplateFields",
    "TemplatePart",
    "TemplateStatus",
    "Verbosity",
    "_download_marketdata",
    "_read_marketdata",
    "build_pipeline_map",
    "capture_warnings",
    "clear_template_cache",
    "create_task_result_from_exception",
    "create_task_result_skipped",
    "create_task_result_success",
    "download_marketdata",
    "execute_download_plan",
    "get_dependency_graph",
    "get_execution_plan",
    "get_fname_part",
    "get_marketdata",
    "import_marketdata",
    "json_convert_from_object",
    "json_convert_to_object",
    "list_templates",
    "load_function_by_name",
    "map_exception_to_download_status",
    "process_etl",
    "process_marketdata",
    "reload_template",
    "resolve_plan_args",
    "retrieve_template",
    "save_parquet_file",
    "save_partitioned_parquet_file",
    "sync_catalog_from_disk",
    "to_task_status",
]

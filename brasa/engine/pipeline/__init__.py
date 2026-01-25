"""Pipeline-based reader system for market data processing.

This module provides a composable, chain-based approach to reading and
transforming market data files. Instead of monolithic reader functions,
data processing is broken down into reusable steps that can be combined
in YAML templates.

Example YAML configuration for reader pipeline:
    reader:
      pipeline:
        - step: read_html
          attrs:
            id: tblDadosAjustes
        - step: select_table
          index: 0
        - step: set_columns
          names: [col1, col2, col3]
        - step: apply_fields

Example YAML configuration for ETL pipeline:
    etl:
      pipeline:
        - step: load
          input: source-dataset
        - step: dataset_filter
          where: { commodity: "DI1" }
    writer:
      partitioning: []

Shared Transforms:
    The `shared_transforms` module provides reusable transformation functions
    that can be used by both reader and ETL pipelines:
    - filter_data, select_columns, sort_data
    - drop_columns, rename_columns
    - drop_duplicates, fill_na, to_dataframe

Unified Step Registry:
    All pipeline steps (reader and ETL) now use the unified StepRegistry.
    ETL-specific steps that work with PyArrow Datasets use the 'dataset_' prefix
    to distinguish them from DataFrame-only reader steps:
    - dataset_filter, dataset_select, dataset_sort
    - dataset_drop_columns, dataset_rename_columns
    - dataset_drop_duplicates, dataset_fill_na
"""

# Import built-in steps to register them
# Shared transforms for code reuse between pipelines
from . import (
    etl_steps,  # noqa: F401 - registers ETL steps
    shared_transforms,
    steps,  # noqa: F401 - registers reader steps
)
from .context import PipelineContext
from .context_protocol import PipelineContextProtocol

# ETL Pipeline components
from .etl_context import ETLPipelineContext
from .etl_executor import ETLPipeline
from .executor import ReaderPipeline
from .registry import StepRegistry
from .step import PipelineStep

# Backward compatibility alias for ETLStepRegistry
# New code should use StepRegistry directly
ETLStepRegistry = StepRegistry

__all__ = [
    # ETL pipeline
    "ETLPipeline",
    "ETLPipelineContext",
    "ETLStepRegistry",  # Backward compat alias for StepRegistry
    # Reader pipeline
    "PipelineContext",
    # Shared
    "PipelineContextProtocol",
    "PipelineStep",
    "ReaderPipeline",
    "StepRegistry",
    "shared_transforms",
]

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
        - step: first_table
        - step: set_columns
          names: [col1, col2, col3]
        - step: apply_fields

Example YAML configuration for ETL pipeline:
    etl:
      pipeline:
        - step: load
          input: source-dataset
        - step: filter_rows
          where: { commodity: "DI1" }
    writer:
      partitioning: []
"""

# Import built-in steps to register them
from . import steps  # noqa: F401 - registers steps
from .context import PipelineContext

# ETL Pipeline components
from .etl_context import ETLPipelineContext
from .etl_executor import ETLPipeline
from .executor import ReaderPipeline
from .registry import StepRegistry
from .step import PipelineStep
from .steps import shared_transforms

__all__ = [
    # ETL pipeline
    "ETLPipeline",
    "ETLPipelineContext",
    # Reader pipeline
    "PipelineContext",
    # Shared
    "PipelineStep",
    "ReaderPipeline",
    "StepRegistry",
    "shared_transforms",
]

"""Pipeline-based reader system for market data processing.

This module provides a composable, chain-based approach to reading and
transforming market data files. Instead of monolithic reader functions,
data processing is broken down into reusable steps that can be combined
in YAML templates.

Example YAML configuration:
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
"""

# Import built-in steps to register them
from . import steps  # noqa: F401
from .context import PipelineContext
from .executor import ReaderPipeline
from .registry import StepRegistry
from .step import PipelineStep

__all__ = [
    "PipelineContext",
    "PipelineStep",
    "ReaderPipeline",
    "StepRegistry",
]

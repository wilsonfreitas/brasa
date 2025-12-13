"""Pipeline executor for running step sequences.

This module provides the ReaderPipeline class that executes a sequence
of steps and manages the data flow between them.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pandas as pd

from .context import PipelineContext
from .registry import StepRegistry

if TYPE_CHECKING:
    from brasa.engine.cache import CacheMetadata
    from brasa.fieldset_schema import Fieldset

    from .step import PipelineStep

logger = logging.getLogger(__name__)


class ReaderPipeline:
    """Executes a sequence of pipeline steps.

    The pipeline maintains the data flow between steps, passing the output
    of each step as input to the next. The final output is returned as the
    result of the pipeline execution.

    Attributes:
        steps: List of pipeline steps to execute.
    """

    def __init__(self, steps: list[PipelineStep]) -> None:
        """Initialize the pipeline with a list of steps.

        Args:
            steps: Ordered list of steps to execute.
        """
        self.steps = steps

    @classmethod
    def from_config(cls, config: list[dict[str, Any]]) -> ReaderPipeline:
        """Create a pipeline from YAML configuration.

        Args:
            config: List of step configurations from YAML.

        Returns:
            Configured pipeline instance.

        Raises:
            ValueError: If any step configuration is invalid.
        """
        steps = []
        for step_config in config:
            if "step" not in step_config:
                raise ValueError(
                    f"Step configuration must have a 'step' key: {step_config}"
                )
            step_name = step_config["step"]
            step = StepRegistry.create(step_name, step_config)
            steps.append(step)
        return cls(steps)

    def execute(
        self,
        meta: CacheMetadata,
        reader_config: dict[str, Any],
        fields: Fieldset | None = None,
        template_id: str = "",
    ) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """Execute the pipeline and return the result.

        Creates a context and runs each step in sequence, passing the
        output of each step as input to the next.

        Args:
            meta: Cache metadata with file paths and download context.
            reader_config: Reader configuration from template.
            fields: Optional field definitions for type conversion.
            template_id: The template ID being processed.

        Returns:
            DataFrame or dictionary of DataFrames with the processed data.

        Raises:
            Exception: If any step fails during execution.
        """
        context = PipelineContext(
            meta=meta,
            reader_config=reader_config,
            fields=fields,
            template_id=template_id,
        )

        data: Any = None
        for i, step in enumerate(self.steps):
            step_name = step.name or step.__class__.__name__
            logger.debug(f"Executing step {i + 1}/{len(self.steps)}: {step_name}")
            try:
                data = step.execute(data, context)
            except Exception as e:
                logger.error(f"Step '{step_name}' failed: {e}")
                raise RuntimeError(
                    f"Pipeline failed at step {i + 1} ({step_name}): {e}"
                ) from e

        return data

    def __repr__(self) -> str:
        step_names = [s.name or s.__class__.__name__ for s in self.steps]
        return f"ReaderPipeline(steps={step_names})"

    def __len__(self) -> int:
        return len(self.steps)

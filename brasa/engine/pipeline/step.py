"""Base class for pipeline steps.

This module defines the abstract base class that all pipeline steps must inherit from.
Both reader pipelines and ETL pipelines use this base class with the unified StepRegistry.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class PipelineStep(ABC):
    """Base class for all pipeline steps.

    A pipeline step is a single unit of work that transforms data in some way.
    Steps are composable and can be chained together to form a complete
    data processing pipeline.

    This unified base class works with both:
    - Reader pipelines (PipelineContext with file metadata)
    - ETL pipelines (ETLPipelineContext with writer config)

    Steps should use duck typing or protocol checks if they need specific
    context features. Most steps only need params and work with Any context.

    Attributes:
        name: The registered name of this step type.
        params: Configuration parameters for this step instance.
    """

    name: str = ""

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        """Initialize the step with configuration parameters.

        Args:
            params: Dictionary of parameters from the YAML configuration.
        """
        self.params = params or {}

    @abstractmethod
    def execute(self, data: Any, context: Any) -> Any:
        """Execute the step and return the transformed data.

        Args:
            data: Input data from the previous step (or None for the first step).
            context: Pipeline context (PipelineContext, ETLPipelineContext, or compatible).

        Returns:
            Transformed data to pass to the next step.
        """
        ...

    def get_param(self, key: str, default: Any = None) -> Any:
        """Get a parameter value with an optional default.

        Args:
            key: Parameter name.
            default: Default value if parameter is not set.

        Returns:
            The parameter value or default.
        """
        return self.params.get(key, default)

    def require_param(self, key: str) -> Any:
        """Get a required parameter value.

        Args:
            key: Parameter name.

        Returns:
            The parameter value.

        Raises:
            ValueError: If the parameter is not set.
        """
        if key not in self.params:
            raise ValueError(f"Step '{self.name}' requires parameter '{key}'")
        return self.params[key]

    def get_input_datasets(self) -> list[str]:
        """Get the list of input dataset names this step depends on.

        Used by ETL pipelines to determine dataset dependencies.

        Returns:
            List of dataset names that are inputs to this step.
        """
        inputs = []
        if "input" in self.params:
            inputs.append(self.params["input"])
        if "right" in self.params:
            inputs.append(self.params["right"])
        return inputs

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PipelineStep:
        """Create a step instance from YAML configuration.

        The default implementation passes all config keys except 'step'
        as parameters. Subclasses can override this for custom initialization.

        Args:
            config: Dictionary from YAML configuration.

        Returns:
            Configured step instance.
        """
        # Remove 'step' key which contains the step name
        params = {k: v for k, v in config.items() if k != "step"}
        return cls(params)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(params={self.params})"

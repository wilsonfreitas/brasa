"""Base class for pipeline steps.

This module defines the abstract base class that all pipeline steps must inherit from.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .context import PipelineContext


class PipelineStep(ABC):
    """Base class for all pipeline steps.

    A pipeline step is a single unit of work that transforms data in some way.
    Steps are composable and can be chained together to form a complete
    data processing pipeline.

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
    def execute(self, data: Any, context: PipelineContext) -> Any:
        """Execute the step and return the transformed data.

        Args:
            data: Input data from the previous step (or None for the first step).
            context: Pipeline context containing metadata and configuration.

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

"""Step registry for pipeline steps.

This module provides a registry pattern for discovering and instantiating
pipeline steps by name.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from .step import PipelineStep


class StepRegistry:
    """Registry of available pipeline steps.

    Provides decorator-based registration and factory method for creating
    step instances from YAML configuration.

    Example:
        @StepRegistry.register("my_step")
        class MyStep(PipelineStep):
            def execute(self, data, context):
                return data
    """

    _steps: ClassVar[dict[str, type[PipelineStep]]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a step class with a given name.

        Args:
            name: The name to register the step under.

        Returns:
            Decorator function.

        Example:
            @StepRegistry.register("read_csv")
            class ReadCsvStep(PipelineStep):
                ...
        """

        def decorator(step_class: type[PipelineStep]):
            if name in cls._steps:
                raise ValueError(
                    f"Step '{name}' is already registered by {cls._steps[name].__name__}"
                )
            step_class.name = name
            cls._steps[name] = step_class
            return step_class

        return decorator

    @classmethod
    def create(cls, name: str, config: dict[str, Any]) -> PipelineStep:
        """Create a step instance from configuration.

        Args:
            name: Registered name of the step.
            config: Configuration dictionary from YAML.

        Returns:
            Configured step instance.

        Raises:
            ValueError: If the step name is not registered.
        """
        if name not in cls._steps:
            available = ", ".join(sorted(cls._steps.keys()))
            raise ValueError(f"Unknown step: '{name}'. Available steps: {available}")
        step_class = cls._steps[name]
        return step_class.from_config(config)

    @classmethod
    def get(cls, name: str) -> type[PipelineStep] | None:
        """Get a step class by name.

        Args:
            name: Registered name of the step.

        Returns:
            The step class or None if not found.
        """
        return cls._steps.get(name)

    @classmethod
    def list_steps(cls) -> list[str]:
        """List all registered step names.

        Returns:
            Sorted list of step names.
        """
        return sorted(cls._steps.keys())

    @classmethod
    def get_all_steps(cls) -> dict[str, type[PipelineStep]]:
        """Get all registered steps.

        Returns:
            Dictionary mapping step names to step classes.
        """
        return cls._steps.copy()

    @classmethod
    def clear(cls) -> None:
        """Clear all registered steps. Useful for testing."""
        cls._steps.clear()

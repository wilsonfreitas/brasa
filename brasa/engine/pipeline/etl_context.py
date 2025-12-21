"""ETL Pipeline context for dataset transformations.

This module defines the ETLPipelineContext class that carries metadata and
configuration through the ETL pipeline execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from brasa.engine.template import MarketDataWriter
    from brasa.fieldset_schema import Fieldset


@dataclass
class ETLPipelineContext:
    """Context object that flows through the ETL pipeline.

    Carries metadata, configuration, and intermediate results that steps
    may need to access during execution.

    Attributes:
        template_id: The ID of the template being processed.
        writer: Writer configuration from the template.
        fields: Field definitions from the template for type conversion.
        intermediate_results: Named results that steps can store for later use.
    """

    template_id: str = ""
    writer: MarketDataWriter | None = None
    fields: Fieldset | None = None
    intermediate_results: dict[str, Any] = field(default_factory=dict)

    def store_result(self, name: str, value: Any) -> None:
        """Store an intermediate result for later use.

        Args:
            name: Key to store the result under.
            value: The result to store.
        """
        self.intermediate_results[name] = value

    def get_result(self, name: str, default: Any = None) -> Any:
        """Retrieve a previously stored intermediate result.

        Args:
            name: Key of the stored result.
            default: Default value if not found.

        Returns:
            The stored result or default.
        """
        return self.intermediate_results.get(name, default)

    def get_partitioning(self) -> list[str]:
        """Get the partitioning columns from writer configuration.

        Returns:
            List of column names for partitioning, empty list if not partitioned.
        """
        if self.writer is not None:
            return getattr(self.writer, "partitioning", [])
        return []

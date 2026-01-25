"""Context protocol for pipeline steps.

This module defines the common interface that pipeline contexts must implement
to be compatible with shared pipeline steps. Both PipelineContext (reader) and
ETLPipelineContext can be used with steps that only require this protocol.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from brasa.fieldsets import Fieldset


@runtime_checkable
class PipelineContextProtocol(Protocol):
    """Common interface for pipeline contexts.

    Steps that only need these common features can work with any context
    implementing this protocol (both reader and ETL contexts).

    Attributes:
        template_id: The ID of the template being processed.
        fields: Field definitions from the template (optional).
        intermediate_results: Storage for named intermediate results.
    """

    template_id: str
    fields: Fieldset | None
    intermediate_results: dict[str, Any]

    def store_result(self, name: str, value: Any) -> None:
        """Store an intermediate result for later use."""
        ...

    def get_result(self, name: str, default: Any = None) -> Any:
        """Retrieve a previously stored intermediate result."""
        ...

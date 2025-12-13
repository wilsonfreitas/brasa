"""Pipeline context for passing data between steps.

This module defines the PipelineContext class that carries metadata and
configuration through the pipeline execution.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from brasa.engine.cache import CacheMetadata
    from brasa.fieldset_schema import Fieldset


@dataclass
class PipelineContext:
    """Context object that flows through the pipeline.

    Carries metadata, configuration, and intermediate results that steps
    may need to access during execution.

    Attributes:
        meta: Cache metadata containing file paths and download context.
        reader_config: Reader configuration from the template (encoding, decimal, etc.).
        fields: Field definitions from the template for type conversion.
        intermediate_results: Named results that steps can store for later use.
        template_id: The ID of the template being processed.
    """

    meta: CacheMetadata
    reader_config: dict[str, Any]
    fields: Fieldset | None = None
    template_id: str = ""
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

    def get_config(self, key: str, default: Any = None) -> Any:
        """Get a value from the reader configuration.

        Args:
            key: Configuration key.
            default: Default value if not found.

        Returns:
            The configuration value or default.
        """
        return self.reader_config.get(key, default)

    @property
    def encoding(self) -> str:
        """Get the file encoding from configuration."""
        return self.get_config("encoding", "utf-8")

    @property
    def decimal(self) -> str:
        """Get the decimal separator from configuration."""
        return self.get_config("decimal", ".")

    @property
    def thousands(self) -> str:
        """Get the thousands separator from configuration."""
        return self.get_config("thousands", ",")

    @property
    def downloaded_file(self) -> str:
        """Get the path to the primary downloaded file."""
        from brasa.engine.cache import CacheManager

        manager = CacheManager()
        if self.meta.downloaded_files:
            return manager.cache_path(self.meta.downloaded_files[-1])
        raise ValueError("No downloaded files available in context")

    @property
    def all_downloaded_files(self) -> list[str]:
        """Get paths to all downloaded files."""
        from brasa.engine.cache import CacheManager

        manager = CacheManager()
        return [manager.cache_path(f) for f in self.meta.downloaded_files]

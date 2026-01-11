"""Data layer definitions for market data storage.

This module defines the hierarchical data layers used to organize datasets
by their processing stage in the ETL pipeline.

Layers:
    RAW: Downloaded files (already exists in the cache structure)
    INPUT: First-level processed data - parsed market data
    STAGING: Refined/enriched data - cleaned, joined datasets
    CURATED: Analytics-ready data - final aggregated datasets
"""

from enum import Enum


class DataLayer(str, Enum):
    """Data layer enumeration for organizing datasets by processing stage.

    The layers follow a logical progression from raw downloads to
    analytics-ready datasets:

    - RAW: Original downloaded files (ZIP, XML, TXT, etc.)
    - INPUT: First-level parsed data from raw files
    - STAGING: Refined data with transformations and joins
    - CURATED: Final analytics-ready datasets

    Attributes:
        value: The string value used in folder paths and configurations.
    """

    RAW = "raw"
    INPUT = "input"
    STAGING = "staging"
    CURATED = "curated"

    @classmethod
    def from_string(cls, value: str | None) -> "DataLayer":
        """Convert a string to a DataLayer enum value.

        Args:
            value: The string representation of the layer. If None or empty,
                defaults to INPUT.

        Returns:
            The corresponding DataLayer enum value.

        Raises:
            ValueError: If the string doesn't match any valid layer.
        """
        if not value:
            return cls.INPUT

        value_lower = value.lower().strip()
        for layer in cls:
            if layer.value == value_lower:
                return layer

        valid_layers = [layer.value for layer in cls]
        raise ValueError(
            f"Invalid layer '{value}'. Valid layers are: {', '.join(valid_layers)}"
        )

    def __str__(self) -> str:
        """Return the string value for use in paths."""
        return self.value


# Layer ordering for validation and dependency checks
LAYER_ORDER: list[DataLayer] = [
    DataLayer.RAW,
    DataLayer.INPUT,
    DataLayer.STAGING,
    DataLayer.CURATED,
]

# Default layer for templates without explicit specification
DEFAULT_LAYER: DataLayer = DataLayer.INPUT

# Default layer for ETL templates without explicit specification
DEFAULT_ETL_LAYER: DataLayer = DataLayer.STAGING

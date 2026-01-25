"""
Unified CSV reader for Fieldset.

Provides a single interface to read CSV files, automatically selecting between
pandas and PyArrow engines based on file characteristics and schema requirements.
"""

import io
import warnings
from pathlib import Path
from typing import Any, Literal

import pandas as pd
import pyarrow as pa

from ..fieldset import Fieldset
from .pandas_adapter import PandasAdapter
from .pyarrow_adapter import PyArrowAdapter


class FieldsetReader:
    """
    Unified CSV reader with automatic engine selection.

    Automatically chooses between pandas and PyArrow based on file size
    and data characteristics.
    """

    # File size threshold for engine selection (in bytes)
    PYARROW_THRESHOLD_MB = 50  # Use PyArrow for files > 50MB

    def __init__(
        self,
        fieldset: Fieldset,
        errors: Literal["raise", "coerce", "ignore"] = "coerce",
        verbose_warnings: bool = True,
    ):
        """
        Initialize FieldsetReader.

        Args:
            fieldset: Fieldset instance defining the schema
            errors: Error handling strategy
            verbose_warnings: Show detailed warnings
        """
        self.fieldset = fieldset
        self.errors = errors
        self.verbose_warnings = verbose_warnings

        self.pandas_adapter = PandasAdapter(
            fieldset, errors=errors, verbose_warnings=verbose_warnings
        )

        self.pyarrow_adapter = PyArrowAdapter(
            fieldset, errors=errors, verbose_warnings=verbose_warnings
        )

    def read_csv(
        self,
        filepath_or_buffer: str | Path | io.StringIO,
        engine: Literal["pandas", "pyarrow", "auto"] | None = "auto",
        return_type: Literal["pandas", "pyarrow"] = "pandas",
        **kwargs: Any,
    ) -> pd.DataFrame | pa.Table:
        """
        Read CSV with automatic engine selection.

        Args:
            filepath_or_buffer: Path to CSV file or file-like object
            engine: Engine to use ('pandas', 'pyarrow', or 'auto')
            return_type: Return pandas DataFrame or PyArrow Table
            **kwargs: Additional arguments for read_csv

        Returns:
            DataFrame or PyArrow Table based on return_type

        Raises:
            FileNotFoundError: If the file does not exist
            ValueError: If an invalid engine or return_type is specified
        """
        # Validate return_type
        if return_type not in ("pandas", "pyarrow"):
            raise ValueError(
                f"Invalid return_type: {return_type}. Must be 'pandas' or 'pyarrow'"
            )

        # Get filepath if applicable
        filepath = self._get_filepath(filepath_or_buffer)

        # Auto-select engine if requested
        if engine == "auto":
            engine = self._select_engine(filepath)
            if self.verbose_warnings:
                print(f"FieldsetReader: Auto-selected engine: {engine}")

        # Read with selected engine and convert to desired return type
        return self._read_and_convert(filepath_or_buffer, engine, return_type, **kwargs)

    def _get_filepath(
        self, filepath_or_buffer: str | Path | io.StringIO
    ) -> Path | None:
        """Extract and validate filepath from input."""
        is_filepath = isinstance(filepath_or_buffer, str) | isinstance(
            filepath_or_buffer, Path
        )

        if is_filepath:
            filepath = Path(filepath_or_buffer)
            if not filepath.exists():
                raise FileNotFoundError(f"File not found: {filepath}")
            return filepath
        return None

    def _read_and_convert(
        self,
        filepath_or_buffer: str | Path | io.StringIO,
        engine: str,
        return_type: str,
        **kwargs: Any,
    ) -> pd.DataFrame | pa.Table:
        """Read CSV with specified engine and convert to desired format."""
        if engine == "pandas":
            df = self.pandas_adapter.read_csv(filepath_or_buffer, **kwargs)
            return (
                pa.Table.from_pandas(df, preserve_index=False)
                if return_type == "pyarrow"
                else df
            )

        elif engine == "pyarrow":
            table = self.pyarrow_adapter.read_csv(filepath_or_buffer, **kwargs)
            return table.to_pandas() if return_type == "pandas" else table

        else:
            raise ValueError(
                f"Invalid engine: {engine}. Must be 'pandas', 'pyarrow', or 'auto'"
            )

    def _select_engine(self, filepath: Path | None) -> Literal["pandas", "pyarrow"]:
        """
        Select optimal engine based on file characteristics.

        Args:
            filepath: Path to CSV file (or None if buffer)

        Returns:
            Selected engine name
        """
        file_size_mb = 0
        if filepath and filepath.exists():
            file_size_mb = filepath.stat().st_size / (1024 * 1024)

        # Check if any fields need preprocessing (pandas is better for this)
        has_custom_formats = bool(self.pyarrow_adapter._needs_pandas_preprocessing)

        # Decision logic
        if has_custom_formats:
            # Pandas is better for custom formats regardless of size
            if self.verbose_warnings and file_size_mb > self.PYARROW_THRESHOLD_MB:
                warnings.warn(
                    f"File is {file_size_mb:.1f}MB with custom formats. "
                    f"Using pandas (recommended for custom formats) for preprocessing.",
                    UserWarning,
                    stacklevel=2,
                )
            return "pandas"

        elif file_size_mb > self.PYARROW_THRESHOLD_MB:
            # Large file without custom formats - use PyArrow
            if self.verbose_warnings:
                print(
                    f"File is {file_size_mb:.1f}MB. "
                    f"Using PyArrow for better performance."
                )
            return "pyarrow"

        else:
            # Small file or buffer - pandas is fine and more flexible
            return "pandas"

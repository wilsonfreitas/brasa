"""
Pandas adapter for Fieldset.

Provides functionality to read CSV files into pandas DataFrames using a Fieldset schema.
"""

import io
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any, ClassVar, Literal

import pandas as pd

from ..exceptions import TypeParseError
from ..field import Field
from ..fieldset import Fieldset


class PandasAdapter:
    """
    Adapter to use Fieldset with pandas read_csv.

    Generates dtype mappings and converter functions based on field definitions,
    enabling automatic type conversion during CSV reading.
    """

    # Mapping from field type names to pandas dtypes (for simple types)
    SIMPLE_TYPE_MAPPING: ClassVar[dict[str, str]] = {
        "integer": "Int64",  # Nullable integer
        "boolean": "boolean",  # Nullable boolean
        "string": "string",  # String dtype (better than object)
        "character": "string",
    }

    def __init__(
        self,
        fieldset: Fieldset,
        errors: Literal["raise", "coerce", "ignore"] = "coerce",
        use_nullable_dtypes: bool = True,
        verbose_warnings: bool = True,
    ):
        """
        Initialize PandasAdapter.

        Args:
            fieldset: Fieldset instance defining the schema
            errors: Error handling strategy:
                - 'raise': Raise exception on parse error
                - 'coerce': Set invalid values to NaN/NaT with warning
                - 'ignore': Silently set invalid values to NaN/NaT
            use_nullable_dtypes: Use nullable dtypes (Int64, boolean, string)
            verbose_warnings: Show detailed warnings for parsing errors
        """
        self.fieldset = fieldset
        self.errors = errors
        self.use_nullable_dtypes = use_nullable_dtypes
        self.verbose_warnings = verbose_warnings

        self._dtype_dict: dict[str, str] = {}
        self._converters: dict[str, Callable] = {}
        self._parse_dates: list[str] = []

        self._build_mappings()

    def _build_mappings(self) -> None:
        """Build dtype and converter mappings from fieldset."""
        for field in self.fieldset.get_all_fields():
            type_name = field.type_name
            field_name = field.name

            # Check if this type needs a converter (complex parsing)
            if self._needs_converter(field):
                # Use converter for complex types
                self._converters[field_name] = self._create_converter(field)
            else:
                # Use dtype for simple types
                dtype = self._get_pandas_dtype(field)
                if dtype:
                    self._dtype_dict[field_name] = dtype

                # Track date fields for parse_dates parameter
                # Only add to parse_dates if using default format
                if type_name in (
                    "date",
                    "datetime",
                ) and not field.parser.parameters.get("format"):
                    self._parse_dates.append(field_name)

    def _needs_converter(self, field: Field) -> bool:
        """
        Check if field needs a custom converter function.

        Fields need converters if they have:
        - Custom date/datetime/time formats
        - Numeric types with decimal parameters
        - Date/datetime/time types when error handling is needed (coerce/ignore)
        - Any other custom parsing logic
        """
        type_name = field.type_name

        # Date/datetime/time with custom format always need converters
        if type_name in ("date", "datetime", "time"):
            if field.parser.parameters.get("format"):
                return True
            # Also use converters when we need error handling (not 'raise')
            # because parse_dates doesn't handle errors gracefully
            if self.errors != "raise":
                return True

        # Numeric with decimal places, sign, or custom separators needs converter
        if type_name == "numeric":
            params = field.parser.parameters
            return bool(
                params.get("dec")
                or params.get("sign")
                or params.get("thousands")
                or params.get("decimal")
            )

        return False

    def _get_pandas_dtype(self, field: Field) -> str | None:
        """
        Get pandas dtype for simple field types.

        Args:
            field: Field instance

        Returns:
            Pandas dtype string or None if converter needed
        """
        type_name = field.type_name

        # Handle simple type mapping
        if type_name in self.SIMPLE_TYPE_MAPPING:
            if self.use_nullable_dtypes:
                return self.SIMPLE_TYPE_MAPPING[type_name]
            # Fallback to standard dtypes
            fallback_dtypes = {
                "integer": "float64",  # Use float to handle NaN
                "boolean": "object",
            }
            return fallback_dtypes.get(type_name, "object")

        # Numeric without parameters maps to float
        if type_name == "numeric" and not self._needs_converter(field):
            return "float64"

        # Date/datetime without custom format handled by parse_dates
        # All other cases return None
        return None

    def _create_converter(self, field: Field) -> Callable:
        """
        Create converter function for a field.

        Args:
            field: Field instance

        Returns:
            Converter function that takes string and returns parsed value
        """

        def get_null_value() -> Any:
            """Get appropriate null value based on field type."""
            if field.type_name in ("date", "datetime", "time"):
                return pd.NaT
            return pd.NA if self.use_nullable_dtypes else None

        def converter(value: Any) -> Any:
            # Handle missing values
            if pd.isna(value) or value == "":
                return pd.NA if self.use_nullable_dtypes else None

            try:
                return field.parse(str(value))
            except TypeParseError as e:
                if self.errors == "raise":
                    raise ValueError(
                        f"Error parsing field '{field.name}' with value '{value}': {e}"
                    ) from e
                if self.errors == "coerce" and self.verbose_warnings:
                    warnings.warn(
                        f"Failed to parse field '{field.name}' with value '{value}': {e}. "
                        f"Setting to NaN/NaT.",
                        UserWarning,
                        stacklevel=2,
                    )
                return get_null_value()
            except Exception as e:
                # Catch any other unexpected errors during parsing
                if self.errors == "raise":
                    raise ValueError(
                        f"Unexpected error parsing field '{field.name}' with value '{value}': {e}"
                    ) from e
                if self.verbose_warnings:
                    warnings.warn(
                        f"Unexpected error parsing field '{field.name}' with value '{value}': {e}. "
                        f"Setting to NaN/NaT.",
                        UserWarning,
                        stacklevel=2,
                    )
                return get_null_value()

        return converter

    def get_dtype_dict(self) -> dict[str, str]:
        """
        Get dtype dictionary for pd.read_csv.

        Returns:
            Dictionary mapping column names to pandas dtype strings
        """
        return self._dtype_dict.copy()

    def get_converters(self) -> dict[str, Callable]:
        """
        Get converters dictionary for pd.read_csv.

        Returns:
            Dictionary mapping column names to converter functions
        """
        return self._converters.copy()

    def get_parse_dates(self) -> list[str]:
        """
        Get list of date columns for pd.read_csv parse_dates parameter.

        Returns:
            List of column names to parse as dates
        """
        return self._parse_dates.copy()

    def read_csv(
        self, filepath_or_buffer: str | Path | io.StringIO, **kwargs: Any
    ) -> pd.DataFrame:
        """
        Read CSV file with automatic type conversion based on fieldset.

        Args:
            filepath_or_buffer: Path to CSV file or file-like object
            **kwargs: Additional arguments passed to pd.read_csv

        Returns:
            DataFrame with properly typed columns

        Example:
            adapter = PandasAdapter(fieldset)
            df = adapter.read_csv('data.csv', sep=';', encoding='utf-8')
        """
        # Build read_csv parameters
        read_params = {
            "dtype": self.get_dtype_dict(),
            "converters": self.get_converters(),
        }

        # Add parse_dates if we have date fields with standard formats
        # But first, we need to check which columns actually exist in the CSV
        # to avoid pandas errors about missing columns
        if self._parse_dates and "parse_dates" not in kwargs:
            # Only include parse_dates if user hasn't provided their own
            # Try to peek at column names to filter parse_dates
            parse_dates_to_use = self._filter_parse_dates_by_columns(
                filepath_or_buffer, kwargs
            )
            if parse_dates_to_use:
                read_params["parse_dates"] = parse_dates_to_use

        # Merge with user-provided kwargs (user params take precedence)
        read_params.update(kwargs)

        # Show info about what we're doing
        if self.verbose_warnings:
            print("PandasAdapter: Reading CSV with Fieldset schema:")
            print(f"  - dtype mapping: {len(self._dtype_dict)} fields")
            print(f"  - converters: {len(self._converters)} fields")
            print(f"  - parse_dates: {len(read_params.get('parse_dates', []))} fields")

        return pd.read_csv(filepath_or_buffer, **read_params)

    def _filter_parse_dates_by_columns(
        self, filepath_or_buffer: str | Path | io.StringIO, kwargs: dict[str, Any]
    ) -> list[str]:
        """
        Filter parse_dates list to only include columns that exist in the CSV.

        Args:
            filepath_or_buffer: Path to CSV file or file-like object
            kwargs: User-provided kwargs for read_csv

        Returns:
            Filtered list of column names
        """
        try:
            # Read just the first row to get column names
            # Preserve the file position if it's a buffer
            if (
                isinstance(filepath_or_buffer, io.IOBase)
                and hasattr(filepath_or_buffer, "tell")
                and hasattr(filepath_or_buffer, "seek")
            ):
                original_pos = filepath_or_buffer.tell()
                columns = pd.read_csv(
                    filepath_or_buffer, nrows=0, **kwargs
                ).columns.tolist()
                filepath_or_buffer.seek(original_pos)
            else:
                # For file paths, just read the header
                columns = pd.read_csv(
                    filepath_or_buffer, nrows=0, **kwargs
                ).columns.tolist()

            # Filter parse_dates to only include existing columns
            return [col for col in self._parse_dates if col in columns]
        except Exception:
            # If we can't peek at columns, just return the full list
            # and let pandas handle any errors
            return self._parse_dates

    def validate_dataframe(self, df: pd.DataFrame) -> dict[str, dict[str, Any]]:
        """
        Validate DataFrame against fieldset schema.

        Args:
            df: DataFrame to validate

        Returns:
            Dictionary with validation results per field
        """
        results = {}

        for field in self.fieldset.get_all_fields():
            field_name = field.name

            if field_name not in df.columns:
                results[field_name] = {
                    "present": False,
                    "error": "Column missing from DataFrame",
                }
                continue

            column = df[field_name]

            # Count valid vs invalid values
            non_null = column.notna().sum()
            null_count = column.isna().sum()

            results[field_name] = {
                "present": True,
                "total_rows": len(df),
                "non_null_count": int(non_null),
                "null_count": int(null_count),
                "dtype": str(column.dtype),
                "expected_type_definition": field.type_definition,
                "expected_base_type": field.type_name,
            }

        return results

    def _convert_with_converter(
        self, df: pd.DataFrame, field_name: str, field: Field
    ) -> pd.Series:
        """Apply converter function to a column."""
        converter = self._create_converter(field)
        result = df[field_name].apply(converter)

        # For date/datetime types, ensure proper dtype after apply
        if field.type_name in (
            "date",
            "datetime",
        ) and not pd.api.types.is_datetime64_any_dtype(result):
            result = pd.to_datetime(result, errors="coerce")

        return result

    def _convert_date_type(self, series: pd.Series) -> pd.Series:
        """Convert series to datetime."""
        return pd.to_datetime(series, errors=self.errors)

    def _convert_integer_type(self, series: pd.Series) -> pd.Series:
        """Convert series to integer."""
        numeric_series = pd.to_numeric(series, errors=self.errors)
        return (
            numeric_series.astype("Int64")
            if self.use_nullable_dtypes
            else numeric_series
        )

    def _convert_numeric_type(self, series: pd.Series) -> pd.Series:
        """Convert series to numeric."""
        return pd.to_numeric(series, errors=self.errors)

    def _convert_boolean_type(self, series: pd.Series) -> pd.Series:
        """Convert series to boolean."""
        return series.astype("boolean" if self.use_nullable_dtypes else bool)

    def _convert_string_type(self, series: pd.Series) -> pd.Series:
        """Convert series to string."""
        return series.astype("string") if self.use_nullable_dtypes else series

    def apply_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply type conversions to an existing DataFrame using fieldset schema.

        This method is useful when you already have a DataFrame (e.g., from
        a parser or other source) and want to apply the type conversions
        defined in the fieldset.

        Args:
            df: DataFrame with columns to be type-converted

        Returns:
            DataFrame with converted column types

        Raises:
            TypeParseError: If errors='raise' and conversion fails

        Example:
            # Create fieldset from template
            fieldset = Fieldset.from_template_fields(template.fields)
            adapter = PandasAdapter(fieldset)

            # Parse data with custom parser, get DataFrame
            df = parser.data

            # Apply type conversions from fieldset
            df = adapter.apply_types(df)
        """
        df = df.copy()

        # Mapping of type names to conversion methods
        type_converters = {
            "date": self._convert_date_type,
            "datetime": self._convert_date_type,
            "integer": self._convert_integer_type,
            "numeric": self._convert_numeric_type,
            "boolean": self._convert_boolean_type,
            "string": self._convert_string_type,
            "character": self._convert_string_type,
        }

        for field in self.fieldset.get_all_fields():
            field_name = field.name

            # Skip if column doesn't exist in DataFrame
            if field_name not in df.columns:
                continue

            # Apply the appropriate conversion
            try:
                if self._needs_converter(field):
                    df[field_name] = self._convert_with_converter(df, field_name, field)
                else:
                    converter = type_converters.get(field.type_name)
                    if converter:
                        df[field_name] = converter(df[field_name])

            except Exception as e:
                if self.errors == "raise":
                    raise TypeParseError(
                        f"Failed to convert field '{field_name}' to type '{field.type_definition}': {e}"
                    ) from e
                elif self.verbose_warnings:
                    warnings.warn(
                        f"Error converting field '{field_name}' to type '{field.type_definition}': {e}",
                        UserWarning,
                        stacklevel=2,
                    )

        return df

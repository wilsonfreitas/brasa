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
        """
        Build dtype and converter mappings from fieldset.

        For ``read_csv``:
        - Fields that require per-cell parsing (date, time, parameterised
          numeric) get a converter callback.
        - Simple fields (integer, boolean, string, plain numeric) get a
          dtype hint so that ``pd.read_csv`` handles them natively.

        Note: ``_parse_dates`` is intentionally unused; all date/datetime
        columns are handled by converter callbacks in ``read_csv`` and by
        vectorized ``pd.to_datetime`` in ``apply_types``.
        """
        for field in self.fieldset.get_all_fields():
            field_name = field.name

            if self._needs_converter(field):
                self._converters[field_name] = self._create_converter(field)
            else:
                dtype = self._get_pandas_dtype(field)
                if dtype:
                    self._dtype_dict[field_name] = dtype

    def _needs_converter(self, field: Field) -> bool:
        """
        Decide whether ``pd.read_csv`` requires a per-cell converter callback.

        This method is intentionally scoped to ``read_csv`` only.  ``apply_types``
        uses a separate, vectorized decision path (see ``_can_vectorize_date`` and
        ``_can_vectorize_numeric``).

        Decision matrix for ``read_csv`` converters
        -------------------------------------------
        type          | condition                              | needs converter
        --------------|----------------------------------------|----------------
        date/datetime | any (format or no format, any errors)  | True
        time          | any                                    | True
        numeric       | dec, sign, thousands, or decimal param | True
        numeric       | no custom params                       | False (dtype=float64)
        integer       | -                                      | False (dtype=Int64)
        boolean       | -                                      | False (dtype=boolean)
        string/char   | -                                      | False (dtype=string)

        Notes
        -----
        Date/datetime remain as converter-only in ``read_csv`` because
        ``pd.read_csv`` ``parse_dates`` does not support per-column format strings
        reliably across pandas versions, and error coercion behaviour is
        inconsistent.  ``apply_types`` handles these with vectorized
        ``pd.to_datetime(format=...)``.
        """
        type_name = field.type_name

        # All date/datetime/time fields use converter in read_csv for
        # reliable format and error handling across pandas versions.
        if type_name in ("date", "datetime", "time"):
            return True

        # Numeric with any custom parameter cannot be mapped to a simple dtype.
        if type_name == "numeric":
            params = field.parser.parameters
            return bool(
                params.get("dec")
                or params.get("sign")
                or params.get("thousands")
                or params.get("decimal")
            )

        return False

    # ------------------------------------------------------------------
    # Vectorized-eligibility helpers (Phase 1 / TASK-006)
    # Used exclusively by apply_types to decide the conversion path.
    # ------------------------------------------------------------------

    def _can_vectorize_date(self, field: Field) -> bool:
        """
        Return True when the date/datetime field can be converted with
        ``pd.to_datetime`` (i.e. the CAND-004 vectorized path is safe).

        Currently *all* date/datetime fields are vectorizable because
        ``pd.to_datetime`` accepts an optional ``format`` parameter and
        supports all ``errors`` modes.

        Args:
            field: Field instance with type_name in ('date', 'datetime').

        Returns:
            True (always, for date/datetime).
        """
        return field.type_name in ("date", "datetime")

    def _can_vectorize_numeric(self, field: Field) -> bool:
        """
        Return True when the numeric field can be converted with
        vectorized string preprocessing + ``pd.to_numeric``.

        All numeric parameter combinations supported by ``NumericParser``
        (``dec``, ``sign``, ``thousands``, ``decimal``) can be reproduced
        with ``Series.str.replace`` followed by ``pd.to_numeric``.

        Args:
            field: Field instance with type_name 'numeric'.

        Returns:
            True (always, for numeric).
        """
        return field.type_name == "numeric"

    def _normalize_numeric_series(self, series: pd.Series, field: Field) -> pd.Series:
        """
        Apply vectorized string preprocessing to match ``NumericParser`` semantics.

        Preprocessing steps (mirrors ``NumericParser.parse``):

        1. Strip whitespace.
        2. Remove thousands separator.
        3. Replace non-standard decimal separator with ``'.'``.

        ``dec`` (implied decimal places) and ``sign`` are applied *after*
        ``pd.to_numeric`` by the caller.

        Args:
            series: Raw string Series to preprocess.
            field:  Field whose parser carries the numeric parameters.

        Returns:
            String Series ready for ``pd.to_numeric``.
        """
        params = field.parser.parameters
        thousands = params.get("thousands")
        decimal_sep = params.get("decimal", ".")

        s = series.astype(str).str.strip()
        if thousands:
            s = s.str.replace(thousands, "", regex=False)
        if decimal_sep != ".":
            s = s.str.replace(decimal_sep, ".", regex=False)
        return s

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
            print(f"  - parse_dates: {len(read_params.get('parse_dates', []))} fields")  # type: ignore[arg-type]

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
        """Apply row-wise converter function to a column (scalar fallback path)."""
        converter = self._create_converter(field)
        result = df[field_name].apply(converter)

        # For date/datetime types, ensure proper dtype after apply
        if field.type_name in (
            "date",
            "datetime",
        ) and not pd.api.types.is_datetime64_any_dtype(result):
            result = pd.to_datetime(result, errors="coerce")

        return result

    # ------------------------------------------------------------------
    # Vectorized conversion methods (Phase 2 / TASK-009, TASK-010)
    # These accept an optional ``field`` parameter so that parser
    # parameters (format, thousands, decimal, dec, sign) are applied
    # without falling back to row-wise Series.apply.
    # ------------------------------------------------------------------

    def _convert_date_type(
        self, series: pd.Series, field: Field | None = None
    ) -> pd.Series:
        """
        Vectorized date/datetime conversion.

        When *field* is provided the parser ``format`` parameter is forwarded
        to ``pd.to_datetime`` so that custom format strings are handled without
        per-row Python callbacks.

        Args:
            series: Raw string (or already date-like) Series.
            field:  Optional Field whose ``DateParser``/``DateTimeParser``
                    carries a ``format`` parameter.

        Returns:
            datetime64[ns] Series with null values for unparseable entries
            (when ``errors`` is ``'coerce'`` or ``'ignore'``) or a
            raised ``ValueError`` (when ``errors`` is ``'raise'``).
        """
        fmt: str | None = None
        if field is not None:
            fmt = field.parser.parameters.get("format")
        pd_errors = "coerce" if self.errors in ("coerce", "ignore") else "raise"
        return pd.to_datetime(series, format=fmt, errors=pd_errors)

    def _convert_integer_type(self, series: pd.Series) -> pd.Series:
        """
        Vectorized integer conversion.

        Args:
            series: Raw string or numeric Series.

        Returns:
            Int64 (nullable) or float64 Series.
        """
        pd_errors = "coerce" if self.errors in ("coerce", "ignore") else "raise"
        numeric_series = pd.to_numeric(series, errors=pd_errors)
        return (
            numeric_series.astype("Int64")
            if self.use_nullable_dtypes
            else numeric_series
        )

    def _convert_numeric_type(
        self, series: pd.Series, field: Field | None = None
    ) -> pd.Series:
        """
        Vectorized numeric (float) conversion.

        When *field* is provided the parser parameters ``thousands``,
        ``decimal``, ``dec``, and ``sign`` are applied using vectorized
        ``Series.str.replace`` before ``pd.to_numeric`` — eliminating
        per-row Python callbacks for parameterized numeric fields.

        Args:
            series: Raw string Series.
            field:  Optional Field whose ``NumericParser`` carries
                    ``thousands``, ``decimal``, ``dec``, and ``sign``
                    parameters.

        Returns:
            float64 Series with null values for unparseable entries.
        """
        pd_errors = "coerce" if self.errors in ("coerce", "ignore") else "raise"

        if field is not None and self._can_vectorize_numeric(field):
            params = field.parser.parameters
            s = self._normalize_numeric_series(series, field)
            result = pd.to_numeric(s, errors=pd_errors)

            dec = int(params.get("dec", 0))
            sign = str(params.get("sign", "+"))
            if dec > 0:
                result = result / (10**dec)
            if sign == "-":
                result = -result
            return result

        return pd.to_numeric(series, errors=pd_errors)

    # Boolean sentinel for mapping string values
    _BOOL_TRUE: frozenset[str] = frozenset({"true", "t", "yes", "y", "1", "on"})
    _BOOL_FALSE: frozenset[str] = frozenset({"false", "f", "no", "n", "0", "off"})

    def _convert_boolean_type(self, series: pd.Series) -> pd.Series:
        """
        Vectorized boolean conversion.

        Maps common boolean string representations (true/false, yes/no,
        1/0, on/off — case-insensitive) to a nullable ``boolean`` dtype.
        Unrecognised values become ``pd.NA``.

        Args:
            series: Raw string Series.

        Returns:
            boolean (nullable) or object bool Series.
        """
        bool_map = {
            **dict.fromkeys(self._BOOL_TRUE, True),
            **dict.fromkeys(self._BOOL_FALSE, False),
        }
        lower = series.astype(str).str.lower().str.strip()
        mapped = lower.map(bool_map)
        if self.use_nullable_dtypes:
            return mapped.astype("boolean")
        return mapped

    def _convert_string_type(self, series: pd.Series) -> pd.Series:
        """Convert series to string dtype."""
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

        for field_obj in self.fieldset.get_all_fields():
            field_name = field_obj.name

            # Skip if column doesn't exist in DataFrame
            if field_name not in df.columns:
                continue

            # -------------------------------------------------------
            # CAND-004 vectorized dispatch (Phase 2 / TASK-011)
            #
            # Routing priority (highest to lowest):
            #   1. date/datetime  -> _convert_date_type(series, field)
            #   2. numeric        -> _convert_numeric_type(series, field)
            #   3. integer        -> _convert_integer_type(series)
            #   4. boolean        -> _convert_boolean_type(series)
            #   5. string/char    -> _convert_string_type(series)
            #   6. fallback       -> _convert_with_converter(df, name, field)
            #
            # Steps 1-5 are fully vectorized pandas operations; step 6
            # is the row-wise scalar fallback for unknown/custom types.
            # -------------------------------------------------------
            type_name = field_obj.type_name
            try:
                if self._can_vectorize_date(field_obj):
                    df[field_name] = self._convert_date_type(
                        df[field_name], field=field_obj
                    )
                elif self._can_vectorize_numeric(field_obj):
                    df[field_name] = self._convert_numeric_type(
                        df[field_name], field=field_obj
                    )
                elif type_name == "integer":
                    df[field_name] = self._convert_integer_type(df[field_name])
                elif type_name == "boolean":
                    df[field_name] = self._convert_boolean_type(df[field_name])
                elif type_name in ("string", "character"):
                    df[field_name] = self._convert_string_type(df[field_name])
                else:
                    # Scalar fallback for custom/unknown types
                    df[field_name] = self._convert_with_converter(
                        df, field_name, field_obj
                    )

            except Exception as e:
                if self.errors == "raise":
                    raise TypeParseError(
                        f"Failed to convert field '{field_name}' to type "
                        f"'{field_obj.type_definition}': {e}"
                    ) from e
                elif self.verbose_warnings:
                    warnings.warn(
                        f"Error converting field '{field_name}' to type "
                        f"'{field_obj.type_definition}': {e}",
                        UserWarning,
                        stacklevel=2,
                    )

        return df

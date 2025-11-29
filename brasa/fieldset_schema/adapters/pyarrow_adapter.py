"""
PyArrow adapter for Fieldset.

Provides functionality to read CSV files into PyArrow Tables using a Fieldset schema.
"""

import io
import warnings
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv

from ..field import Field
from ..fieldset import Fieldset
from .pandas_adapter import PandasAdapter


class PyArrowAdapter:
    """
    Adapter to use Fieldset with PyArrow.

    Generates PyArrow schema from fieldset and provides CSV reading
    with type-safe conversion.
    """

    def __init__(
        self,
        fieldset: Fieldset,
        errors: Literal["raise", "coerce", "ignore"] = "coerce",
        use_decimal_for_numeric: bool = False,
        verbose_warnings: bool = True,
    ):
        """
        Initialize PyArrowAdapter.

        Args:
            fieldset: Fieldset instance defining the schema
            errors: Error handling strategy
            use_decimal_for_numeric: Use decimal128 for numeric types (recommended for financial data)
            verbose_warnings: Show detailed warnings
        """
        self.fieldset = fieldset
        self.errors: Literal["raise", "coerce", "ignore"] = errors
        self.use_decimal_for_numeric = use_decimal_for_numeric
        self.verbose_warnings = verbose_warnings

        self._schema: pa.Schema | None = None
        self._target_schema: pa.Schema | None = (
            None  # Schema with target types after preprocessing
        )
        self._needs_pandas_preprocessing: list[str] = []

        self._build_schema()

    def _build_schema(self) -> None:
        """Build PyArrow schema from fieldset."""
        fields = []
        target_fields = []

        for field in self.fieldset.get_all_fields():
            pa_type = self._get_pyarrow_type(field)
            target_type = pa_type  # Default: target type is same as initial type

            # Check if this field needs pandas preprocessing
            if self._needs_preprocessing(field):
                self._needs_pandas_preprocessing.append(field.name)
                # For pure PyArrow CSV reading, read as string first
                pa_type = pa.string()
                # But target type after pandas preprocessing should be the correct type

            pa_field = pa.field(field.name, pa_type, nullable=True)
            fields.append(pa_field)

            target_field = pa.field(field.name, target_type, nullable=True)
            target_fields.append(target_field)

        self._schema = pa.schema(fields)  # For pure PyArrow CSV reading
        self._target_schema = pa.schema(target_fields)  # For after pandas preprocessing

        # Warning if preprocessing needed
        if self._needs_pandas_preprocessing and self.verbose_warnings:
            warnings.warn(
                f"PyArrowAdapter: Fields with custom formats will be preprocessed with pandas: "
                f"{self._needs_pandas_preprocessing}. "
                f"This may impact performance on very large files (>1GB). "
                f"Consider using standard formats for optimal PyArrow performance.",
                UserWarning,
                stacklevel=2,
            )

    def _needs_preprocessing(self, field: Field) -> bool:
        """
        Check if field needs pandas preprocessing.

        Custom date formats and numeric with decimals require preprocessing.
        """
        type_name = field.type_name

        # Date/datetime/time with custom format
        if type_name in ("date", "datetime", "time") and field.parser.parameters.get(
            "format"
        ):
            return True

        # Numeric with decimal places or sign
        return (
            type_name == "numeric" and bool(field.parser.parameters.get("dec"))
        ) or bool(field.parser.parameters.get("sign"))

    def _get_pyarrow_type(self, field: Field) -> pa.DataType:
        """
        Map field type to PyArrow data type.

        Args:
            field: Field instance

        Returns:
            PyArrow DataType
        """
        type_name = field.type_name

        # Handle numeric type specially due to conditional logic
        if type_name == "numeric":
            if self.use_decimal_for_numeric:
                # Get decimal places from parameters, default to 2 for financial data
                dec_places = field.parser.parameters.get("dec", 2)
                # decimal128(precision, scale)
                # For financial data, typically precision=18, scale=dec_places
                return pa.decimal128(18, dec_places if dec_places > 0 else 2)
            else:
                return pa.float64()

        # Map other types using dictionary
        type_mapping = {
            "integer": pa.int64(),
            "date": pa.date32(),  # Use date32 for dates (days since epoch)
            "datetime": pa.timestamp("us"),  # Use timestamp with microsecond precision
            "time": pa.time64("us"),  # Use time64 with microsecond precision
            "boolean": pa.bool_(),
            "string": pa.string(),
            "character": pa.string(),
        }

        # Return mapped type or default to string
        return type_mapping.get(type_name, pa.string())

    def get_schema(self) -> pa.Schema:
        """
        Get PyArrow schema.

        Returns:
            PyArrow Schema object
        """
        if self._schema is None:
            self._build_schema()
        return self._schema

    def get_target_schema(self) -> pa.Schema:
        """
        Get the target PyArrow schema after preprocessing.

        This schema represents the final column types after any pandas
        preprocessing is applied. Use this schema when writing parquet files
        to ensure consistent type definitions.

        Returns:
            PyArrow Schema with target types
        """
        if self._target_schema is None:
            self._build_schema()
        return self._target_schema

    def read_csv(
        self, filepath_or_buffer: str | Path | io.StringIO, **kwargs: Any
    ) -> pa.Table:
        """
        Read CSV file as PyArrow Table with automatic type conversion.

        For fields with custom formats, uses pandas preprocessing then converts to PyArrow.

        Args:
            filepath_or_buffer: Path to CSV file or file-like object
            **kwargs: Additional arguments passed to pyarrow.csv.read_csv or pd.read_csv

        Returns:
            PyArrow Table with properly typed columns
        """
        if self.verbose_warnings:
            print("PyArrowAdapter: Reading CSV with Fieldset schema:")
            print(f"  - Schema fields: {len(self.get_schema())}")
            if self._needs_pandas_preprocessing:
                print(
                    f"  - Preprocessing with pandas: {len(self._needs_pandas_preprocessing)} fields"
                )

        # If we need pandas preprocessing, use hybrid approach
        if self._needs_pandas_preprocessing:
            return self._read_csv_with_pandas_preprocessing(
                filepath_or_buffer, **kwargs
            )
        else:
            return self._read_csv_pure_pyarrow(filepath_or_buffer, **kwargs)

    def _read_csv_pure_pyarrow(
        self, filepath_or_buffer: str | Path | io.StringIO, **kwargs: Any
    ) -> pa.Table:
        """Read CSV using pure PyArrow (no preprocessing needed)."""
        read_options = pa_csv.ReadOptions()
        parse_options = pa_csv.ParseOptions()
        convert_options = pa_csv.ConvertOptions()

        # Apply schema
        convert_options.column_types = self.get_schema()

        # Merge user options if provided
        if "read_options" in kwargs:
            read_options = kwargs.pop("read_options")
        if "parse_options" in kwargs:
            parse_options = kwargs.pop("parse_options")
        if "convert_options" in kwargs:
            convert_options = kwargs.pop("convert_options")

        try:
            table = pa_csv.read_csv(
                filepath_or_buffer,
                read_options=read_options,
                parse_options=parse_options,
                convert_options=convert_options,
            )
            return table
        except Exception as e:
            if self.errors == "raise":
                raise
            else:
                if self.verbose_warnings:
                    warnings.warn(
                        f"PyArrow CSV reading failed: {e}. "
                        f"Falling back to pandas preprocessing.",
                        UserWarning,
                        stacklevel=2,
                    )
                return self._read_csv_with_pandas_preprocessing(
                    filepath_or_buffer, **kwargs
                )

    def _read_csv_with_pandas_preprocessing(
        self, filepath_or_buffer: str | Path | io.StringIO, **kwargs: Any
    ) -> pa.Table:
        """
        Read CSV with pandas preprocessing for custom formats.

        Strategy:
        1. Use PandasAdapter to read CSV with correct types
        2. Convert pandas DataFrame to PyArrow Table
        3. Cast columns to target PyArrow schema types
        """
        # Create pandas adapter
        pandas_adapter = PandasAdapter(
            self.fieldset,
            errors=self.errors,
            use_nullable_dtypes=True,
            verbose_warnings=self.verbose_warnings,
        )

        # Read with pandas
        df = pandas_adapter.read_csv(filepath_or_buffer, **kwargs)

        # Convert to PyArrow table
        table = pa.Table.from_pandas(df, preserve_index=False)

        # Cast columns to match our schema
        try:
            table = self._cast_table_to_schema(table)
        except Exception as e:
            if self.errors == "raise":
                raise
            elif self.verbose_warnings:
                warnings.warn(
                    f"Some columns could not be cast to target schema: {e}",
                    UserWarning,
                    stacklevel=2,
                )

        return table

    def _cast_table_to_schema(self, table: pa.Table) -> pa.Table:
        """
        Cast PyArrow table columns to match target schema.

        Args:
            table: Input PyArrow table

        Returns:
            Table with columns cast to schema types
        """
        arrays = []
        fields = []

        # Use target schema for casting (has correct types after preprocessing)
        target_schema: pa.Schema = (
            self._target_schema if self._target_schema else self._schema
        )

        for field_name in target_schema.names:
            target_field = target_schema.field(field_name)
            target_type = target_field.type

            if field_name not in table.column_names:
                # Missing column - create null array
                null_array = pa.array([None] * len(table), type=target_type)
                arrays.append(null_array)
                fields.append(target_field)
                continue

            column = table.column(field_name)

            # Try to cast
            try:
                if pa.types.is_decimal(target_type):
                    # Special handling for decimal conversion
                    cast_column = self._cast_to_decimal(column, target_type)
                else:
                    cast_column = pc.cast(column, target_type, safe=False)

                arrays.append(cast_column)
                fields.append(target_field)
            except Exception as e:
                if self.errors == "raise":
                    raise ValueError(
                        f"Cannot cast column '{field_name}' to {target_type}: {e}"
                    ) from e
                else:
                    # Keep original column
                    if self.verbose_warnings:
                        warnings.warn(
                            f"Column '{field_name}' could not be cast to {target_type}, "
                            f"keeping as {column.type}",
                            UserWarning,
                            stacklevel=2,
                        )
                    arrays.append(column)
                    fields.append(pa.field(field_name, column.type, nullable=True))

        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))

    def _cast_to_decimal(self, column: pa.Array, target_type: pa.DataType) -> pa.Array:
        """
        Cast column to decimal type with proper handling of float conversion.

        Args:
            column: Input column
            target_type: Target decimal type

        Returns:
            Column cast to decimal
        """
        # First ensure we have numeric data
        if pa.types.is_floating(column.type) or pa.types.is_integer(column.type):
            # Convert to float first, then to decimal
            # PyArrow can cast float to decimal
            return pc.cast(column, target_type, safe=False)
        elif pa.types.is_string(column.type):
            # Parse string to float, then to decimal
            # Handle null values properly
            try:
                float_array = pc.cast(column, pa.float64(), safe=False)
                return pc.cast(float_array, target_type, safe=False)
            except pa.ArrowInvalid:
                # If cast fails, it might have invalid values
                # Try to handle them gracefully
                if self.errors == "raise":
                    raise
                # Create array with nulls for invalid values
                pylist = column.to_pylist()
                float_list = []
                for val in pylist:
                    if val is None or val == "":
                        float_list.append(None)
                    else:
                        try:
                            float_list.append(float(val))
                        except (ValueError, TypeError):
                            float_list.append(None)
                float_array = pa.array(float_list, type=pa.float64())
                return pc.cast(float_array, target_type, safe=False)
        else:
            return pc.cast(column, target_type, safe=False)

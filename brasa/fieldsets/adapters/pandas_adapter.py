"""
Pandas adapter for Fieldset.

Provides functionality to apply Fieldset type definitions to existing pandas DataFrames.
"""

import warnings
from collections.abc import Callable
from typing import Any

import pandas as pd

from ..field import Field
from ..fieldset import Fieldset


class PandasAdapter:
    """
    Adapter to apply Fieldset types to pandas DataFrames.

    Provides vectorized type conversion using pandas operations.
    """

    def __init__(self, fieldset: Fieldset, verbose_warnings: bool = True):
        """
        Initialize PandasAdapter.

        Args:
            fieldset: Fieldset instance defining the schema
            verbose_warnings: Show detailed warnings for conversion errors
        """
        self.fieldset = fieldset
        self.verbose_warnings = verbose_warnings

    def _create_converter(self, field: Field) -> Callable:
        """Create converter function for a field (scalar fallback path)."""

        def get_null_value() -> Any:
            """Get appropriate null value based on field type."""
            if field.type_name in ("date", "datetime", "time"):
                return pd.NaT
            return pd.NA

        def converter(value: Any) -> Any:
            if pd.isna(value) or value == "":
                return pd.NA
            try:
                return field.parse(str(value))
            except Exception as e:
                if self.verbose_warnings:
                    warnings.warn(
                        f"Failed to parse field '{field.name}' with value '{value}': {e}. "
                        f"Setting to NaN/NaT.",
                        UserWarning,
                        stacklevel=2,
                    )
                return get_null_value()

        return converter

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

    def _convert_date_type(
        self, series: pd.Series, field: Field | None = None
    ) -> pd.Series:
        """Vectorized date/datetime conversion."""
        fmt: str | None = None
        if field is not None:
            fmt = field.parser.parameters.get("format")
        return pd.to_datetime(series, format=fmt, errors="coerce")

    def _convert_integer_type(self, series: pd.Series) -> pd.Series:
        """Vectorized integer conversion."""
        return pd.to_numeric(series, errors="coerce").astype("Int64")

    def _convert_numeric_type(
        self, series: pd.Series, field: Field | None = None
    ) -> pd.Series:
        """Vectorized numeric (float) conversion."""
        if field is not None and field.type_name == "numeric":
            params = field.parser.parameters
            s = series.astype(str).str.strip()
            thousands = params.get("thousands")
            decimal_sep = params.get("decimal", ".")
            if thousands:
                s = s.str.replace(thousands, "", regex=False)
            if decimal_sep != ".":
                s = s.str.replace(decimal_sep, ".", regex=False)
            result = pd.to_numeric(s, errors="coerce")
            dec = int(params.get("dec", 0))
            sign = str(params.get("sign", "+"))
            if dec > 0:
                result = result / (10**dec)
            if sign == "-":
                result = -result
            return result

        return pd.to_numeric(series, errors="coerce")

    def _convert_boolean_type(self, series: pd.Series) -> pd.Series:
        """Vectorized boolean conversion."""
        _BOOL_TRUE = frozenset({"true", "t", "yes", "y", "1", "on"})
        _BOOL_FALSE = frozenset({"false", "f", "no", "n", "0", "off"})
        bool_map = {
            **dict.fromkeys(_BOOL_TRUE, True),
            **dict.fromkeys(_BOOL_FALSE, False),
        }
        lower = series.astype(str).str.lower().str.strip()
        mapped = lower.map(bool_map)
        return mapped.astype("boolean")

    def _convert_string_type(self, series: pd.Series) -> pd.Series:
        """Convert series to string dtype."""
        return series.astype("string")

    def apply_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply type conversions to an existing DataFrame using fieldset schema.

        Args:
            df: DataFrame with columns to be type-converted

        Returns:
            DataFrame with converted column types
        """
        df = df.copy()

        for field_obj in self.fieldset.get_all_fields():
            field_name = field_obj.name

            # Skip if column doesn't exist in DataFrame
            if field_name not in df.columns:
                continue

            type_name = field_obj.type_name
            try:
                if type_name in ("date", "datetime"):
                    df[field_name] = self._convert_date_type(
                        df[field_name], field=field_obj
                    )
                elif type_name == "numeric":
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
                if self.verbose_warnings:
                    warnings.warn(
                        f"Error converting field '{field_name}' to type "
                        f"'{field_obj.type_definition}': {e}",
                        UserWarning,
                        stacklevel=2,
                    )

        return df

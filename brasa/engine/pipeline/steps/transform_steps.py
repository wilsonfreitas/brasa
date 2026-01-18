"""Data transformation pipeline steps.

Steps for transforming data values, parsing types, and applying field schemas.
These steps work with both DataFrames and PyArrow Datasets where possible.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from brasa.engine.pipeline.shared_transforms import to_dataframe

from ..context import PipelineContext
from ..registry import StepRegistry
from ..step import PipelineStep


@StepRegistry.register("apply_fields")
class ApplyFieldsStep(PipelineStep):
    """Apply field definitions from the template to the DataFrame.

    Uses the Fieldset's PandasAdapter to convert columns to their
    defined types (dates, numbers, etc.).

    Parameters:
        errors: How to handle conversion errors ('raise', 'coerce', 'ignore')
        set_columns: Whether to set DataFrame columns to field names
    """

    def execute(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        if context.fields is None:
            return data

        from brasa.fieldset_schema import PandasAdapter

        errors = self.get_param("errors", "coerce")
        set_columns = self.get_param("set_columns", False)
        adapter = PandasAdapter(context.fields, errors=errors)
        if set_columns:
            data.columns = context.fields.get_field_names()
        return adapter.apply_types(data)


@StepRegistry.register("apply_fields_multi")
class ApplyFieldsMultiStep(PipelineStep):
    """Apply field definitions to multiple DataFrames in a dict.

    For multi-output pipelines, applies the corresponding fieldset to each
    dataset using the dataset configurations from context.

    Parameters:
        errors: How to handle conversion errors ('raise', 'coerce', 'ignore')
    """

    def execute(
        self, data: dict[str, pd.DataFrame], context: PipelineContext
    ) -> dict[str, pd.DataFrame]:
        if not isinstance(data, dict):
            raise ValueError(
                f"apply_fields_multi expects Dict[str, DataFrame], got {type(data).__name__}"
            )

        from brasa.fieldset_schema import PandasAdapter

        errors = self.get_param("errors", "coerce")
        result: dict[str, pd.DataFrame] = {}

        for dataset_name, df in data.items():
            fieldset = context.get_dataset_fieldset(dataset_name)
            if fieldset and len(fieldset) > 0:
                adapter = PandasAdapter(fieldset, errors=errors)
                result[dataset_name] = adapter.apply_types(df)
            else:
                # No fieldset defined, pass through unchanged
                result[dataset_name] = df

        return result


@StepRegistry.register("parse_numeric")
class ParseNumericStep(PipelineStep):
    """Parse string columns as numeric values.

    Parameters:
        columns: List of column names to parse
        errors: How to handle parsing errors ('raise', 'coerce', 'ignore')
    """

    def execute(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        columns = self.require_param("columns")
        errors = self.get_param("errors", "coerce")
        decimal = context.decimal
        thousands = context.thousands

        for col in columns:
            if col not in data.columns:
                continue
            if data[col].dtype == object:
                # String column - need to clean and convert
                cleaned = data[col].astype(str)
                if thousands and thousands != "":
                    cleaned = cleaned.str.replace(thousands, "", regex=False)
                if decimal and decimal != ".":
                    cleaned = cleaned.str.replace(decimal, ".", regex=False)
                data[col] = pd.to_numeric(cleaned, errors=errors)
            else:
                data[col] = pd.to_numeric(data[col], errors=errors)

        return data


@StepRegistry.register("parse_date")
class ParseDateStep(PipelineStep):
    """Parse string columns as dates.

    Parameters:
        columns: List of column names to parse
        format: Date format string (e.g., '%Y-%m-%d')
        errors: How to handle parsing errors ('raise', 'coerce', 'ignore')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        columns = self.require_param("columns")
        date_format = self.get_param("format")
        errors = self.get_param("errors", "coerce")

        for col in columns:
            if col not in data.columns:
                continue
            data[col] = pd.to_datetime(data[col], format=date_format, errors=errors)

        return data


@StepRegistry.register("parse_datetime")
class ParseDateTimeStep(PipelineStep):
    """Parse string columns as datetime values.

    Parameters:
        columns: List of column names to parse
        format: DateTime format string
        errors: How to handle parsing errors ('raise', 'coerce', 'ignore')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        columns = self.require_param("columns")
        dt_format = self.get_param("format")
        errors = self.get_param("errors", "coerce")

        for col in columns:
            if col not in data.columns:
                continue
            data[col] = pd.to_datetime(data[col], format=dt_format, errors=errors)

        return data


@StepRegistry.register("fill_na")
class FillNaStep(PipelineStep):
    """Fill NA/NaN values in columns (supports DataFrame and PyArrow Dataset).

    Parameters:
        columns: List of column names to fill (optional, all if not specified)
        value: Value to use for filling
        method: Fill method ('ffill', 'bfill')
    """

    def execute(self, data: pd.DataFrame, _context: Any) -> pd.DataFrame:
        from .. import shared_transforms

        columns = self.get_param("columns")
        value = self.get_param("value")
        method = self.get_param("method")

        return shared_transforms.fill_na(data, value, method, columns)


@StepRegistry.register("drop_duplicates")
class DropDuplicatesStep(PipelineStep):
    """Remove duplicate rows (supports DataFrame and PyArrow Dataset).

    Parameters:
        subset: Column names to consider for identifying duplicates (optional).
        keep: Which duplicates to keep ('first', 'last', False). Default: 'first'.
    """

    def execute(self, data: pd.DataFrame, _context: Any) -> pd.DataFrame:
        from .. import shared_transforms

        subset = self.get_param("subset")
        keep = self.get_param("keep", "first")
        return shared_transforms.drop_duplicates(data, subset, keep)


@StepRegistry.register("drop_na")
class DropNaStep(PipelineStep):
    """Drop rows with NA/NaN values.

    Parameters:
        columns: List of column names to check (optional, all if not specified)
        how: 'any' or 'all' (default: 'any')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        columns = self.get_param("columns")
        how = self.get_param("how", "any")

        if columns:
            return data.dropna(subset=columns, how=how)
        return data.dropna(how=how)


@StepRegistry.register("filter_rows")
class FilterRowsStep(PipelineStep):
    """Filter rows based on a condition.

    Parameters:
        column: Column to filter on
        operator: Comparison operator ('eq', 'ne', 'gt', 'lt', 'ge', 'le', 'in', 'notin', 'notna', 'isna')
        value: Value to compare against (not needed for 'notna', 'isna')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        column = self.require_param("column")
        operator = self.require_param("operator")
        value = self.get_param("value")

        col = data[column]

        if operator == "eq":
            mask = col == value
        elif operator == "ne":
            mask = col != value
        elif operator == "gt":
            mask = col > value
        elif operator == "lt":
            mask = col < value
        elif operator == "ge":
            mask = col >= value
        elif operator == "le":
            mask = col <= value
        elif operator == "in":
            mask = col.isin(value)
        elif operator == "notin":
            mask = ~col.isin(value)
        elif operator == "notna":
            mask = col.notna()
        elif operator == "isna":
            mask = col.isna()
        else:
            raise ValueError(f"Unknown operator: {operator}")

        return data[mask]


@StepRegistry.register("forward_fill_column")
class ForwardFillColumnStep(PipelineStep):
    """Forward fill values in a column where condition is met.

    This is useful for tables where a cell value applies to multiple rows below it.

    Parameters:
        column: Column to forward fill
        condition: Condition to check ('notna', 'isna', or None for unconditional)
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        column = self.require_param("column")

        data[column] = data[column].ffill()
        return data


@StepRegistry.register("extract_regex")
class ExtractRegexStep(PipelineStep):
    """Extract values from a column using regex.

    Parameters:
        column: Column to extract from
        pattern: Regex pattern with capture groups
        output: Output column name (default: same as input)
        group: Capture group index (default: 0, meaning first group)
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        column = self.require_param("column")
        pattern = self.require_param("pattern")
        output = self.get_param("output", column)
        group = self.get_param("group", 0)

        extracted = data[column].str.extract(pattern)
        data[output] = extracted.iloc[:, group]
        return data


@StepRegistry.register("concat_columns")
class ConcatColumnsStep(PipelineStep):
    """Concatenate multiple columns into one.

    Parameters:
        columns: List of column names to concatenate
        output: Output column name
        separator: Separator between values (default: '')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        columns = self.require_param("columns")
        output = self.require_param("output")
        separator = self.get_param("separator", "")

        data[output] = data[columns[0]].astype(str)
        for col in columns[1:]:
            data[output] = data[output] + separator + data[col].astype(str)

        return data


@StepRegistry.register("melt")
class MeltStep(PipelineStep):
    """Unpivot a DataFrame from wide to long format.

    Similar to pandas.melt() or R's tidyr::pivot_longer().

    Parameters:
        id_vars: Column(s) to use as identifier variables
        value_vars: Column(s) to unpivot (if not specified, uses all columns not in id_vars)
        var_name: Name for the variable column (default: 'variable')
        value_name: Name for the value column (default: 'value')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        id_vars = self.get_param("id_vars")
        value_vars = self.get_param("value_vars")
        var_name = self.get_param("var_name", "variable")
        value_name = self.get_param("value_name", "value")

        data = to_dataframe(data)

        return pd.melt(
            data,
            id_vars=id_vars,
            value_vars=value_vars,
            var_name=var_name,
            value_name=value_name,
        )


@StepRegistry.register("sort")
class SortStep(PipelineStep):
    """Sort data by one or more columns (supports DataFrame and PyArrow Dataset).

    Parameters:
        by: Column name or list of column names to sort by
        ascending: Sort ascending (default: True). Can be bool or list of bools.
        descending: Sort descending (default: False). Alternative to ascending.
        na_position: Where to place NAs ('first' or 'last', default: 'last')
    """

    def execute(self, data: pd.DataFrame, _context: Any) -> pd.DataFrame:
        from .. import shared_transforms

        by = self.require_param("by")
        ascending = self.get_param("ascending")
        descending = self.get_param("descending")

        # Handle ascending/descending parameters
        if descending is not None:
            # Use descending parameter (ETL style)
            return shared_transforms.sort_data(data, by, descending)
        elif ascending is not None:
            # Use ascending parameter (reader style) - convert to descending
            if isinstance(ascending, bool):
                desc = not ascending
            else:
                desc = [not a for a in ascending]
            return shared_transforms.sort_data(data, by, desc)
        else:
            # Default: ascending (descending=False)
            return shared_transforms.sort_data(data, by, False)


@StepRegistry.register("make_date")
class MakeDateStep(PipelineStep):
    """Create a date column from year, month, and day components.

    Parameters:
        year_column: Column containing year values
        month_column: Column containing month values
        day_column: Column containing day values
        output: Name of the output date column (default: 'date')
        errors: How to handle errors ('raise', 'coerce', 'ignore', default: 'coerce')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        year_col = self.require_param("year_column")
        month_col = self.require_param("month_column")
        day_col = self.require_param("day_column")
        output = self.get_param("output", "date")
        errors = self.get_param("errors", "coerce")

        data[output] = pd.to_datetime(
            {
                "year": data[year_col],
                "month": data[month_col],
                "day": data[day_col],
            },
            errors=errors,
        )

        return data


@StepRegistry.register("str_replace")
class StrReplaceStep(PipelineStep):
    """Replace occurrences of pattern in a string column.

    Parameters:
        column: Column to apply the replacement
        pattern: Pattern to search for (string or regex)
        replacement: Replacement string
        output: Output column name (default: same as input)
        regex: Whether pattern is a regex (default: False)
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        column = self.require_param("column")
        pattern = self.require_param("pattern")
        replacement = self.get_param("replacement", "")
        output = self.get_param("output", column)
        regex = self.get_param("regex", False)

        data[output] = data[column].str.replace(pattern, replacement, regex=regex)
        return data


@StepRegistry.register("cast")
class CastStep(PipelineStep):
    """Cast column(s) to a specific type.

    Parameters:
        column: Column name or list of columns to cast
        dtype: Target data type ('int', 'float', 'str', 'datetime', 'bool')
        errors: How to handle errors ('raise', 'coerce', 'ignore', default: 'coerce')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        columns = self.require_param("column")
        dtype = self.require_param("dtype")
        errors = self.get_param("errors", "coerce")

        if isinstance(columns, str):
            columns = [columns]

        for col in columns:
            if col not in data.columns:
                continue

            if dtype == "int":
                data[col] = pd.to_numeric(data[col], errors=errors).astype("Int64")
            elif dtype == "float":
                data[col] = pd.to_numeric(data[col], errors=errors)
            elif dtype == "str":
                data[col] = data[col].astype(str)
            elif dtype == "datetime":
                data[col] = pd.to_datetime(data[col], errors=errors)
            elif dtype == "bool":
                data[col] = data[col].astype(bool)
            else:
                data[col] = data[col].astype(dtype)

        return data

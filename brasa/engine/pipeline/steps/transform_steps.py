"""Data transformation pipeline steps.

Steps for transforming data values, parsing types, and applying field schemas.
"""

from __future__ import annotations

import pandas as pd

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
    """

    def execute(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        if context.fields is None:
            return data

        from brasa.fieldset_schema import PandasAdapter

        errors = self.get_param("errors", "coerce")
        adapter = PandasAdapter(context.fields, errors=errors)
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
    """Fill NA/NaN values in columns.

    Parameters:
        columns: List of column names to fill (optional, all if not specified)
        value: Value to use for filling
        method: Fill method ('ffill', 'bfill')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        columns = self.get_param("columns")
        value = self.get_param("value")
        method = self.get_param("method")

        if columns:
            subset = data[columns]
            if value is not None:
                data[columns] = subset.fillna(value)
            elif method == "ffill":
                data[columns] = subset.ffill()
            elif method == "bfill":
                data[columns] = subset.bfill()
        elif value is not None:
            data = data.fillna(value)
        elif method == "ffill":
            data = data.ffill()
        elif method == "bfill":
            data = data.bfill()

        return data


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

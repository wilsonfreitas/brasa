"""Shared pipeline steps that work with both reader and ETL pipelines.

This module provides steps that only depend on the common context interface
(PipelineContextProtocol) and can be registered in both StepRegistry and
ETLStepRegistry.

Steps in this module:
- Work with both PipelineContext and ETLPipelineContext
- Operate on DataFrames or PyArrow Datasets
- Don't require context-specific features (like meta or writer)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pandas as pd
import pyarrow.dataset as ds

if TYPE_CHECKING:
    pass


def filter_data(
    data: ds.Dataset | pd.DataFrame,
    where: dict[str, Any],
) -> pd.DataFrame:
    """Filter rows based on conditions.

    Args:
        data: Input dataset or DataFrame.
        where: Dictionary of column -> value(s) for equality filtering.
               Can be a single value or list of values (IN clause).

    Returns:
        Filtered DataFrame.
    """
    import pyarrow.compute as pc

    if isinstance(data, ds.Dataset):
        # Build filter expression for PyArrow Dataset
        expr = None
        for column, value in where.items():
            if isinstance(value, list):
                condition = pc.field(column).isin(value)
            else:
                condition = pc.field(column) == value
            expr = condition if expr is None else expr & condition
        _data = data.to_table()
        return (_data.filter(expr) if expr is not None else _data).to_pandas()

    elif isinstance(data, pd.DataFrame):
        # Filter pandas DataFrame
        mask = pd.Series([True] * len(data))
        for column, value in where.items():
            if isinstance(value, list):
                mask &= data[column].isin(value)
            else:
                mask &= data[column] == value
        return data[mask]

    else:
        raise TypeError(f"Cannot filter data of type {type(data)}")


def select_columns(
    data: ds.Dataset | pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Select specific columns from the dataset.

    Args:
        data: Input dataset or DataFrame.
        columns: List of column names to select.

    Returns:
        DataFrame with only selected columns.
    """
    if isinstance(data, ds.Dataset):
        # For datasets, use scanner with column projection
        return data.to_table(columns=columns).to_pandas()
    elif isinstance(data, pd.DataFrame):
        return data[columns]
    else:
        raise TypeError(f"Cannot select columns from data of type {type(data)}")


def sort_data(
    data: ds.Dataset | pd.DataFrame,
    by: str | list[str],
    descending: bool | list[bool] = False,
) -> pd.DataFrame:
    """Sort data by specified columns.

    Args:
        data: Input dataset or DataFrame.
        by: Column name or list of column names to sort by.
        descending: Whether to sort in descending order.

    Returns:
        Sorted DataFrame (converts to DataFrame if needed).
    """
    # Ensure by is a list
    if isinstance(by, str):
        by = [by]

    # Convert to DataFrame if needed
    if isinstance(data, ds.Dataset):
        df = data.to_table().to_pandas()
    elif hasattr(data, "to_pandas"):
        df = data.to_pandas()
    else:
        df = data

    # Handle descending parameter
    if isinstance(descending, bool):
        ascending = [not descending] * len(by)
    else:
        ascending = [not d for d in descending]

    return df.sort_values(by=by, ascending=ascending)


def to_dataframe(data: Any) -> pd.DataFrame:
    """Convert data to pandas DataFrame.

    Args:
        data: Input data (Dataset, Table, or DataFrame).

    Returns:
        Pandas DataFrame.
    """
    if isinstance(data, pd.DataFrame):
        return data
    elif isinstance(data, ds.Dataset):
        return data.to_table().to_pandas()
    elif hasattr(data, "to_pandas"):
        return data.to_pandas()
    else:
        raise TypeError(f"Cannot convert {type(data)} to DataFrame")


def drop_columns(
    data: ds.Dataset | pd.DataFrame,
    columns: list[str],
) -> pd.DataFrame:
    """Drop specified columns from the dataset.

    Args:
        data: Input dataset or DataFrame.
        columns: List of column names to drop.

    Returns:
        DataFrame with columns removed.
    """
    df = to_dataframe(data)
    return df.drop(columns=columns, errors="ignore")


def rename_columns(
    data: ds.Dataset | pd.DataFrame,
    mapping: dict[str, str],
) -> pd.DataFrame:
    """Rename columns in the dataset.

    Args:
        data: Input dataset or DataFrame.
        mapping: Dictionary of old_name -> new_name.

    Returns:
        DataFrame with renamed columns.
    """
    df = to_dataframe(data)
    return df.rename(columns=mapping)


def drop_duplicates(
    data: ds.Dataset | pd.DataFrame,
    subset: list[str] | None = None,
    keep: str = "first",
) -> pd.DataFrame:
    """Remove duplicate rows.

    Args:
        data: Input dataset or DataFrame.
        subset: Column names to consider for identifying duplicates.
        keep: Which duplicates to keep ('first', 'last', False).

    Returns:
        DataFrame with duplicates removed.
    """
    df = to_dataframe(data)
    return df.drop_duplicates(subset=subset, keep=keep)


def fill_na(
    data: ds.Dataset | pd.DataFrame,
    value: Any = None,
    method: str | None = None,
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Fill missing values.

    Args:
        data: Input dataset or DataFrame.
        value: Value to fill NA with.
        method: Fill method ('ffill', 'bfill').
        columns: Columns to fill (None = all columns).

    Returns:
        DataFrame with NA values filled.
    """
    df = to_dataframe(data)
    if columns:
        if method:
            df[columns] = df[columns].fillna(method=method)
        else:
            df[columns] = df[columns].fillna(value)
    elif method:
        df = df.fillna(method=method)
    else:
        df = df.fillna(value)
    return df


def convert_future_maturity_codes_to_dates(
    data: ds.Dataset | pd.DataFrame,
    code_column: str,
    date_column: str,
    maturity_day: str = "first day",
    calendar: str = "Actual",
) -> pd.DataFrame:
    """Convert futures maturity codes to actual dates.

    Args:
        data: Input dataset or DataFrame.
        code_column: Column with maturity codes (e.g. 'H23').
        date_column: Column to create with actual dates.
        maturity_day: 'first day' or 'last day' of the month.
        calendar: Business day calendar to use (default: Actual).

    Returns:
        DataFrame with new date column.
    """

    from bizdays import Calendar

    from brasa.parsers.b3.futures_settlement_prices import maturity2date

    cal: Any = Calendar.load(calendar)
    df = to_dataframe(data)

    df[date_column] = df[code_column].apply(
        lambda x: maturity2date(x, cal, maturity_day)
    )
    return df


def adjust_following_bizdays(
    data: ds.Dataset | pd.DataFrame,
    date_column: str,
    adjusted_column: str,
    calendar: str = "Actual",
) -> pd.DataFrame:
    """Convert futures maturity codes to actual dates.

    Args:
        data: Input dataset or DataFrame.
        date_column: Column with dates to adjust.
        adjusted_column: Column to create with adjusted dates.
        calendar: Business day calendar to use (default: Actual).

    Returns:
        DataFrame with new date column.
    """

    from bizdays import Calendar

    cal: Any = Calendar.load(calendar)
    df = to_dataframe(data)

    df[adjusted_column] = cal.following(df[date_column])
    return df


def calculate_bizdays(
    data: ds.Dataset | pd.DataFrame,
    start_date_column: str,
    end_date_column: str,
    bizdays_column: str,
    calendar: str = "Actual",
) -> pd.DataFrame:
    """Calculate business days between two date columns.

    Args:
        data: Input dataset or DataFrame.
        start_date_column: Column with start dates.
        end_date_column: Column with end dates.
        bizdays_column: Column to create with business days count.
        calendar: Business day calendar to use (default: Actual).

    Returns:
        DataFrame with new bizdays column.
    """

    from bizdays import Calendar

    cal: Any = Calendar.load(calendar)
    df = to_dataframe(data)

    df[bizdays_column] = cal.bizdays(
        df[start_date_column],
        df[end_date_column],
    )
    return df


def calculate_implied_rate(
    data: ds.Dataset | pd.DataFrame,
    price_column: str,
    implied_rate_column: str,
    days_to_maturity_column: str,
    compounding: str = "simple",
    forward_price: float = 100_000,
) -> pd.DataFrame:
    """Calculate implied interest rate from futures prices.

    Args:
        data: Input dataset or DataFrame.
        price_column: Column with futures prices.
        settlement_column: Column with settlement prices.
        days_to_maturity_column: Column with days to maturity.
        implied_rate_column: Column to create with implied rates.
    Returns:
        DataFrame with new implied rate column.
    """
    df = to_dataframe(data)

    if compounding == "simple":
        t = df[days_to_maturity_column] / 360
        df[implied_rate_column] = (forward_price / df[price_column] - 1) * (1 / t)
    elif compounding == "discrete":
        t = df[days_to_maturity_column] / 252
        df[implied_rate_column] = (forward_price / df[price_column]) ** (1 / t) - 1

    return df


def flatten_column(
    data: ds.Dataset | pd.DataFrame,
    columns: list[str],
    separator: str = ",",
) -> pd.DataFrame:
    """Flatten columns by splitting values and exploding into separate rows.

    This function takes columns containing delimited values (e.g., "A,B,C")
    and expands them into multiple rows, one for each value.

    Args:
        data: Input dataset or DataFrame.
        columns: List of column names to flatten.
        separator: The delimiter used to separate values in the column.
            Defaults to comma (",").

    Returns:
        DataFrame with the specified columns flattened into separate rows.

    Example:
        Input DataFrame:
            | code  | indexes     |
            |-------|-------------|
            | PETR4 | IBOV,IBRX   |
            | VALE3 | IBOV        |

        After flatten_column(df, ["indexes"], separator=","):
            | code  | indexes |
            |-------|---------|
            | PETR4 | IBOV    |
            | PETR4 | IBRX    |
            | VALE3 | IBOV    |
    """
    df = to_dataframe(data)

    for column in columns:
        if column not in df.columns:
            raise ValueError(f"Column '{column}' not found in DataFrame")
        # Split values by separator and strip whitespace from each value
        df[column] = (
            df[column]
            .str.split(separator)
            .apply(lambda x: [v.strip() for v in x] if isinstance(x, list) else x)
        )
        # Explode the column to create separate rows
        df = df.explode(column, ignore_index=True)

    return df

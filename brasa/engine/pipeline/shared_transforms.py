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
) -> ds.Dataset | pd.DataFrame:
    """Filter rows based on conditions.

    Args:
        data: Input dataset or DataFrame.
        where: Dictionary of column -> value(s) for equality filtering.
               Can be a single value or list of values (IN clause).

    Returns:
        Filtered dataset or DataFrame.
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
        return data.filter(expr) if expr else data

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
) -> ds.Dataset | pd.DataFrame:
    """Select specific columns from the dataset.

    Args:
        data: Input dataset or DataFrame.
        columns: List of column names to select.

    Returns:
        Dataset or DataFrame with only selected columns.
    """
    if isinstance(data, ds.Dataset):
        # For datasets, use scanner with column projection
        return data.scanner(columns=columns).to_table()
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

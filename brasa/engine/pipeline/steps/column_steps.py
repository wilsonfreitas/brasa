"""Column manipulation pipeline steps.

Steps for renaming, selecting, and transforming DataFrame columns.
These steps work with both DataFrames and PyArrow Datasets where possible.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..context import PipelineContext
from ..registry import StepRegistry
from ..step import PipelineStep


@StepRegistry.register("set_columns")
class SetColumnsStep(PipelineStep):
    """Set column names for a DataFrame.

    Parameters:
        names: List of column names to assign
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        names = self.require_param("names")

        if len(names) != len(data.columns):
            raise ValueError(
                f"Number of names ({len(names)}) doesn't match "
                f"number of columns ({len(data.columns)})"
            )

        data.columns = names
        return data


@StepRegistry.register("rename_columns")
class RenameColumnsStep(PipelineStep):
    """Rename columns using a mapping (supports DataFrame and PyArrow Dataset).

    Parameters:
        mapping: Dictionary mapping old names to new names
    """

    def execute(self, data: pd.DataFrame, _context: Any) -> pd.DataFrame:
        from .. import shared_transforms

        mapping = self.require_param("mapping")
        return shared_transforms.rename_columns(data, mapping)


@StepRegistry.register("select_columns")
class SelectColumnsStep(PipelineStep):
    """Select specific columns from data (supports DataFrame and PyArrow Dataset).

    Parameters:
        columns: List of column names to keep
    """

    def execute(self, data: pd.DataFrame, _context: Any) -> pd.DataFrame:
        from .. import shared_transforms

        columns = self.require_param("columns")
        return shared_transforms.select_columns(data, columns)


@StepRegistry.register("drop_columns")
class DropColumnsStep(PipelineStep):
    """Drop columns from data (supports DataFrame and PyArrow Dataset).

    Parameters:
        columns: List of column names to drop
        errors: How to handle missing columns ('raise' or 'ignore', default: 'ignore')
    """

    def execute(self, data: pd.DataFrame, _context: Any) -> pd.DataFrame:
        from .. import shared_transforms

        columns = self.require_param("columns")
        return shared_transforms.drop_columns(data, columns)


@StepRegistry.register("add_column")
class AddColumnStep(PipelineStep):
    """Add a new column to the DataFrame.

    Parameters:
        name: Name of the new column
        value: Static value to assign (optional)
        from: Dictionary with 'where' and 'key' to get value dynamically (optional)
            - where: 'context', 'download_args', or 'extra_key'
            - key: The key to look up (not needed for 'extra_key')
    """

    def _resolve_value(self, context: PipelineContext) -> Any:
        """Resolve the column value from params or context.

        Returns:
            The resolved value to assign to the column.

        Raises:
            ValueError: If no valid value source is provided.
        """
        if "value" in self.params:
            return self.params["value"]
        elif "from" in self.params:
            from_param = self.params["from"]
            where = from_param["where"]
            if where == "context":
                key = from_param["key"]
                return context.get_result(key)
            elif where == "download_args":
                key = from_param["key"]
                return context.meta.download_args.get(key)
            elif where == "extra_key":
                return context.meta.extra_key
            else:
                raise ValueError(f"Unknown 'from.where' value: {where}")
        else:
            raise ValueError("add_column requires 'value', or 'from' parameter")

    def execute(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        name = self.require_param("name")
        value = self._resolve_value(context)
        data[name] = value
        return data


@StepRegistry.register("add_column_multi")
class AddColumnMultiStep(AddColumnStep):
    """Add a new column to multiple DataFrames in a dictionary.

    Works the same way as add_column but operates on a dict of DataFrames.

    Parameters:
        name: Name of the new column
        value: Static value to assign (optional)
        from: Dictionary with 'where' and 'key' to get value dynamically (optional)
            - where: 'context', 'download_args', or 'extra_key'
            - key: The key to look up (not needed for 'extra_key')
    """

    def execute(
        self, data: dict[str, pd.DataFrame], context: PipelineContext
    ) -> dict[str, pd.DataFrame]:
        name = self.require_param("name")
        value = self._resolve_value(context)

        for df in data.values():
            df[name] = value

        return data


@StepRegistry.register("reorder_columns")
class ReorderColumnsStep(PipelineStep):
    """Reorder columns in a specific order.

    Parameters:
        order: List of column names in desired order
        keep_rest: Whether to keep unlisted columns at the end (default: False)
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        order = self.require_param("order")
        keep_rest = self.get_param("keep_rest", False)

        missing = set(order) - set(data.columns)
        if missing:
            raise ValueError(f"Columns not found: {missing}")

        if keep_rest:
            rest = [c for c in data.columns if c not in order]
            order = list(order) + rest

        return data[order]

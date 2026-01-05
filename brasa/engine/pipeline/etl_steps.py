"""ETL pipeline steps for dataset transformations.

This module provides pipeline steps for ETL operations that transform
datasets into derived datasets.

The ETL steps use shared transformation functions from shared_transforms.py
to enable code reuse between reader and ETL pipelines.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

import pandas as pd
import pyarrow.dataset as ds

from . import shared_transforms

if TYPE_CHECKING:
    from .etl_context import ETLPipelineContext


class ETLPipelineStep(ABC):
    """Base class for ETL pipeline steps.

    ETL pipeline steps operate on PyArrow datasets or DataFrames and
    produce transformed data for the next step in the pipeline.

    Attributes:
        name: The registered name of this step type.
        params: Configuration parameters for this step instance.
    """

    name: str = ""

    def __init__(self, params: dict[str, Any] | None = None) -> None:
        """Initialize the step with configuration parameters.

        Args:
            params: Dictionary of parameters from the YAML configuration.
        """
        self.params = params or {}

    @abstractmethod
    def execute(self, data: Any, context: ETLPipelineContext) -> Any:
        """Execute the step and return the transformed data.

        Args:
            data: Input data from the previous step (or None for first step).
            context: ETL pipeline context containing metadata and configuration.

        Returns:
            Transformed data to pass to the next step.
        """
        ...

    def get_param(self, key: str, default: Any = None) -> Any:
        """Get a parameter value with an optional default.

        Args:
            key: Parameter name.
            default: Default value if parameter is not set.

        Returns:
            The parameter value or default.
        """
        return self.params.get(key, default)

    def require_param(self, key: str) -> Any:
        """Get a required parameter value.

        Args:
            key: Parameter name.

        Returns:
            The parameter value.

        Raises:
            ValueError: If the parameter is not set.
        """
        if key not in self.params:
            raise ValueError(f"Step '{self.name}' requires parameter '{key}'")
        return self.params[key]

    def get_input_datasets(self) -> list[str]:
        """Get the list of input dataset names this step depends on.

        Returns:
            List of dataset names that are inputs to this step.
        """
        inputs = []
        if "input" in self.params:
            inputs.append(self.params["input"])
        if "right" in self.params:
            inputs.append(self.params["right"])
        return inputs

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ETLPipelineStep:
        """Create a step instance from YAML configuration.

        Args:
            config: Dictionary from YAML configuration.

        Returns:
            Configured step instance.
        """
        params = {k: v for k, v in config.items() if k != "step"}
        return cls(params)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(params={self.params})"


class ETLStepRegistry:
    """Registry of available ETL pipeline steps.

    Provides decorator-based registration and factory method for creating
    step instances from YAML configuration.
    """

    _steps: ClassVar[dict[str, type[ETLPipelineStep]]] = {}

    @classmethod
    def register(cls, name: str):
        """Decorator to register a step class with a given name.

        Args:
            name: The name to register the step under.

        Returns:
            Decorator function.
        """

        def decorator(step_class: type[ETLPipelineStep]):
            if name in cls._steps:
                raise ValueError(
                    f"ETL Step '{name}' is already registered by {cls._steps[name].__name__}"
                )
            step_class.name = name
            cls._steps[name] = step_class
            return step_class

        return decorator

    @classmethod
    def create(cls, name: str, config: dict[str, Any]) -> ETLPipelineStep:
        """Create a step instance from configuration.

        Args:
            name: The registered name of the step.
            config: Step configuration from YAML.

        Returns:
            Configured step instance.

        Raises:
            ValueError: If the step name is not registered.
        """
        if name not in cls._steps:
            available = ", ".join(sorted(cls._steps.keys()))
            raise ValueError(f"Unknown ETL step '{name}'. Available steps: {available}")
        step_class = cls._steps[name]
        return step_class.from_config(config)

    @classmethod
    def get_all_steps(cls) -> dict[str, type[ETLPipelineStep]]:
        """Get all registered steps.

        Returns:
            Dictionary mapping step names to step classes.
        """
        return cls._steps.copy()


# =============================================================================
# Built-in ETL Steps
# =============================================================================


@ETLStepRegistry.register("load")
class LoadDatasetStep(ETLPipelineStep):
    """Load a dataset by name.

    This step loads an existing dataset and returns it as a PyArrow Dataset.
    It must be the first step in a pipeline (or used to load additional datasets).

    Parameters:
        input: Name of the dataset to load.
    """

    def execute(self, _data: Any, _context: ETLPipelineContext) -> ds.Dataset:
        """Load the specified dataset.

        Args:
            data: Ignored (this is typically the first step).
            context: ETL pipeline context.

        Returns:
            PyArrow Dataset.
        """
        from brasa.queries import get_dataset

        dataset_name = self.require_param("input")
        return get_dataset(dataset_name)


@ETLStepRegistry.register("filter")
class FilterStep(ETLPipelineStep):
    """Filter rows based on conditions.

    Parameters:
        where: Dictionary of column -> value(s) for equality filtering.
               Can be a single value or list of values (IN clause).
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> ds.Dataset | pd.DataFrame:
        """Filter the data based on the where clause."""
        where = self.require_param("where")
        return shared_transforms.filter_data(data, where)


@ETLStepRegistry.register("select")
class SelectColumnsStep(ETLPipelineStep):
    """Select specific columns from the dataset.

    Parameters:
        columns: List of column names to select.
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> ds.Dataset | pd.DataFrame:
        """Select the specified columns."""
        columns = self.require_param("columns")
        return shared_transforms.select_columns(data, columns)


@ETLStepRegistry.register("sort")
class SortStep(ETLPipelineStep):
    """Sort data by specified columns.

    Parameters:
        by: Column name or list of column names to sort by.
        descending: Whether to sort in descending order (default: False).
                   Can be a single bool or list of bools matching 'by'.
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Sort the data by specified columns."""
        by = self.require_param("by")
        descending = self.get_param("descending", False)
        return shared_transforms.sort_data(data, by, descending)


@ETLStepRegistry.register("to_dataframe")
class ToDataFrameStep(ETLPipelineStep):
    """Convert a PyArrow Dataset/Table to pandas DataFrame.

    This is useful when subsequent steps require DataFrame operations.
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Convert data to DataFrame."""
        return shared_transforms.to_dataframe(data)


@ETLStepRegistry.register("drop_columns")
class DropColumnsStep(ETLPipelineStep):
    """Drop specified columns from the dataset.

    Parameters:
        columns: List of column names to drop.
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Drop the specified columns."""
        columns = self.require_param("columns")
        return shared_transforms.drop_columns(data, columns)


@ETLStepRegistry.register("rename_columns")
class RenameColumnsStep(ETLPipelineStep):
    """Rename columns in the dataset.

    Parameters:
        mapping: Dictionary of old_name -> new_name.
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Rename the columns."""
        mapping = self.require_param("mapping")
        return shared_transforms.rename_columns(data, mapping)


@ETLStepRegistry.register("drop_duplicates")
class DropDuplicatesStep(ETLPipelineStep):
    """Remove duplicate rows.

    Parameters:
        subset: Column names to consider for identifying duplicates (optional).
        keep: Which duplicates to keep ('first', 'last', False).
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Remove duplicates."""
        subset = self.get_param("subset")
        keep = self.get_param("keep", "first")
        return shared_transforms.drop_duplicates(data, subset, keep)


@ETLStepRegistry.register("fill_na")
class FillNAStep(ETLPipelineStep):
    """Fill missing values.

    Parameters:
        value: Value to fill NA with.
        method: Fill method ('ffill', 'bfill').
        columns: Columns to fill (None = all columns).
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Fill NA values."""
        value = self.get_param("value")
        method = self.get_param("method")
        columns = self.get_param("columns")
        return shared_transforms.fill_na(data, value, method, columns)


@ETLStepRegistry.register("future_maturity_to_date")
class FutureMaturity2Date(ETLPipelineStep):
    """Convert future maturity codes to actual dates.

    Parameters:
        code_column: Name of the column with future maturity codes.
        date_column: Name of the output column for the converted dates.
        maturity_day: Day of month for maturity date (default: first day, examples: 15th day, first bizday).
        calendar: Business day calendar to use (default: Actual).
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Convert future maturity codes to dates."""
        code_column = self.require_param("code_column")
        date_column = self.require_param("date_column")
        maturity_day = self.get_param("maturity_day", "first day")
        calendar = self.get_param("calendar", "Actual")
        return shared_transforms.convert_future_maturity_codes_to_dates(
            data, code_column, date_column, maturity_day, calendar
        )


@ETLStepRegistry.register("following_bizday")
class AdjustFollowingBizdays(ETLPipelineStep):
    """Adjust dates to the following business day.

    Parameters:
        date_column: Name of the column with dates to adjust.
        adjusted_column: Name of the output column for adjusted dates.
        calendar: Business day calendar to use (default: Actual).
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Convert future maturity codes to dates."""
        date_column = self.require_param("date_column")
        adjusted_column = self.require_param("adjusted_column")
        calendar = self.get_param("calendar", "Actual")
        return shared_transforms.adjust_following_bizdays(
            data, date_column, adjusted_column, calendar
        )


@ETLStepRegistry.register("bizdays")
class CalculateBizdays(ETLPipelineStep):
    """Calculate business days between two date columns.

    Parameters:
        from_column: Name of the start date column.
        to_column: Name of the end date column.
        output_column: Name of the output column for business day counts.
        calendar: Business day calendar to use (default: Actual).
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Convert future maturity codes to dates."""
        from_column = self.require_param("from_column")
        to_column = self.require_param("to_column")
        output_column = self.require_param("output_column")
        calendar = self.get_param("calendar", "Actual")
        return shared_transforms.calculate_bizdays(
            data, from_column, to_column, output_column, calendar
        )


@ETLStepRegistry.register("implied_rate")
class CalculateImpliedRate(ETLPipelineStep):
    """Calculate implied interest rate from price.

    Parameters:
        price_column: Name of the column with prices.
        rate_column: Name of the output column for implied rates.
        days_to_maturity_column: Name of the column with days to maturity.
        compounding: Compounding regime ('simple', 'compound' or 'discrete', 'continuous').
        forward_price: Forward price used in calculation (default: 100,000).
    """

    def execute(
        self, data: ds.Dataset | pd.DataFrame, _context: ETLPipelineContext
    ) -> pd.DataFrame:
        """Calculate implied interest rates."""
        price_column = self.require_param("price_column")
        rate_column = self.require_param("rate_column")
        days_to_maturity_column = self.require_param("days_to_maturity_column")
        compounding = self.get_param("compounding", "simple")
        forward_price = self.get_param("forward_price", 100_000)
        return shared_transforms.calculate_implied_rate(
            data,
            price_column,
            rate_column,
            days_to_maturity_column,
            compounding,
            forward_price,
        )

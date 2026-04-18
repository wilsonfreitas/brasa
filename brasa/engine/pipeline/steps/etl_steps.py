"""ETL pipeline steps for dataset transformations.

This module provides pipeline steps for ETL operations that transform
datasets into derived datasets.

The ETL steps use shared transformation functions from shared_transforms.py
to enable code reuse between reader and ETL pipelines.

All steps now use the unified StepRegistry and PipelineStep base class,
allowing steps to be shared between reader and ETL pipelines.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import pyarrow
import pyarrow.dataset as ds

from brasa.engine.pipeline.context import PipelineContext

from ..registry import StepRegistry
from ..step import PipelineStep
from . import shared_transforms

# =============================================================================
# Built-in ETL Steps
# =============================================================================


@StepRegistry.register("load")
class LoadDatasetStep(PipelineStep):
    """Load a dataset by name.

    This step loads an existing dataset and returns it as a PyArrow Dataset.
    It must be the first step in a pipeline (or used to load additional datasets).

    Two modes of operation:

    **Mode 1: Template-based loading (recommended)**
        template: Name of the template that created the dataset.
            The layer, dataset name, and partitioning are derived from the template.

    **Mode 2: Explicit loading**
        input: Name of the dataset to load.
        layer: Data layer to load from (e.g., 'input', 'staging', 'curated').
        partitioning: Optional list of column names for partitioning.
            If not provided, uses 'hive' auto-detection.

    Examples:
        # Template-based (derives layer, dataset, partitioning from template)
        - step: load
          template: b3-futures-settlement-prices-consolidated

        # Explicit (specify input and layer directly)
        - step: load
          input: b3-futures-settlement-prices
          layer: staging
          partitioning: [commodity]  # optional
    """

    def execute(self, _data: Any, _context: Any) -> ds.Dataset:
        """Load the specified dataset.

        Args:
            _data: Ignored (this is typically the first step).
            _context: Pipeline context (ETLPipelineContext or compatible).

        Returns:
            PyArrow Dataset.
        """
        from brasa.queries import get_dataset

        template_name = self.params.get("template")

        if template_name:
            # Mode 1: Template-based - derive everything from template
            return get_dataset(template_name)

        # Mode 2: Explicit - require input and layer
        dataset_name = self.require_param("input")
        layer_name = self.require_param("layer")
        partitioning = self.params.get("partitioning", "hive")

        return get_dataset(
            dataset_name,
            layer=layer_name,
            partitioning=partitioning,
        )


@StepRegistry.register("concat_datasets")
class ConcatDatasetsStep(PipelineStep):
    """Concatenate multiple datasets into a single DataFrame.

    This step loads multiple datasets from the same layer and concatenates
    them vertically (stacking rows). All datasets must have compatible schemas.

    Parameters:
        inputs: List of dataset names to concatenate.
        layer: Data layer to load datasets from
               (e.g., 'input', 'staging', 'curated').
        columns: Optional list of column names to select from each
                dataset before concatenating. If not provided, all
                columns are included (schemas must match).

    Examples:
        # Concatenate all columns from multiple datasets
        - step: concat_datasets
          inputs:
            - dataset-a
            - dataset-b
            - dataset-c
          layer: input

        # Concatenate only specific columns
        - step: concat_datasets
          inputs:
            - dataset-a
            - dataset-b
          layer: staging
          columns: [refdate, symbol, value]
    """

    def execute(self, _data: Any, _context: Any) -> pd.DataFrame:
        """Concatenate the specified datasets.

        Args:
            _data: Ignored (this step loads data independently).
            _context: Pipeline context.

        Returns:
            Concatenated pandas DataFrame.
        """
        from brasa.queries import get_dataset

        inputs = self.require_param("inputs")
        layer = self.require_param("layer")
        columns = self.get_param("columns")

        if not inputs:
            raise ValueError("concat_datasets requires at least one input dataset")

        tables = []
        for dataset_name in inputs:
            if columns:
                tb = (
                    get_dataset(dataset_name, layer=layer)
                    .scanner(columns=columns)
                    .to_table()
                )
            else:
                tb = get_dataset(dataset_name, layer=layer).to_table()
            tables.append(tb)

        concatenated = pyarrow.concat_tables(tables)
        return concatenated.to_pandas()

    def get_input_datasets(self) -> list[str]:
        """Get the list of input dataset names from the 'inputs' parameter.

        Returns:
            List of dataset names that are inputs to this step.
        """
        return self.params.get("inputs", [])


@StepRegistry.register("dataset_filter")
class DatasetFilterStep(PipelineStep):
    """Filter rows based on conditions (supports PyArrow Dataset and DataFrame).

    Parameters:
        where: Dictionary of column -> value(s) for equality filtering.
               Can be a single value or list of values (IN clause).
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Filter the data based on the where clause."""
        where = self.require_param("where")
        return shared_transforms.filter_data(data, where)


@StepRegistry.register("dataset_select")
class DatasetSelectColumnsStep(PipelineStep):
    """Select specific columns from the dataset (supports PyArrow Dataset and DataFrame).

    Parameters:
        columns: List of column names to select.
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Select the specified columns."""
        columns = self.require_param("columns")
        return shared_transforms.select_columns(data, columns)


@StepRegistry.register("select_fields")
class DatasetSelectFieldsStep(PipelineStep):
    """Select specific columns from the dataset (supports PyArrow Dataset and DataFrame) based on field names.

    This step is similar to 'dataset_select' but is designed to work with field names in PyArrow Datasets,
    which may differ from column names in DataFrames. It ensures compatibility when working directly with PyArrow Datasets."""

    def execute(
        self, data: ds.Dataset | pd.DataFrame, context: PipelineContext
    ) -> pd.DataFrame:
        """Select the specified columns."""
        if context.fields is None:
            raise ValueError(
                "PipelineContext must have 'fields' defined for select_fields step."
            )

        columns = context.fields.get_field_names()
        return shared_transforms.select_columns(data, columns)


@StepRegistry.register("dataset_sort")
class DatasetSortStep(PipelineStep):
    """Sort data by specified columns (supports PyArrow Dataset and DataFrame).

    Parameters:
        by: Column name or list of column names to sort by.
        descending: Whether to sort in descending order (default: False).
                   Can be a single bool or list of bools matching 'by'.
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Sort the data by specified columns."""
        by = self.require_param("by")
        descending = self.get_param("descending", False)
        return shared_transforms.sort_data(data, by, descending)


@StepRegistry.register("to_dataframe")
class ToDataFrameStep(PipelineStep):
    """Convert a PyArrow Dataset/Table to pandas DataFrame.

    This is useful when subsequent steps require DataFrame operations.
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Convert data to DataFrame."""
        return shared_transforms.to_dataframe(data)


@StepRegistry.register("dataset_drop_columns")
class DatasetDropColumnsStep(PipelineStep):
    """Drop specified columns from the dataset (supports PyArrow Dataset and DataFrame).

    Parameters:
        columns: List of column names to drop.
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Drop the specified columns."""
        columns = self.require_param("columns")
        return shared_transforms.drop_columns(data, columns)


@StepRegistry.register("dataset_rename_columns")
class DatasetRenameColumnsStep(PipelineStep):
    """Rename columns in the dataset (supports PyArrow Dataset and DataFrame).

    Parameters:
        mapping: Dictionary of old_name -> new_name.
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Rename the columns."""
        mapping = self.require_param("mapping")
        return shared_transforms.rename_columns(data, mapping)


@StepRegistry.register("dataset_drop_duplicates")
class DatasetDropDuplicatesStep(PipelineStep):
    """Remove duplicate rows (supports PyArrow Dataset and DataFrame).

    Parameters:
        subset: Column names to consider for identifying duplicates (optional).
        keep: Which duplicates to keep ('first', 'last', False).
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Remove duplicates."""
        subset = self.get_param("subset")
        keep = self.get_param("keep", "first")
        return shared_transforms.drop_duplicates(data, subset, keep)


@StepRegistry.register("dataset_fill_na")
class DatasetFillNAStep(PipelineStep):
    """Fill missing values (supports PyArrow Dataset and DataFrame).

    Parameters:
        value: Value to fill NA with.
        method: Fill method ('ffill', 'bfill').
        columns: Columns to fill (None = all columns).
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Fill NA values."""
        value = self.get_param("value")
        method = self.get_param("method")
        columns = self.get_param("columns")
        return shared_transforms.fill_na(data, value, method, columns)


@StepRegistry.register("future_maturity_to_date")
class FutureMaturity2Date(PipelineStep):
    """Convert future maturity codes to actual dates.

    Parameters:
        code_column: Name of the column with future maturity codes.
        date_column: Name of the output column for the converted dates.
        maturity_day: Day of month for maturity date (default: first day, examples: 15th day, first bizday).
        calendar: Business day calendar to use (default: Actual).
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Convert future maturity codes to dates."""
        code_column = self.require_param("code_column")
        date_column = self.require_param("date_column")
        maturity_day = self.get_param("maturity_day", "first day")
        calendar = self.get_param("calendar", "Actual")
        return shared_transforms.convert_future_maturity_codes_to_dates(
            data, code_column, date_column, maturity_day, calendar
        )


@StepRegistry.register("following_bizday")
class AdjustFollowingBizdays(PipelineStep):
    """Adjust dates to the following business day.

    Parameters:
        date_column: Name of the column with dates to adjust.
        adjusted_column: Name of the output column for adjusted dates.
        calendar: Business day calendar to use (default: Actual).
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Convert future maturity codes to dates."""
        date_column = self.require_param("date_column")
        adjusted_column = self.require_param("adjusted_column")
        calendar = self.get_param("calendar", "Actual")
        return shared_transforms.adjust_following_bizdays(
            data, date_column, adjusted_column, calendar
        )


@StepRegistry.register("bizdays")
class CalculateBizdays(PipelineStep):
    """Calculate business days between two date columns.

    Parameters:
        from_column: Name of the start date column.
        to_column: Name of the end date column.
        output_column: Name of the output column for business day counts.
        calendar: Business day calendar to use (default: Actual).
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Convert future maturity codes to dates."""
        from_column = self.require_param("from_column")
        to_column = self.require_param("to_column")
        output_column = self.require_param("output_column")
        calendar = self.get_param("calendar", "Actual")
        return shared_transforms.calculate_bizdays(
            data, from_column, to_column, output_column, calendar
        )


@StepRegistry.register("implied_rate")
class CalculateImpliedRate(PipelineStep):
    """Calculate implied interest rate from price.

    Parameters:
        price_column: Name of the column with prices.
        rate_column: Name of the output column for implied rates.
        days_to_maturity_column: Name of the column with days to maturity.
        compounding: Compounding regime ('simple', 'compound' or 'discrete', 'continuous').
        forward_price: Forward price used in calculation (default: 100,000).
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
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


@StepRegistry.register("flatten_columns")
class FlattenStep(PipelineStep):
    """Flatten columns by splitting delimited values into separate rows.

    This step is useful for exploding columns that contain multiple values
    separated by a delimiter (e.g., comma-separated index names).

    Parameters:
        columns: List of column names to flatten.
        separator: The delimiter used to split values (default: ",").

    Examples:
        # Flatten a column with comma-separated values
        - step: flatten_columns
          columns:
            - indexes

        # Flatten with a custom separator
        - step: flatten_columns
          columns:
            - tags
          separator: "|"
    """

    def execute(self, data: ds.Dataset | pd.DataFrame, _context: Any) -> pd.DataFrame:
        """Flatten the specified columns."""
        columns = self.require_param("columns")
        separator = self.get_param("separator", ",")
        return shared_transforms.flatten_column(data, columns, separator)


@StepRegistry.register("sql_query")
class RunQueryStep(PipelineStep):
    """Execute SQL query on datasets in an in-memory DuckDB database.

    This step creates a temporary `:memory:` DuckDB connection, registers
    the specified datasets as views using their full names, executes the
    provided SQL query, and returns the result as a pandas DataFrame.

    Parameters:
        datasets: List of input dataset names to load and register as views.
        query: SQL query string (can be multi-line) to execute on the datasets.
               Dataset names in the query should match those in the 'datasets' list.

    Returns:
        pandas DataFrame containing the query results.

    Examples:
        # Simple join and filter
        - step: run_query
          datasets:
            - input.cvm-companies-registration
            - input.b3-company-details
          query: |
            SELECT
              DISTINCT cvm.code_cvm,
              cvm.denom_social as company_name,
              b3.issuingCompany as asset_name
            FROM 'input.cvm-companies-registration' cvm
            LEFT JOIN 'input.b3-company-details' b3
              ON cvm.code_cvm = CAST(b3.codeCVM AS VARCHAR)
            WHERE cvm.tp_merc = 'BOLSA'
            ORDER BY cvm.denom_social
    """

    def execute(self, _data: Any, _context: Any) -> pd.DataFrame:
        """Execute SQL query on datasets in an in-memory DuckDB database.

        Args:
            _data: Ignored (datasets are loaded explicitly).
            _context: Pipeline context.

        Returns:
            Query results as a pandas DataFrame.

        Raises:
            ValueError: If required parameters are missing.
            RuntimeError: If query execution fails.
        """
        import duckdb

        from brasa.queries import get_dataset

        datasets = self.require_param("datasets")
        query = self.require_param("query")

        if not datasets:
            raise ValueError("run_query requires a non-empty 'datasets' list")
        if not query or not query.strip():
            raise ValueError("run_query requires a non-empty 'query' string")

        # Create an in-memory DuckDB connection
        conn = duckdb.connect(":memory:")

        try:
            # Register each dataset as a view using its full name
            for dataset_name in datasets:
                # Load the dataset using brasa's dataset loader.
                # Dataset references may come as "<layer>.<dataset-name>".
                if "." in dataset_name:
                    layer_name, base_dataset_name = dataset_name.split(".", 1)
                    dataset = get_dataset(
                        base_dataset_name,
                        layer=layer_name,
                        use_template_schema=False,
                        use_catalog_schema=True,
                    )
                else:
                    dataset = get_dataset(dataset_name)

                # Convert PyArrow Dataset to Table and register with DuckDB.
                # DuckDB natively supports PyArrow Tables, avoiding pandas overhead.
                # table = dataset.to_table()
                conn.register(dataset_name, dataset)

            # Execute the query and return results as a DataFrame
            result = conn.execute(query).fetch_df()

            return result

        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e!s}") from e
        finally:
            conn.close()

    def get_input_datasets(self) -> list[str]:
        """Get the list of input dataset names from the 'datasets' parameter.

        Returns:
            List of dataset names that are inputs to this step.
        """
        return self.params.get("datasets", [])


# =============================================================================
# Backward Compatibility Aliases
# =============================================================================
# Register the dataset steps with their original names for backward compatibility
# with existing YAML templates. Steps like sort, fill_na, rename_columns,
# drop_columns, select_columns are now defined in transform_steps.py and
# column_steps.py using shared_transforms, so they work with both DataFrames
# and Datasets.

# These register the dataset-specific filter step under the simpler name
StepRegistry.register("filter")(DatasetFilterStep)
StepRegistry.register("select")(DatasetSelectColumnsStep)

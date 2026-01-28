"""Custom function pipeline steps.

Steps that allow running custom Python functions within the pipeline.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pandas as pd

from ..context import PipelineContext
from ..registry import StepRegistry
from ..step import PipelineStep


def _load_function(func_name: str):
    """Dynamically load a function by its fully qualified name."""
    module_name, fn_name = func_name.rsplit(".", 1)
    module = __import__(module_name, fromlist=[fn_name])
    return getattr(module, fn_name)


@StepRegistry.register("custom")
class CustomStep(PipelineStep):
    """Execute a custom function.

    The function receives the current data and context, and should return
    the transformed data.

    Parameters:
        function: Fully qualified function name (e.g., 'mymodule.my_function')

    The function signature should be:
        def my_function(data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
            ...
    """

    def execute(self, data: Any, context: PipelineContext) -> Any:
        func_name = self.require_param("function")
        func = _load_function(func_name)
        return func(data, context)


@StepRegistry.register("custom_simple")
class CustomSimpleStep(PipelineStep):
    """Execute a simple custom function that only receives data.

    Parameters:
        function: Fully qualified function name (e.g., 'mymodule.my_function')

    The function signature should be:
        def my_function(data: pd.DataFrame) -> pd.DataFrame:
            ...
    """

    def execute(self, data: Any, _context: PipelineContext) -> Any:
        func_name = self.require_param("function")
        func = _load_function(func_name)
        return func(data)


@StepRegistry.register("legacy_reader")
class LegacyReaderStep(PipelineStep):
    """Execute a legacy reader function that uses CacheMetadata.

    This step provides backward compatibility with existing reader functions
    that follow the old signature: func(meta: CacheMetadata) -> pd.DataFrame

    Parameters:
        function: Fully qualified function name (e.g., 'brasa.readers.read_xxx')
    """

    def execute(self, _data: Any, context: PipelineContext) -> pd.DataFrame:
        func_name = self.require_param("function")
        func = _load_function(func_name)
        return func(context.meta)


@StepRegistry.register("apply_lambda")
class ApplyLambdaStep(PipelineStep):
    """Apply a lambda expression to each row or column.

    Parameters:
        column: Column to apply the lambda to
        expression: Python expression using 'x' as the value
        output: Output column name (default: same as input)
        axis: 0 for columns, 1 for rows (default: 0)

    Example:
        - step: apply_lambda
          column: price
          expression: "x * 100"
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        column = self.require_param("column")
        expression = self.require_param("expression")
        output = self.get_param("output", column)

        # Create a lambda function from the expression
        # Note: Using eval here is intentional for flexibility
        func = eval(f"lambda x: {expression}")

        data[output] = data[column].apply(func)
        return data


@StepRegistry.register("exec_code")
class ExecCodeStep(PipelineStep):
    """Execute arbitrary Python code with access to data and context.

    WARNING: Only use this for trusted code. The code has full access
    to the Python environment.

    Parameters:
        code: Python code to execute. Has access to 'data' (DataFrame),
              'context' (PipelineContext), and 'pd' (pandas module).
              Must assign the result to 'result'.

    Example:
        - step: exec_code
          code: |
            data['new_col'] = data['col1'] + data['col2']
            result = data
    """

    def execute(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        code = self.require_param("code")

        # Set up the execution namespace
        namespace = {
            "data": data,
            "context": context,
            "pd": pd,
            "result": None,
        }

        exec(code, namespace)

        result = namespace.get("result")
        if result is None:
            raise ValueError("exec_code step must assign a value to 'result'")

        return result


@StepRegistry.register("store_result")
class StoreResultStep(PipelineStep):
    """Store the current data in the pipeline context.

    Parameters:
        name: Key to store the current data under.
    """

    def execute(self, data: Any, context: PipelineContext) -> Any:
        name = self.require_param("name")
        context.store_result(name, data)
        return data


@StepRegistry.register("load_result")
class LoadResultStep(PipelineStep):
    """Load a stored result from the pipeline context.

    Parameters:
        name: Key to retrieve from context.
    """

    def execute(self, _data: Any, context: PipelineContext) -> Any:
        name = self.require_param("name")
        value = context.get_result(name)
        if value is None:
            raise ValueError(f"No stored result for key '{name}'")
        return value


@StepRegistry.register("legacy_etl_output")
class LegacyETLOutputStep(PipelineStep):
    """Run a legacy ETL function and return its output dataset.

    This step executes the legacy ETL function using a lightweight handler,
    then loads the generated dataset and returns it as a DataFrame.

    Parameters:
        function: Fully qualified ETL function name.
        args: Mapping of ETL function arguments.
    """

    def execute(self, _data: Any, context: PipelineContext) -> pd.DataFrame:
        from brasa.queries import get_dataset

        function = self.require_param("function")
        args = self.get_param("args", {})

        handler = SimpleNamespace(template_id=context.template_id, **args)
        func = _load_function(function)
        func(handler)

        dataset = get_dataset(context.template_id)
        return dataset.to_table().to_pandas()

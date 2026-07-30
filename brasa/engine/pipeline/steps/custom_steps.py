"""Custom function pipeline steps.

Steps that allow running custom Python functions within the pipeline.
"""

from __future__ import annotations

import pandas as pd

from ..context import PipelineContext
from ..registry import StepRegistry
from ..step import PipelineStep


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

"""HTML-related pipeline steps.

Steps for reading and processing HTML content.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..context import PipelineContext
from ..registry import StepRegistry
from ..step import PipelineStep


@StepRegistry.register("read_html")
class ReadHtmlStep(PipelineStep):
    """Read HTML tables into a list of DataFrames.

    Parameters:
        attrs: Dictionary of HTML attributes to match (e.g., {"id": "table1"})
        match: Regular expression to match table text (optional)
        flavor: Parser to use ('lxml', 'bs4', etc.)
    """

    def execute(self, _data: Any, context: PipelineContext) -> list[pd.DataFrame]:
        filepath = context.downloaded_file

        attrs = self.get_param("attrs")
        match = self.get_param("match")
        flavor = self.get_param("flavor")

        kwargs: dict[str, Any] = {
            "encoding": context.encoding,
            "decimal": context.decimal,
            "thousands": context.thousands,
        }

        if attrs:
            kwargs["attrs"] = attrs
        if match:
            kwargs["match"] = match
        if flavor:
            kwargs["flavor"] = flavor

        return pd.read_html(filepath, **kwargs)


@StepRegistry.register("first_table")
class FirstTableStep(PipelineStep):
    """Select the first table from a list of DataFrames.

    Convenience step equivalent to select_table with index=0.
    """

    def execute(
        self, data: list[pd.DataFrame], _context: PipelineContext
    ) -> pd.DataFrame:
        if not isinstance(data, list):
            raise TypeError(f"Expected list of DataFrames, got {type(data)}")

        if len(data) == 0:
            raise ValueError("No tables found in HTML")

        return data[0]

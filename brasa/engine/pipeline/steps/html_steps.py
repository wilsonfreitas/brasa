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


@StepRegistry.register("select_table")
class SelectTableStep(PipelineStep):
    """Select a single table from a list of DataFrames.

    Parameters:
        index: Index of the table to select (default: 0)
    """

    def execute(
        self, data: list[pd.DataFrame], _context: PipelineContext
    ) -> pd.DataFrame:
        index = self.get_param("index", 0)

        if not isinstance(data, list):
            raise TypeError(f"Expected list of DataFrames, got {type(data)}")

        if index >= len(data):
            raise IndexError(
                f"Table index {index} out of range (only {len(data)} tables found)"
            )

        return data[index]


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


@StepRegistry.register("parse_html_element")
class ParseHtmlElementStep(PipelineStep):
    """Parse an HTML element using XPath and store in context.

    Parameters:
        xpath: XPath expression to find the element
        attribute: Element attribute to extract (default: 'value')
        store_as: Name to store the result under in context
    """

    def execute(self, data: Any, context: PipelineContext) -> Any:
        from lxml import etree

        filepath = context.downloaded_file
        xpath = self.require_param("xpath")
        attribute = self.get_param("attribute", "value")
        store_as = self.get_param("store_as")

        tree = etree.parse(filepath, etree.HTMLParser())
        elements = tree.xpath(xpath)

        if elements:
            value = elements[0].attrib.get(attribute) if attribute else elements[0].text
        else:
            value = None

        if store_as:
            context.store_result(store_as, value)

        return data

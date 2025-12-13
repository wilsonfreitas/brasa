"""B3-specific pipeline steps.

Custom steps for processing B3 (Brazilian Stock Exchange) data.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from lxml import etree

from brasa.engine.pipeline.context import PipelineContext
from brasa.engine.pipeline.registry import StepRegistry
from brasa.engine.pipeline.step import PipelineStep


@StepRegistry.register("b3_parse_refdate_from_html")
class B3ParseRefdateFromHtmlStep(PipelineStep):
    """Parse the reference date from B3 HTML page.

    Extracts the date from an input element in the HTML file and stores
    it in the context for later use.

    Parameters:
        xpath: XPath expression to find the date element (default: "//input[@id='dData1']")
        attribute: Attribute containing the date (default: 'value')
        store_as: Name to store the result under in context (default: 'refdate')
    """

    def execute(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        filepath = context.downloaded_file
        xpath = self.get_param("xpath", "//input[@id='dData1']")
        attribute = self.get_param("attribute", "value")
        store_as = self.get_param("store_as", "refdate")

        tree = etree.parse(filepath, etree.HTMLParser())
        elements = tree.xpath(xpath)

        if elements:
            value = elements[0].attrib.get(attribute)
            context.store_result(store_as, value)

        return data


@StepRegistry.register("b3_forward_fill_commodity")
class B3ForwardFillCommodityStep(PipelineStep):
    """Forward fill commodity names in B3 settlement prices table.

    In the B3 settlement prices table, commodity names appear only in the
    first row of each group. This step forward-fills the values.

    Parameters:
        column: Column name to forward fill (default: 'commodity')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        column = self.get_param("column", "commodity")

        for ix in range(data.shape[0]):
            if data.loc[ix, column] is not np.nan and pd.notna(data.loc[ix, column]):
                last_name = data.loc[ix, column]
            data.loc[ix, column] = last_name

        return data


@StepRegistry.register("b3_extract_commodity_code")
class B3ExtractCommodityCodeStep(PipelineStep):
    """Extract the commodity code from the commodity name.

    The commodity name typically starts with the code (e.g., "DOL - Dólar").
    This step extracts just the code part.

    Parameters:
        column: Column to process (default: 'commodity')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        column = self.get_param("column", "commodity")
        data[column] = data[column].str.extract(r"^(\w+)")[0]
        return data


@StepRegistry.register("b3_create_symbol")
class B3CreateSymbolStep(PipelineStep):
    """Create the futures symbol by concatenating commodity and maturity code.

    Parameters:
        commodity_column: Column with commodity code (default: 'commodity')
        maturity_column: Column with maturity code (default: 'maturity_code')
        output_column: Output column name (default: 'symbol')
    """

    def execute(self, data: pd.DataFrame, _context: PipelineContext) -> pd.DataFrame:
        commodity_col = self.get_param("commodity_column", "commodity")
        maturity_col = self.get_param("maturity_column", "maturity_code")
        output_col = self.get_param("output_column", "symbol")

        data[output_col] = data[commodity_col] + data[maturity_col]
        return data

"""B3-specific pipeline steps.

Custom steps for processing B3 (Brazilian Stock Exchange) data.
"""

from __future__ import annotations

import gzip
import logging
from typing import Any

import numpy as np
import pandas as pd
from lxml import etree

from brasa.engine.pipeline.context import PipelineContext
from brasa.engine.pipeline.registry import StepRegistry
from brasa.engine.pipeline.step import PipelineStep

logger = logging.getLogger(__name__)


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


@StepRegistry.register("b3_read_bvbg086_xml")
class B3ReadBVBG086XmlStep(PipelineStep):
    """Read and parse B3 BVBG086 gzipped XML file.

    Extracts price report data from the BVBG086 XML format using XPath
    based on field tags defined in the template.

    The BVBG086 file contains market prices for various financial instruments
    traded on B3. Each field in the template should have a 'tag' attribute
    specifying the XML path to extract the value from.

    Parameters:
        None (uses field tags from context.fields)
    """

    def execute(self, _data: Any, context: PipelineContext) -> pd.DataFrame:
        filepath = context.downloaded_file
        logger.debug(f"Reading BVBG086 XML file: {filepath}")

        # Parse the gzipped XML
        with gzip.open(filepath) as f:
            tree = etree.parse(f)

        exchange = tree.getroot()[0][0]
        ns_bvmf052 = {None: "urn:bvmf.052.01.xsd"}

        # Extract creation date from BizGrpDtls
        td_xpath = etree.ETXPath("//{urn:bvmf.052.01.xsd}BizGrpDtls")
        td = td_xpath(exchange)
        if len(td) == 0:
            logger.error("Invalid XML: tag BizGrpDtls not found")
            raise ValueError("Invalid XML: tag BizGrpDtls not found")

        creation_date = td[0].find("CreDtAndTm", ns_bvmf052).text[:10]
        logger.debug(f"Creation date extracted: {creation_date}")

        # Find all price report nodes
        price_reports = exchange.findall(
            "{urn:bvmf.052.01.xsd}BizGrp/"
            "{urn:bvmf.217.01.xsd}Document/"
            "{urn:bvmf.217.01.xsd}PricRpt"
        )
        logger.debug(f"Found {len(price_reports)} price report nodes")

        # Build tag mapping from fields
        tags: dict[str, str] = {}
        if context.fields:
            for field in context.fields:
                tag = field.get_attribute("tag")
                if tag:
                    tags[field.name] = tag
            logger.debug(f"Field tags mapping: {list(tags.keys())}")
        else:
            logger.warning("No fields defined in context, DataFrame will be empty")

        # Parse each price report node
        instruments: list[dict[str, Any]] = []
        ns_bvmf217 = {None: "urn:bvmf.217.01.xsd"}

        for node in price_reports:
            data: dict[str, Any] = {}
            for field_name, tag_path in tags.items():
                elements = node.findall(tag_path, ns_bvmf217)
                data[field_name] = elements[0].text if elements else None
            instruments.append(data)

        logger.info(f"Parsed {len(instruments)} instruments from BVBG086")
        return pd.DataFrame(instruments)


@StepRegistry.register("b3_read_bvbg087_xml")
class B3ReadBVBG087XmlStep(PipelineStep):
    """Read and parse B3 BVBG087 gzipped XML file.

    Parses the file ONCE and returns Dict[str, DataFrame] for each
    index type. The output keys are the dataset output names (e.g., 'indexes_info')
    rather than the XML tags (e.g., 'IndxInf').

    The BVBG087 file contains index information including:
    - IndxInf: Market indexes (IBOV, IFIX, etc.)
    - IOPVInf: Indicative Optimized Portfolio Value for ETFs
    - BDRInf: BDR reference prices

    Each field in the dataset fieldset should have a 'tag' attribute
    specifying the XML path to extract the value from.

    Parameters:
        None (uses datasets configuration from context)
    """

    def _smart_find(self, node, xpath: str, ns: dict) -> str | None:
        """Safely find an element and return its text."""
        try:
            element = node.find(xpath, ns)
            return element.text if element is not None else None
        except Exception:
            return None

    def _build_field_tags(self, fieldset) -> dict[str, str]:
        """Build field name to XML tag mapping from fieldset."""
        tags: dict[str, str] = {}
        if fieldset:
            for field in fieldset:
                tag = field.get_attribute("tag")
                if tag:
                    tags[field.name] = tag
        return tags

    def execute(self, _data: Any, context: PipelineContext) -> dict[str, pd.DataFrame]:
        filepath = context.downloaded_file
        logger.debug(f"Reading BVBG087 XML file: {filepath}")

        # Parse the gzipped XML
        with gzip.open(filepath) as f:
            tree = etree.parse(f)

        ns = {None: "urn:bvmf.218.01.xsd"}
        exchange = tree.getroot()[0][0]

        # Extract trade date
        td_xpath = etree.ETXPath("//{urn:bvmf.218.01.xsd}TradDt")
        td = td_xpath(exchange)
        if len(td) == 0:
            logger.error("Invalid XML: tag TradDt not found")
            raise ValueError("Invalid XML: tag TradDt not found")

        trade_date = td[0].find("Dt", ns).text
        logger.debug(f"Trade date extracted: {trade_date}")

        # Parse each dataset from context
        results: dict[str, pd.DataFrame] = {}

        if not context.datasets:
            logger.warning("No datasets defined in context, returning empty results")
            return results

        for output_name, dataset_config in context.datasets.items():
            xml_tag = dataset_config.tag
            fieldset = dataset_config.fields

            # Build field tag mapping from fieldset
            field_tags = self._build_field_tags(fieldset)
            logger.debug(f"Field tags for {output_name}: {list(field_tags.keys())}")

            records: list[dict[str, Any]] = []
            _xpath = etree.ETXPath(f"//{{urn:bvmf.218.01.xsd}}{xml_tag}")

            for node in _xpath(exchange):
                data: dict[str, Any] = {
                    "refdate": trade_date,
                }
                for field_name, xpath in field_tags.items():
                    data[field_name] = self._smart_find(node, xpath, ns)
                records.append(data)

            results[output_name] = pd.DataFrame(records)
            logger.debug(
                f"Parsed {len(records)} records for {output_name} (tag: {xml_tag})"
            )

        logger.info(f"Parsed BVBG087 with {len(results)} datasets")
        return results


@StepRegistry.register("b3_add_columns_from_json_fields")
class B3AddColumnsFromJsonFieldsStep(PipelineStep):
    """Parse B3's JSON fields and add as columns.

    Parameters:
        mapping: Mapping of column names to JSON paths
    """

    def execute(self, data: pd.DataFrame, context: PipelineContext) -> pd.DataFrame:
        import gzip
        import json
        from pathlib import Path

        filepath = context.downloaded_file
        map_param = self.require_param("mapping")

        if str(filepath).endswith(".gz"):
            with gzip.open(filepath, "rt", encoding=context.encoding) as f:
                json_data = json.load(f)
        else:
            with Path(filepath).open(encoding=context.encoding) as f:
                json_data = json.load(f)

        for store_as, json_path in map_param.items():
            json_data_copy = json_data  # Use a copy to traverse
            for key in json_path.split("."):
                if isinstance(json_data_copy, dict):
                    json_data_copy = json_data_copy[key]
                elif isinstance(json_data_copy, list) and key.isdigit():
                    json_data_copy = json_data_copy[int(key)]
            data[store_as] = json_data_copy

        return data

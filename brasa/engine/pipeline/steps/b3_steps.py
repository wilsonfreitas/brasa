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


@StepRegistry.register("b3_read_bvbg028_xml")
class B3ReadBVBG028XmlStep(PipelineStep):
    """Read and parse B3 BVBG028 gzipped XML file.

    Parses the file ONCE and returns Dict[str, DataFrame] for each
    instrument type. The output keys are the dataset output names (e.g., 'equities')
    rather than the XML tags (e.g., 'EqtyInf').

    The BVBG028 file contains market prices information including:
    - EqtyInf: Equities information
    - OptnOnEqtsInf: Options on equities information
    - FutrCtrctsInf: Future contracts information

    Each field in the dataset fieldset should have a 'tag' attribute
    specifying the XML path to extract the value from.

    Parameters:
        None (uses datasets configuration from context)
    """

    NS_052 = "urn:bvmf.052.01.xsd"
    NS_100 = "urn:bvmf.100.02.xsd"

    def _smart_find(self, node, xpath: str, ns: dict) -> str | None:
        """Safely find an element and return its text."""
        try:
            element = node.find(xpath, ns)
            return (
                element.text.strip() if element is not None and element.text else None
            )
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

    def _get_instrument_type(self, node, ns: dict) -> str | None:
        """Get the instrument type from the InstrmInf child tag."""
        instrm_inf = node.find("InstrmInf", ns)
        if instrm_inf is not None and len(instrm_inf) > 0:
            # Get the first child tag name, removing namespace
            child = instrm_inf[0]
            # Extract tag name without namespace (format: {namespace}tagname)
            tag = child.tag
            if "}" in tag:
                return tag.split("}")[1]
            return tag
        return None

    def execute(self, _data: Any, context: PipelineContext) -> dict[str, pd.DataFrame]:
        filepath = context.downloaded_file
        logger.debug(f"Reading BVBG028 XML file: {filepath}")

        # Parse the gzipped XML
        with gzip.open(filepath) as f:
            tree = etree.parse(f)

        exchange = tree.getroot()[0][0]
        ns_052 = {None: self.NS_052}
        ns_100 = {None: self.NS_100}

        # Extract creation date from BizGrpDtls
        td_xpath = etree.ETXPath(f"//{{{self.NS_052}}}BizGrpDtls")
        td = td_xpath(exchange)
        if len(td) == 0:
            logger.error("Invalid XML: tag BizGrpDtls not found")
            raise ValueError("Invalid XML: tag BizGrpDtls not found")

        creation_date = td[0].find("CreDtAndTm", ns_052).text[:10]
        logger.debug(f"Creation date extracted: {creation_date}")

        # Build a mapping from XML tag to (output_name, field_tags)
        tag_to_dataset: dict[str, tuple[str, dict[str, str]]] = {}
        if context.datasets:
            for output_name, dataset_config in context.datasets.items():
                xml_tag = dataset_config.tag
                fieldset = dataset_config.fields
                field_tags = self._build_field_tags(fieldset)
                tag_to_dataset[xml_tag] = (output_name, field_tags)
                logger.debug(
                    f"Mapped {xml_tag} -> {output_name} with {len(field_tags)} fields"
                )

        # Initialize results dict with empty lists for each dataset
        results: dict[str, list[dict[str, Any]]] = {
            output_name: [] for output_name, _ in tag_to_dataset.values()
        }

        # Find all instrument nodes
        instrm_nodes = exchange.findall(
            f"{{{self.NS_052}}}BizGrp/{{{self.NS_100}}}Document/{{{self.NS_100}}}Instrm"
        )
        logger.debug(f"Found {len(instrm_nodes)} instrument nodes")

        # Parse each instrument node
        for node in instrm_nodes:
            instr_type = self._get_instrument_type(node, ns_100)
            if instr_type is None or instr_type not in tag_to_dataset:
                continue

            output_name, field_tags = tag_to_dataset[instr_type]
            data: dict[str, Any] = {
                "creation_date": creation_date,
            }

            # Extract fields based on tag mapping
            for field_name, xpath in field_tags.items():
                if field_name == "refdate":
                    # refdate is extracted from RptParams/RptDtAndTm/Dt
                    data[field_name] = self._smart_find(node, xpath, ns_100)
                else:
                    data[field_name] = self._smart_find(node, xpath, ns_100)

            results[output_name].append(data)

        # Convert lists to DataFrames
        df_results: dict[str, pd.DataFrame] = {}
        for output_name, records in results.items():
            df_results[output_name] = pd.DataFrame(records)
            logger.debug(f"Parsed {len(records)} records for {output_name}")

        logger.info(f"Parsed BVBG028 with {len(df_results)} datasets")
        return df_results


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


@StepRegistry.register("b3_read_company_info_json")
class B3ReadCompanyInfoJsonStep(PipelineStep):
    """Read and parse B3 company info gzipped JSON file.

    Parses the file and returns Dict[str, DataFrame] for each
    dataset type. The output keys are the dataset output names.

    The company info file contains:
    - info: Main company information
    - cash_dividends: Cash dividend events
    - stock_dividends: Stock dividend events
    - subscriptions: Subscription events

    Parameters:
        None (uses datasets configuration from context)
    """

    def execute(self, _data: Any, context: PipelineContext) -> dict[str, pd.DataFrame]:
        import json

        filepath = context.downloaded_file
        logger.debug(f"Reading B3 company info JSON file: {filepath}")

        # Parse the gzipped JSON
        with gzip.open(filepath) as f:
            obj = json.load(f)

        results: dict[str, pd.DataFrame] = {}

        if not context.datasets:
            logger.warning("No datasets defined in context, returning empty results")
            return results

        # Nested array keys to drop from main info
        nested_keys = {"cashDividends", "stockDividends", "subscriptions"}

        for output_name, dataset_config in context.datasets.items():
            json_key = dataset_config.tag

            if json_key is None:
                # Main info dataset - use root object minus nested arrays
                df = pd.DataFrame(obj)
                cols_to_drop = [col for col in nested_keys if col in df.columns]
                df = df.drop(columns=cols_to_drop, errors="ignore")
            else:
                # Nested array dataset
                nested_data = obj[0].get(json_key, [])
                df = pd.DataFrame(nested_data)

            results[output_name] = df
            logger.debug(f"Parsed {len(df)} records for {output_name}")

        logger.info(f"Parsed company info with {len(results)} datasets")
        return results


@StepRegistry.register("b3_read_company_details_json")
class B3ReadCompanyDetailsJsonStep(PipelineStep):
    """Read and parse B3 company details gzipped JSON file.

    Reads the JSON and expands the otherCodes nested array by duplicating
    rows. Each row gets a code and isin from the otherCodes array.

    Parameters:
        None
    """

    def execute(self, _data: Any, context: PipelineContext) -> pd.DataFrame:
        import json

        filepath = context.downloaded_file
        logger.debug(f"Reading B3 company details JSON file: {filepath}")

        # Parse the gzipped JSON
        with gzip.open(filepath) as f:
            obj = json.load(f)

        # Create DataFrame from JSON
        df = pd.DataFrame(obj) if isinstance(obj, list) else pd.DataFrame([obj])

        # Expand otherCodes: duplicate rows for each code/isin pair
        if "otherCodes" in df.columns and df["otherCodes"].iloc[0] is not None:
            other_codes = df["otherCodes"].iloc[0]
            codes = [d["code"] for d in other_codes]
            isins = [d["isin"] for d in other_codes]
            df = pd.concat([df] * len(codes), ignore_index=True)
            df["code"] = codes
            df["isin"] = isins
        else:
            df["code"] = np.nan
            df["isin"] = np.nan

        # Drop the nested otherCodes column
        df = df.drop(columns=["otherCodes"], errors="ignore")

        logger.info(f"Parsed company details with {len(df)} rows")
        return df


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

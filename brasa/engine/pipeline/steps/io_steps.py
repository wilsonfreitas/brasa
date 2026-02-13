"""I/O related pipeline steps.

Steps for reading data from various file formats.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ..context import PipelineContext
from ..registry import StepRegistry
from ..step import PipelineStep


@StepRegistry.register("read_csv")
class ReadCsvStep(PipelineStep):
    """Read a CSV file into a DataFrame.

    Parameters:
        separator: Field separator (default: from context or ',')
        skip: Number of rows to skip (default: 0)
        header: Row number to use as header, or None for no header (default: 0)
        names: Column names to use (optional)
        converters: Dict of column converters (optional)
    """

    def execute(self, _data: Any, context: PipelineContext) -> pd.DataFrame:
        filepath = context.downloaded_file

        separator = self.get_param("separator", context.get_config("separator", ","))
        skip = self.get_param("skip", 0)
        header = self.get_param("header", 0)
        names = self.get_param("names")
        converters = self.get_param("converters")

        kwargs: dict[str, Any] = {
            "encoding": context.encoding,
            "sep": separator,
            "skiprows": skip,
            "dtype_backend": "pyarrow",
        }

        if names:
            kwargs["names"] = names
            kwargs["header"] = None
        else:
            kwargs["header"] = header

        if converters:
            kwargs["converters"] = converters

        return pd.read_csv(filepath, **kwargs)


@StepRegistry.register("read_fwf")
class ReadFwfStep(PipelineStep):
    """Read a fixed-width format file into a DataFrame.

    Supports both plain and gzip-compressed files.

    Derives column specifications from field widths if available in context.
    Widths are extracted from field.get_attribute('width') to build colspecs.

    Parameters:
        colspecs: List of (start, end) tuples for column positions (optional if fields with width available)
        names: Column names to use (optional if fields available)
        skip: Number of rows to skip (default: 0)
        dtype: Data type for columns. Can be a single type (e.g., str) or a dict mapping
            column names to types. (optional)
    """

    def execute(self, _data: Any, context: PipelineContext) -> pd.DataFrame:
        import gzip

        filepath = context.downloaded_file

        colspecs = self.get_param("colspecs")
        names = self.get_param("names")
        skip = self.get_param("skip", 0)
        dtype = self.get_param("dtype")

        # Derive colspecs and names from fields if not provided
        if not colspecs and context.fields:
            colspecs = []
            names = []
            position = 0
            for field in context.fields:
                width = field.get_attribute("width")
                if width is None:
                    raise ValueError(
                        f"Field '{field.name}' missing 'width' attribute for read_fwf"
                    )
                colspecs.append((position, position + width))
                names.append(field.name)
                position += width

        kwargs: dict[str, Any] = {
            "encoding": context.encoding,
            "skiprows": skip,
            "dtype_backend": "pyarrow",
        }

        if colspecs:
            kwargs["colspecs"] = [tuple(cs) for cs in colspecs]
        if names:
            kwargs["names"] = names
        if dtype is not None:
            kwargs["dtype"] = dtype

        # Handle gzip-compressed files
        if str(filepath).endswith(".gz"):
            with gzip.open(filepath, "rt", encoding=context.encoding) as f:
                return pd.read_fwf(f, **kwargs)
        else:
            return pd.read_fwf(filepath, **kwargs)


@StepRegistry.register("read_json")
class ReadJsonStep(PipelineStep):
    """Read a JSON file into a DataFrame.

    Parameters:
        orient: JSON orientation (default: 'records')
        path: JSON path to extract data from (optional)
    """

    def execute(self, _data: Any, context: PipelineContext) -> pd.DataFrame:
        import gzip
        import json
        from pathlib import Path

        filepath = context.downloaded_file
        json_path = self.get_param("path")

        # Handle gzip-compressed files
        if str(filepath).endswith(".gz"):
            with gzip.open(filepath, "rt", encoding=context.encoding) as f:
                json_data = json.load(f)
        else:
            with Path(filepath).open(encoding=context.encoding) as f:
                json_data = json.load(f)

        if json_path:
            # Navigate to the specified path
            for key in json_path.split("."):
                if isinstance(json_data, dict):
                    json_data = json_data[key]
                elif isinstance(json_data, list) and key.isdigit():
                    json_data = json_data[int(key)]

        if isinstance(json_data, list):
            return pd.DataFrame(json_data)
        elif isinstance(json_data, dict):
            return pd.DataFrame([json_data])
        else:
            raise ValueError(
                f"Cannot convert JSON data to DataFrame: {type(json_data)}"
            )


@StepRegistry.register("read_excel")
class ReadExcelStep(PipelineStep):
    """Read an Excel file into a DataFrame.

    Parameters:
        sheet: Sheet name or index (default: 0)
        skip: Number of rows to skip (default: 0)
        header: Row number to use as header (default: 0)
    """

    def execute(self, _data: Any, context: PipelineContext) -> pd.DataFrame:
        filepath = context.downloaded_file

        sheet = self.get_param("sheet", 0)
        skip = self.get_param("skip", 0)
        header = self.get_param("header", 0)

        return pd.read_excel(
            filepath,
            sheet_name=sheet,
            skiprows=skip,
            header=header,
        )

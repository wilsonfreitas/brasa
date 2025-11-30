"""Template management for market data sources.

This module handles loading and representing YAML template configurations
that define how to download, read, and write market data from various sources.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import IO, TYPE_CHECKING, Any

import pandas as pd
import yaml

from brasa.fieldset_schema import Fieldset
from brasa.fieldset_schema.field import Field

from .core import load_function_by_name

if TYPE_CHECKING:
    from .cache import CacheMetadata


class TemplatePart:
    """Placeholder class for template parts."""

    pass


class MarketDataETL:
    """Configuration for ETL (Extract-Transform-Load) processes.

    Wraps ETL configuration from a template and loads the processing function.
    """

    def __init__(self, etl: dict, template_id: str) -> None:
        for n, v in etl.items():
            self.__dict__[n] = v
        self.template_id = template_id
        self.process_function = load_function_by_name(etl["function"])


class MarketDataReader:
    """Configuration for reading market data files.

    Defines how to read downloaded files and convert them to DataFrames.
    """

    def __init__(self, reader: dict):
        self.attributes = {}
        for n, v in reader.items():
            self.attributes[n] = v
        self.locale = reader.get("locale", "en_US")
        self.encoding = reader.get("encoding", "utf-8")
        self.decimal = reader.get("decimal", ".")
        self.thousands = reader.get("thousands", ",")
        self.separator = reader.get("separator", ",")
        self.read_function = load_function_by_name(reader["function"])
        self.multi = reader.get("multi", {})
        self.parts: list | None = None
        self.fields: Fieldset | None = None
        self.output_filename_format = reader.get("output-filename-format", "%Y-%m-%d")

    def set_parts(self, parts: list) -> None:
        """Set template parts for multi-part file reading."""
        self.parts = parts

    def set_fields(self, fields: Fieldset) -> None:
        """Set field definitions for the reader."""
        self.fields = fields

    def get_attribute(self, key: str, default: Any = None) -> Any:
        """Get an attribute from the reader configuration."""
        return self.attributes.get(key, default)

    def read(self, meta: CacheMetadata) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """Read data using the configured read function.

        Args:
            meta: Cache metadata containing file paths and context.

        Returns:
            DataFrame or dictionary of DataFrames with the read data.
        """
        df = self.read_function(meta)
        return df


class MarketDataWriter:
    """Configuration for writing processed market data.

    Defines partitioning and other write options for parquet output.
    """

    def __init__(self, writer: dict):
        for n, v in writer.items():
            self.__dict__[n] = v
        self.partitioning = writer.get("partitioning", [])


class MarketDataDownloader:
    """Configuration for downloading market data from remote sources.

    Defines URL patterns, download functions, and file format handling.
    """

    def __init__(self, downloader: dict) -> None:
        self.url = None
        self.format = ""
        self.decoded_format = ""
        for n, v in downloader.items():
            self.__dict__[n] = v
        self.args = downloader.get("args", {})
        self.encoding = downloader.get("encoding", "utf-8")
        self.verify_ssl = downloader.get("verify_ssl", True)
        self.download_function = load_function_by_name(downloader["function"])
        validator: str = downloader.get(
            "validator", "brasa.downloaders.validate_empty_file"
        )
        self.validate_function = load_function_by_name(validator)
        self._extra_key = downloader.get("extra-key")
        if self._extra_key == "date":
            self.extra_key = datetime.now().isoformat()[:10]
        elif self._extra_key == "datetime":
            self.extra_key = datetime.now().isoformat()
        else:
            self.extra_key = ""

    def download_args(self, **kwargs) -> dict:
        """Prepare download arguments by merging defaults with provided kwargs."""
        args = {}
        for key, val in self.args.items():
            if key in kwargs:
                args[key] = kwargs[key]
            elif val is not None:
                args[key] = val
            else:
                raise ValueError(f"Missing argument {key}")
        return args

    def download(self, **kwargs) -> tuple[IO | None, Any]:
        """Execute the download.

        Args:
            **kwargs: Arguments to pass to the download function.

        Returns:
            Tuple of (file pointer, response object).

        Raises:
            DownloadException: If download fails.
        """
        from .exceptions import DownloadException

        args = self.download_args(**kwargs)
        try:
            fp, response = self.download_function(self, **args)
        except Exception as err:
            raise DownloadException("Problem downloading data.") from err
        return fp, response

    def validate(self, fname: str) -> None:
        """Validate a downloaded file."""
        return self.validate_function(fname)


class MarketDataTemplate:
    """Main template class that loads and represents a complete data source configuration.

    Loads a YAML template file and creates appropriate reader, writer, and downloader
    configurations.
    """

    def __init__(self, template_path) -> None:
        self.template_path = template_path
        self.has_reader = False
        self.has_downloader = False
        self.has_parts = False
        self.id = ""
        self.is_etl = False
        self.template = self.load_template()

    def load_template(self) -> dict:
        """Load and parse the YAML template file."""
        with Path(self.template_path).open(encoding="utf-8") as f:
            template = yaml.safe_load(f)
        for n in template:
            self.__dict__[n] = template[n]
            if n == "downloader":
                self.has_downloader = True
                self.downloader = MarketDataDownloader(template[n])
            elif n == "reader":
                self.has_reader = True
                self.reader = MarketDataReader(template[n])
            elif n == "writer":
                self.writer = MarketDataWriter(template[n])
            elif n == "fields":
                # Filter out null fields (where name is None/null)
                valid_fields = [f for f in template[n] if f.get("name") is not None]
                flds = [Field.from_dict(f) for f in valid_fields]
                fs = Fieldset()
                fs.add_fields(*flds)
                self.fields = fs
            elif n == "parts":
                self.has_parts = True
                self.parts = template[n]
            elif n == "etl":
                self.etl = MarketDataETL(template[n], template["id"])
                self.is_etl = True
        if self.has_reader:
            if self.has_parts:
                self.reader.set_parts(self.parts)
            else:
                self.reader.set_fields(self.fields)
        return template


def retrieve_template(template_name: str) -> MarketDataTemplate:
    """Load a template by name from the templates directory.

    Args:
        template_name: Name of the template (without .yaml extension).

    Returns:
        Loaded MarketDataTemplate instance.

    Raises:
        ValueError: If the template is not found.
    """
    templates_dir = Path(__file__).parent / ".." / ".." / "templates"
    sel = [f for f in templates_dir.iterdir() if f.name == f"{template_name}.yaml"]
    if len(sel) == 0:
        raise ValueError(f"Invalid template {template_name}")
    else:
        template_path = sel[0]
        return MarketDataTemplate(template_path)

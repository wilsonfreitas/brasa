"""Template management for market data sources.

This module handles loading and representing YAML template configurations
that define how to download, read, and write market data from various sources.
"""

from __future__ import annotations

from dataclasses import dataclass
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


@dataclass
class DatasetConfig:
    """Configuration for a single dataset in a multi-output template.

    Attributes:
        name: The output name for this dataset (e.g., 'indexes_info')
        tag: The source identifier/XML tag (e.g., 'IndxInf')
        fields: Field definitions for this dataset
    """

    name: str
    tag: str
    fields: Fieldset


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
    Supports both legacy function-based readers and the new pipeline approach.
    """

    def __init__(self, reader: dict, template_id: str = ""):
        self.attributes = {}
        self._template_id = template_id
        for n, v in reader.items():
            self.attributes[n] = v
        self.locale = reader.get("locale", "en_US")
        self.encoding = reader.get("encoding", "utf-8")
        self.decimal = reader.get("decimal", ".")
        self.thousands = reader.get("thousands", ",")
        self.separator = reader.get("separator", ",")
        self.multi = reader.get("multi", {})
        self.parts: list | None = None
        self.fields: Fieldset | None = None
        self.datasets: dict[str, DatasetConfig] | None = None
        self.output_filename_format = reader.get("output-filename-format", "%Y-%m-%d")

        # Check for pipeline-based reader (new approach)
        self._pipeline = None
        if "pipeline" in reader:
            from .pipeline import ReaderPipeline

            self._pipeline = ReaderPipeline.from_config(reader["pipeline"])
            self.read_function = None
        elif "function" in reader:
            # Legacy function-based reader
            self.read_function = load_function_by_name(reader["function"])
        else:
            self.read_function = None

    @property
    def has_pipeline(self) -> bool:
        """Check if this reader uses the pipeline approach."""
        return self._pipeline is not None

    def set_parts(self, parts: list) -> None:
        """Set template parts for multi-part file reading."""
        self.parts = parts

    def set_fields(self, fields: Fieldset) -> None:
        """Set field definitions for the reader."""
        self.fields = fields

    def set_datasets(self, datasets: dict[str, DatasetConfig]) -> None:
        """Set dataset configurations for multi-output reading.

        Also builds the multi mapping from datasets (tag -> output_name).

        Args:
            datasets: Dictionary mapping output names to DatasetConfig objects.
        """
        self.datasets = datasets
        # Build multi mapping from datasets: tag -> output_name
        self.multi = {cfg.tag: name for name, cfg in datasets.items()}

    def get_attribute(self, key: str, default: Any = None) -> Any:
        """Get an attribute from the reader configuration."""
        return self.attributes.get(key, default)

    def read(self, meta: CacheMetadata) -> pd.DataFrame | dict[str, pd.DataFrame]:
        """Read data using the configured read function or pipeline.

        Args:
            meta: Cache metadata containing file paths and context.

        Returns:
            DataFrame or dictionary of DataFrames with the read data.
        """
        if self._pipeline is not None:
            # Use the new pipeline approach
            return self._pipeline.execute(
                meta=meta,
                reader_config=self.attributes,
                fields=self.fields,
                datasets=self.datasets,
                template_id=self._template_id,
            )
        elif self.read_function is not None:
            # Use the legacy function approach
            return self.read_function(meta)
        else:
            raise ValueError("Reader has no pipeline or function configured")


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

    def _process_template_section(
        self, section_name: str, section_data: Any, template: dict
    ) -> None:
        """Process a single section of the template configuration."""
        self.__dict__[section_name] = section_data

        if section_name == "downloader":
            self.has_downloader = True
            self.downloader = MarketDataDownloader(section_data)
        elif section_name == "reader":
            self.has_reader = True
            self.reader = MarketDataReader(section_data, template_id=self.id)
        elif section_name == "writer":
            self.writer = MarketDataWriter(section_data)
        elif section_name == "fields":
            self._process_fields(section_data)
        elif section_name == "datasets":
            self._process_datasets(section_data)
        elif section_name == "parts":
            self.has_parts = True
            self.parts = section_data
        elif section_name == "etl":
            self.etl = MarketDataETL(section_data, template["id"])
            self.is_etl = True

    def _process_fields(self, fields_data: list) -> None:
        """Process the fields section of the template."""
        valid_fields = [f for f in fields_data if f.get("name") is not None]
        flds = [Field.from_dict(f) for f in valid_fields]
        fs = Fieldset()
        fs.add_fields(*flds)
        self.fields = fs

    def _process_datasets(self, datasets_data: dict) -> None:
        """Process the datasets section of the template."""
        self.datasets = {}
        for dataset_name, dataset_config in datasets_data.items():
            tag = dataset_config.get("tag", dataset_name)
            raw_fields = dataset_config.get("fields", [])
            valid_fields = [f for f in raw_fields if f.get("name") is not None]
            flds = [Field.from_dict(f) for f in valid_fields]
            fs = Fieldset()
            fs.add_fields(*flds)
            self.datasets[dataset_name] = DatasetConfig(
                name=dataset_name,
                tag=tag,
                fields=fs,
            )
        # Clear top-level fields when using datasets
        self.fields = None

    def load_template(self) -> dict:
        """Load and parse the YAML template file."""
        with Path(self.template_path).open(encoding="utf-8") as f:
            template = yaml.safe_load(f)

        # First pass: extract the template ID
        self.id = template.get("id", "")

        # Initialize datasets to None
        self.datasets: dict[str, DatasetConfig] | None = None

        # Second pass: process all template sections
        for section_name, section_data in template.items():
            self._process_template_section(section_name, section_data, template)

        # Configure reader with fields/datasets/parts
        if self.has_reader:
            if self.has_parts:
                self.reader.set_parts(self.parts)
            elif self.datasets is not None:
                self.reader.set_datasets(self.datasets)
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

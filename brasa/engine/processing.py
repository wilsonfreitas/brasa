"""Data processing operations for market data.

This module handles reading downloaded files, transforming data,
and saving to parquet format.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from brasa.fieldset_schema.adapters import PyArrowAdapter

from .cache import CacheManager, CacheMetadata
from .template import retrieve_template


def get_fname_part(meta: CacheMetadata, df: pd.DataFrame) -> str:
    """Generate a filename part based on metadata and data content.

    Args:
        meta: Cache metadata with download arguments.
        df: DataFrame containing the data.

    Returns:
        String to use as part of the output filename.
    """
    template = retrieve_template(meta.template)
    fmt = template.reader.output_filename_format
    if "refdate" in meta.download_args:
        fname_part = meta.download_args["refdate"].strftime(fmt)
    elif template.id == "b3-company-info":
        fname_part = f"{df['refdate'].iloc[0].strftime(fmt)}-{meta.download_args['issuingCompany']}"
    elif template.id == "b3-company-details":
        fname_part = (
            f"{df['refdate'].iloc[0].strftime(fmt)}-{meta.download_args['codeCVM']}"
        )
    elif template.id == "b3-cash-dividends":
        fname_part = (
            f"{df['refdate'].iloc[0].strftime(fmt)}-{meta.download_args['tradingName']}"
        )
    elif template.id == "b3-indexes-theoretical-portfolio":
        fname_part = (
            f"{df['refdate'].iloc[0].strftime(fmt)}-{meta.download_args['index']}"
        )
    elif "refdate" in df:
        fname_part = df["refdate"].iloc[0].strftime(fmt)
    else:
        fname_part = meta.download_checksum
    return fname_part


def save_parquet_file(
    meta: CacheMetadata, folder: str, processed_files_name: str, df: pd.DataFrame
) -> None:
    """Save a DataFrame to a single parquet file.

    Args:
        meta: Cache metadata to update with processed file info.
        folder: Target folder for the parquet file.
        processed_files_name: Key for the processed files registry.
        df: DataFrame to save.
    """
    man = CacheManager()
    fname_part = get_fname_part(meta, df)
    fname = str(Path(folder) / man.parquet_file_name(fname_part))
    meta.add_processed_file(processed_files_name, fname)
    df.to_parquet(man.cache_path(fname))


def save_partitioned_parquet_file(
    meta: CacheMetadata,
    folder: str,
    processed_files_name: str,
    df: pd.DataFrame,
    partition_cols: list[str],
    schema: pa.Schema = None,
) -> None:
    """Save DataFrame as partitioned parquet dataset with optional schema.

    Args:
        meta: Cache metadata to update with processed file info.
        folder: Target folder for parquet files.
        processed_files_name: Key for the processed files registry.
        df: DataFrame to save.
        partition_cols: Columns to use for partitioning.
        schema: Optional PyArrow schema for type enforcement.
    """
    if schema:
        tb = pa.Table.from_pandas(df, schema=schema)
        pq.write_to_dataset(
            tb,
            root_path=folder,
            partition_cols=partition_cols,
            schema=schema,
            existing_data_behavior="delete_matching",
        )
    else:
        tb = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            tb,
            root_path=folder,
            partition_cols=partition_cols,
            existing_data_behavior="delete_matching",
        )
    meta.add_processed_file(processed_files_name, folder)


def _get_schema_from_fields(fields):
    """Generate PyArrow schema from fields if available.

    Args:
        fields: Field definitions to convert to schema.

    Returns:
        PyArrow schema or None if generation fails.
    """
    if not fields:
        return None
    try:
        adapter = PyArrowAdapter(fields, verbose_warnings=False)
        return adapter.get_target_schema()
    except Exception:
        return None


def _process_multi_dataset_output(
    meta: CacheMetadata, df: dict, template, man: CacheManager
) -> None:
    """Process multi-dataset dictionary output.

    Args:
        meta: Cache metadata to update.
        df: Dictionary of DataFrames.
        template: Template configuration.
        man: CacheManager instance.
    """
    db_folder: dict = man.db_folders(template)

    if template.datasets:
        # New approach: use datasets for per-dataset schema and metadata
        for output_name, dx in df.items():
            if dx.shape[0] > 0:
                dataset_config = template.datasets[output_name]
                schema = _get_schema_from_fields(
                    dataset_config.fields if dataset_config.fields else None
                )
                folder = man.cache_path(db_folder[output_name])
                save_partitioned_parquet_file(
                    meta,
                    folder,
                    output_name,
                    dx,
                    template.writer.partitioning,
                    schema=schema,
                )
    elif template.reader.multi:
        # Legacy fallback: use multi mapping (XML tag -> output name)
        schema = _get_schema_from_fields(
            template.fields if hasattr(template, "fields") else None
        )

        for name, dx in df.items():
            if dx.shape[0] > 0:
                pfn = template.reader.multi[name]
                save_partitioned_parquet_file(
                    meta,
                    db_folder[name],
                    pfn,
                    dx,
                    template.writer.partitioning,
                    schema=schema,
                )


def _read_marketdata(meta: CacheMetadata) -> None:
    """Read downloaded files and save as processed parquet files.

    Uses the template's reader configuration to read the downloaded files,
    then saves the result as partitioned parquet datasets.

    Args:
        meta: Cache metadata containing download info and to update with processed files.
    """
    template = retrieve_template(meta.template)
    df = template.reader.read(meta)
    man = CacheManager()

    if isinstance(df, dict):
        _process_multi_dataset_output(meta, df, template, man)
    elif isinstance(df, pd.DataFrame):
        # Single dataset output
        schema = _get_schema_from_fields(
            template.fields if hasattr(template, "fields") else None
        )
        folder = man.cache_path(man.db_folder(template))
        save_partitioned_parquet_file(
            meta, folder, "data", df, template.writer.partitioning, schema=schema
        )

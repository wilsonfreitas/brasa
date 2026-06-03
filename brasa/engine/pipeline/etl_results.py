"""Result sentinels for ETL pipeline steps."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ETLWriteComplete:
    """Signals that an ETL step already wrote and registered its output dataset.

    Returned by steps (e.g. ``sql_export``) that write parquet and register the
    dataset themselves. ``ETLPipeline.execute_and_write`` detects this sentinel
    and skips its default DataFrame-based write.

    Attributes:
        path: Absolute path to the written dataset directory or file.
        layer: Data layer the dataset was written to.
        dataset: Output dataset name.
    """

    path: str
    layer: str
    dataset: str

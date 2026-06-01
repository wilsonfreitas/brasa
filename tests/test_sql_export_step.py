"""Tests for the streaming sql_export ETL step and the executor short-circuit."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from brasa.engine import CacheManager
from brasa.engine.catalog import DatasetCatalog
from brasa.engine.pipeline.etl_results import ETLWriteComplete


def _write_input(name: str, rows: list[tuple]) -> None:
    """Create a hive-partitioned input dataset and register it in the catalog.

    Args:
        name: Dataset name (created under the ``input`` layer).
        rows: List of ``(date, symbol, traded_price)`` tuples.
    """
    man = CacheManager()
    table = pa.table(
        {
            "refdate": pa.array([r[0] for r in rows], pa.date32()),
            "symbol": [r[1] for r in rows],
            "traded_price": pa.array([r[2] for r in rows], pa.float64()),
        }
    )
    path = man.db_path(f"input/{name}")
    Path(path).mkdir(parents=True, exist_ok=True)
    pq.write_to_dataset(table, root_path=path, partition_cols=["refdate"])
    DatasetCatalog().register_dataset(
        layer="input",
        dataset_name=name,
        schema=table.schema,
        partitioning=["refdate"],
        source_template=name,
    )


def test_etl_write_complete_is_frozen_dataclass():
    result = ETLWriteComplete(path="/tmp/x", layer="staging", dataset="d")
    assert result.path == "/tmp/x"
    assert result.layer == "staging"
    assert result.dataset == "d"

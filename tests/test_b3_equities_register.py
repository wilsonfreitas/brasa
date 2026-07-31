"""Regression tests for the b3-equities-register ETL (WIL-132)."""

from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from brasa import process_etl
from brasa.engine import CacheManager
from brasa.engine.catalog import DatasetCatalog
from brasa.queries import get_dataset


def _seed(layer: str, name: str, table: pa.Table) -> None:
    man = CacheManager()
    path = man.db_path(f"{layer}/{name}")
    Path(path).mkdir(parents=True, exist_ok=True)
    pq.write_table(table, Path(path) / "data.parquet")
    DatasetCatalog().register_dataset(
        layer=layer,
        dataset_name=name,
        schema=table.schema,
        partitioning=[],
        source_template=name,
    )


def test_register_spans_all_refdates_and_keeps_same_day_duplicates():
    """Regression for WIL-132: register must not be a latest-day snapshot."""
    _seed(
        "input",
        "b3-bvbg028-equities",
        pa.table(
            {
                "refdate": pa.array(
                    [date(2016, 2, 1), date(2016, 2, 1), date(2026, 7, 24)],
                    pa.date32(),
                ),
                "symbol": ["PETR4", "PETR4", "PETR4"],
                "distribution_id": pa.array([101, 102, 150], pa.int64()),
                "corporation_name": ["PETROBRAS"] * 3,
            }
        ),
    )
    process_etl("b3-equities-register")
    df = get_dataset("b3-equities-register", layer="staging").to_table().to_pandas()
    # Both refdates present (old snapshot behavior kept only max(refdate))
    assert sorted(df["refdate"].unique()) == [date(2016, 2, 1), date(2026, 7, 24)]
    # Same-day distribution_id duplicates preserved (no dedup by design)
    assert len(df) == 3

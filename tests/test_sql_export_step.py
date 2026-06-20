"""Tests for the streaming sql_export ETL step and the executor short-circuit."""

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from brasa.engine import CacheManager
from brasa.engine.catalog import DatasetCatalog
from brasa.engine.pipeline.etl_context import ETLPipelineContext
from brasa.engine.pipeline.etl_executor import ETLPipeline
from brasa.engine.pipeline.etl_results import ETLWriteComplete
from brasa.engine.pipeline.registry import StepRegistry
from brasa.engine.template import MarketDataTemplate, MarketDataWriter
from brasa.queries import get_dataset


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


def test_sql_export_step_registered():
    step = StepRegistry.create(
        "sql_export",
        {
            "step": "sql_export",
            "datasets": ["input.x"],
            "query": "SELECT * FROM 'input.x'",
        },
    )
    assert step is not None
    assert step.name == "sql_export"
    assert step.get_input_datasets() == ["input.x"]


def test_sql_export_writes_partitioned_with_date_cutover():
    # legacy: the 2026-05-01 row must be dropped (legacy ends in April)
    _write_input(
        "ti-legacy",
        [(dt.date(2026, 4, 30), "L", 1.0), (dt.date(2026, 5, 1), "L", 9.0)],
    )
    # equities: the 2026-04-01 row must be dropped (equities start in May)
    _write_input(
        "ti-eq",
        [(dt.date(2026, 4, 1), "E", 7.0), (dt.date(2026, 5, 1), "E", 2.0)],
    )

    query = (
        "SELECT refdate, symbol, traded_price FROM 'input.ti-legacy' "
        "WHERE refdate <= DATE '2026-04-30' "
        "UNION ALL "
        "SELECT refdate, symbol, traded_price FROM 'input.ti-eq' "
        "WHERE refdate >= DATE '2026-05-01'"
    )
    step = StepRegistry.create(
        "sql_export",
        {
            "step": "sql_export",
            "datasets": ["input.ti-legacy", "input.ti-eq"],
            "query": query,
        },
    )
    writer = MarketDataWriter(
        {"layer": "staging", "dataset": "ti-out", "partitioning": ["refdate"]},
        "consolidated-tpl",
    )
    ctx = ETLPipelineContext(template_id="consolidated-tpl", writer=writer)

    result = step.execute(None, ctx)

    assert isinstance(result, ETLWriteComplete)
    assert result.layer == "staging"
    assert result.dataset == "ti-out"

    man = CacheManager()
    out_path = Path(man.db_path("staging/ti-out"))
    assert (out_path / ".last_processed").exists()
    part_dirs = sorted(p.name for p in out_path.glob("refdate=*"))
    assert part_dirs == ["refdate=2026-04-30", "refdate=2026-05-01"]

    # Catalog row registered with refdate partitioning
    info = DatasetCatalog().get_dataset_info("staging", "ti-out")
    assert info is not None
    assert info.partitioning == ["refdate"]

    df = (
        get_dataset(
            "ti-out",
            layer="staging",
            use_template_schema=False,
            use_catalog_schema=True,
        )
        .to_table()
        .to_pandas()
        .sort_values("refdate")
        .reset_index(drop=True)
    )
    # Legacy May row and equities April row excluded; no day sourced twice
    assert list(df["symbol"]) == ["L", "E"]
    assert list(df["traded_price"]) == [1.0, 2.0]


def test_sql_export_unpartitioned_writes_single_file():
    _write_input("ti-solo", [(dt.date(2026, 5, 1), "S", 3.0)])
    step = StepRegistry.create(
        "sql_export",
        {
            "step": "sql_export",
            "datasets": ["input.ti-solo"],
            "query": "SELECT refdate, symbol, traded_price FROM 'input.ti-solo'",
        },
    )
    writer = MarketDataWriter({"layer": "staging", "dataset": "ti-solo-out"}, "tpl")
    ctx = ETLPipelineContext(template_id="tpl", writer=writer)

    result = step.execute(None, ctx)

    assert isinstance(result, ETLWriteComplete)
    man = CacheManager()
    assert Path(man.db_path("staging/ti-solo-out/data.parquet")).exists()


def test_executor_skips_default_write_on_sentinel():
    _write_input("ti-sc", [(dt.date(2026, 5, 1), "Z", 5.0)])
    pipeline = ETLPipeline.from_config(
        [
            {
                "step": "sql_export",
                "datasets": ["input.ti-sc"],
                "query": ("SELECT refdate, symbol, traded_price FROM 'input.ti-sc'"),
            }
        ]
    )
    writer = MarketDataWriter(
        {"layer": "staging", "dataset": "ti-sc-out", "partitioning": ["refdate"]},
        "tpl",
    )

    # Must not raise (no pa.Table.from_pandas on a sentinel); step does the write
    pipeline.execute_and_write("tpl", writer=writer, fields=None)

    man = CacheManager()
    out = Path(man.db_path("staging/ti-sc-out"))
    assert (out / ".last_processed").exists()
    assert sorted(p.name for p in out.glob("refdate=*")) == ["refdate=2026-05-01"]


def test_consolidated_template_uses_sql_export():
    tpl = MarketDataTemplate(
        "brasa/files/templates/b3/intraday/b3-trades-intraday-consolidated.yaml"
    )
    assert tpl.id == "b3-trades-intraday-consolidated"
    assert tpl.is_etl
    assert tpl.etl.is_pipeline

    step_names = [s.name for s in tpl.etl.pipeline.steps]
    assert step_names == ["sql_export"]

    assert tpl.writer.layer.value == "staging"
    assert tpl.writer.dataset == "b3-trades-intraday"
    assert tpl.writer.partitioning == ["refdate"]

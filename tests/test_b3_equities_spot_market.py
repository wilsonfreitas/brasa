"""Regression tests for the b3-equities-spot-market ETL (WIL-132)."""

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


def _register_row(refdate, symbol, corporation_name, market=10):
    return {
        "refdate": refdate,
        "symbol": symbol,
        "corporation_name": corporation_name,
        "instrument_market": market,
        "instrument_segment": 1,
        "instrument_asset": symbol[:4],
        "trading_start_date": date(2010, 1, 4),
        "security_category": 11,
    }


def test_spot_market_is_full_span_and_excludes_test_assets():
    """Regression for WIL-132: full-span universe, B3 test instruments out."""
    rows = [
        _register_row(date(2016, 2, 1), "PETR4", "PETROBRAS"),
        _register_row(date(2026, 7, 24), "PETR4", "PETROBRAS"),
        _register_row(date(2026, 7, 24), "TF603", "ATIVO TESTE ON"),
        _register_row(date(2026, 7, 24), "IPNN3", "TESTE IPN VS SA"),
        _register_row(date(2026, 7, 24), "TF999", "XX"),
        _register_row(date(2026, 7, 24), "FRAC4F", "FRACIONARIO", market=20),
    ]
    table = pa.table(
        {
            "refdate": pa.array([r["refdate"] for r in rows], pa.date32()),
            "symbol": [r["symbol"] for r in rows],
            "corporation_name": [r["corporation_name"] for r in rows],
            "instrument_market": pa.array(
                [r["instrument_market"] for r in rows], pa.int64()
            ),
            "instrument_segment": pa.array(
                [r["instrument_segment"] for r in rows], pa.int64()
            ),
            "instrument_asset": [r["instrument_asset"] for r in rows],
            "trading_start_date": pa.array(
                [r["trading_start_date"] for r in rows], pa.date32()
            ),
            "security_category": pa.array(
                [r["security_category"] for r in rows], pa.int64()
            ),
        }
    )
    _seed("staging", "b3-equities-register", table)
    process_etl("b3-equities-spot-market")
    df = get_dataset("b3-equities-spot-market", layer="staging").to_table().to_pandas()
    # Full-span: PETR4 present on BOTH refdates
    petr = df[df["symbol"] == "PETR4"]
    assert sorted(petr["refdate"].unique()) == [date(2016, 2, 1), date(2026, 7, 24)]
    # Test assets and non-spot rows excluded
    assert set(df["symbol"]) == {"PETR4"}

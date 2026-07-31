"""Regression tests for the b3-equities-unadjusted-prices ETL (WIL-129)."""

from datetime import date
from pathlib import Path

import pandas as pd
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


def test_unadjusted_prices_pre2016_cotahist_branch_with_isin_guard():
    """Regression for WIL-129: pre-2016 cotahist history, ISIN-guarded joins."""
    _seed(
        "staging",
        "b3-equities-spot-market",
        pa.table({"symbol": ["PETR4"], "isin": ["BRPETRACNPR6"]}),
    )
    _seed(
        "staging",
        "b3-cotahist",
        pa.table(
            {
                "refdate": pa.array(
                    [
                        date(2005, 3, 1),
                        date(2010, 6, 15),
                        date(2010, 6, 16),
                        date(2021, 6, 10),
                    ],
                    pa.date32(),
                ),
                "symbol": ["PETR4"] * 4,
                "isin": [
                    "OTHER1111111",
                    "BRPETRACNPR6",
                    "BRPETRACNPR6",
                    "BRPETRACNPR6",
                ],
                "instrument_market": pa.array([10, 10, 20, 10], pa.int64()),
                "trade_quantity": pa.array([50.0, 100.0, 7.0, 300.0], pa.float64()),
                "traded_contracts": pa.array(
                    [500.0, 1000.0, 70.0, 3000.0], pa.float64()
                ),
                "volume": pa.array([5000.0, 10000.0, 700.0, 90000.0], pa.float64()),
                "open": pa.array([8.0, 10.0, 10.6, 27.0], pa.float64()),
                "low": pa.array([7.5, 9.0, 10.4, 26.5], pa.float64()),
                "high": pa.array([8.5, 11.0, 10.8, 28.5], pa.float64()),
                "close": pa.array([8.2, 10.5, 10.7, 28.0], pa.float64()),
                "average": pa.array([8.1, 10.2, 10.6, 27.5], pa.float64()),
                "distribution_id": pa.array([90, 102, 102, 205], pa.int64()),
            }
        ),
    )
    _seed(
        "input",
        "b3-bvbg086",
        pa.table(
            {
                "refdate": pa.array([date(2016, 3, 1), date(2016, 3, 2)], pa.date32()),
                "symbol": ["PETR4", "PETR4"],
                "traded_quantity": pa.array([200.0, 210.0], pa.float64()),
                "traded_contracts": pa.array([2000.0, 2100.0], pa.float64()),
                "volume": pa.array([25000.0, 26000.0], pa.float64()),
                "open": pa.array([12.0, 12.5], pa.float64()),
                "low": pa.array([11.8, 12.3], pa.float64()),
                "high": pa.array([12.8, 13.2], pa.float64()),
                "close": pa.array([12.5, 13.0], pa.float64()),
                "average": pa.array([12.4, 12.9], pa.float64()),
            }
        ),
    )
    _seed(
        "input",
        "b3-bdin-stocks-summary",
        pa.table(
            {
                "refdate": pa.array([date(2016, 2, 29)], pa.date32()),
                "cod_negociacao": ["PETR4"],
                "cod_isin": ["BRPETRACNPR6"],
                "cod_bdi": pa.array([2], pa.int64()),
                "qtd_negocios": pa.array([150.0], pa.float64()),
                "qtd_titulos_negociados": pa.array([1500.0], pa.float64()),
                "volume_titulos_negociados": pa.array([16000.0], pa.float64()),
                "preco_abertura": pa.array([10.8], pa.float64()),
                "preco_min": pa.array([10.6], pa.float64()),
                "preco_max": pa.array([11.2], pa.float64()),
                "preco_ult": pa.array([11.0], pa.float64()),
                "preco_med": pa.array([10.9], pa.float64()),
                "num_dist": pa.array([115.0], pa.float64()),
            }
        ),
    )
    _seed(
        "input",
        "b3-bvbg028-equities",
        pa.table(
            {
                "refdate": pa.array(
                    [date(2016, 3, 1), date(2016, 3, 1), date(2016, 3, 1)],
                    pa.date32(),
                ),
                "symbol": ["PETR4", "PETR4", "PETR4"],
                "distribution_id": pa.array([137, 138, 999], pa.int64()),
                "instrument_market": pa.array([10, 10, 20], pa.int64()),
                "instrument_segment": pa.array([1, 1, 1], pa.int64()),
                "instrument_asset": ["PETR", "PETR", "PETR"],
                "security_category": pa.array([11, 11, 11], pa.int64()),
            }
        ),
    )

    process_etl("b3-equities-unadjusted-prices")

    df = (
        get_dataset(
            "b3-equities-unadjusted-prices",
            layer="staging",
            use_template_schema=False,
            use_catalog_schema=True,
        )
        .to_table()
        .to_pandas()
    )
    df["refdate"] = pd.to_datetime(df["refdate"]).dt.date

    # Pre-2016 cotahist row with matching ISIN is included; the 2005
    # different-ISIN row (ticker reuse) and the market=20 row are excluded.
    expected = {
        (date(2010, 6, 15), 10.5),
        (date(2016, 2, 29), 11.0),
        (date(2016, 3, 1), 12.5),
        (date(2016, 3, 2), 13.0),
        (date(2021, 6, 10), 28.0),
    }
    assert set(zip(df["refdate"], df["close"], strict=True)) == expected
    assert len(df) == 5
    assert df.groupby(["symbol", "refdate"]).size().max() == 1

    # distribution_id per branch: cotahist native (pre-2016 and 2021-06-10),
    # BDIN num_dist cast (2016-02-29), register max() dedup picking 138 over
    # 137 and ignoring the market=20 row (2016-03-01), null when the register
    # has no same-day row (2016-03-02).
    dist = df.set_index("refdate")["distribution_id"]
    assert dist[date(2010, 6, 15)] == 102
    assert dist[date(2016, 2, 29)] == 115
    assert dist[date(2016, 3, 1)] == 138
    assert pd.isna(dist[date(2016, 3, 2)])
    assert dist[date(2021, 6, 10)] == 205

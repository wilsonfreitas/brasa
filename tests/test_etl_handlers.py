"""Unit tests for ETL handler functions in brasa.etl.

These tests exercise the handlers in isolation by faking the dataset
boundary (get_dataset/write_dataset) — no cache or network required.
"""

from datetime import date
from types import SimpleNamespace

import pyarrow

import brasa.etl


def test_create_cotahist_dataset_writes_sorted_by_refdate(monkeypatch):
    tb_yearly = pyarrow.table(
        {
            "refdate": [date(2024, 1, 4), date(2024, 1, 2)],
            "close": [4.0, 2.0],
        }
    )
    tb_daily = pyarrow.table(
        {
            "refdate": [date(2024, 1, 3)],
            "close": [3.0],
        }
    )
    datasets = {
        "yearly": SimpleNamespace(to_table=lambda: tb_yearly),
        "daily": SimpleNamespace(to_table=lambda: tb_daily),
    }
    written = {}
    monkeypatch.setattr(brasa.etl, "get_dataset", lambda name: datasets[name])
    monkeypatch.setattr(
        brasa.etl,
        "write_dataset",
        lambda df, template_id: written.update(df=df, template_id=template_id),
    )

    handler = SimpleNamespace(
        yearly_dataset="yearly", daily_dataset="daily", template_id="cotahist"
    )
    brasa.etl.create_cotahist_dataset(handler)

    assert written["template_id"] == "cotahist"
    refdates = list(written["df"]["refdate"])
    assert refdates == sorted(refdates)

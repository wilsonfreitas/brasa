"""Input-validation and empty-result guards for query functions (audit Q7.2/Q7.5)."""

from datetime import date, datetime

import pandas as pd
import pyarrow
import pyarrow.dataset as pads
import pytest

import brasa.queries


@pytest.fixture
def fake_returns_dataset(monkeypatch):
    table = pyarrow.table(
        {
            "refdate": pd.to_datetime([date(2024, 1, 2), date(2024, 1, 3)]),
            "symbol": ["PETR4", "PETR4"],
            "returns": [0.01, -0.02],
        }
    )
    datasets = {
        "brasa-returns": pads.dataset(table),
        "brasa-prices": pads.dataset(
            pyarrow.table(
                {
                    "refdate": pd.to_datetime([date(2024, 1, 2)]),
                    "symbol": ["PETR4"],
                    "close": [37.5],
                }
            )
        ),
    }
    monkeypatch.setattr(brasa.queries, "get_dataset", lambda name, **kw: datasets[name])


def test_get_returns_unknown_symbol_returns_empty_frame(fake_returns_dataset):
    df = brasa.queries.get_returns("NOPE11")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_prices_unknown_symbol_returns_empty_frame(fake_returns_dataset):
    df = brasa.queries.get_prices("NOPE11")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


def test_get_returns_rejects_reversed_date_range(fake_returns_dataset):
    with pytest.raises(ValueError, match="start"):
        brasa.queries.get_returns(
            "PETR4", start=datetime(2024, 2, 1), end=datetime(2024, 1, 1)
        )


def test_get_prices_rejects_reversed_date_range(fake_returns_dataset):
    with pytest.raises(ValueError, match="start"):
        brasa.queries.get_prices(
            "PETR4", start=datetime(2024, 2, 1), end=datetime(2024, 1, 1)
        )


def test_get_returns_restores_bizdays_mode_on_error(fake_returns_dataset, monkeypatch):
    """Regression for audit Q7.5: the bizdays 'mode' option must be restored
    even when calendar loading fails mid-block."""
    from bizdays import get_option

    def boom(name):
        raise RuntimeError("no such calendar")

    monkeypatch.setattr(brasa.queries.Calendar, "load", boom)
    mode_before = get_option("mode")
    with pytest.raises(RuntimeError):
        brasa.queries.get_returns("PETR4")
    assert get_option("mode") == mode_before


def test_list_tables_logs_on_error(monkeypatch, caplog):
    """Regression for audit Q7.6: DB errors must at least be logged."""
    import logging

    def boom():
        raise RuntimeError("db unreachable")

    monkeypatch.setattr(brasa.queries.BrasaDB, "get_connection", boom)
    with caplog.at_level(logging.WARNING, logger="brasa.queries"):
        assert brasa.queries.BrasaDB.list_tables() == []
    assert any("db unreachable" in rec.message for rec in caplog.records)


def test_get_returns_known_symbol_still_works(fake_returns_dataset):
    df = brasa.queries.get_returns("PETR4")
    assert list(df.columns) == ["PETR4"]
    assert len(df) == 2

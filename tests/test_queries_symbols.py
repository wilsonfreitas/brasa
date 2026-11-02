"""Symbol and industry-sector helpers read datasets that templates produce (WIL-206)."""

import pandas as pd
import pyarrow
import pyarrow.dataset as pads
import pytest

import brasa.queries


@pytest.fixture
def fake_company_datasets(monkeypatch):
    datasets = {
        "b3-companies-symbols": pads.dataset(
            pyarrow.table(
                {
                    "symbol": ["ITUB4", "ITUB3", "ITUB1", "PETR4", "AAPL34"],
                    "code_cvm": ["19348", "19348", "19348", "9512", "80000"],
                    "instrument_type": [
                        "EQUITY",
                        "EQUITY",
                        "SUBSCRIPTION_RIGHT",
                        "EQUITY",
                        "BDR",
                    ],
                }
            )
        ),
        "brasa-companies": pads.dataset(
            pyarrow.table(
                {
                    "code_cvm": ["19348", "9512", "80000", "1"],
                    "sector": ["Financeiro", "Petróleo", "Financeiro", None],
                    "subsector": ["Bancos", "Petróleo", "Bancos", None],
                    "segment": ["Bancos", "Exploração", "Bancos", None],
                }
            )
        ),
    }
    monkeypatch.setattr(brasa.queries, "get_dataset", lambda name, **kw: datasets[name])


def test_get_industry_sectors_distinct_sorted_without_null(fake_company_datasets):
    df = brasa.queries.get_industry_sectors()
    assert list(df.columns) == ["sector", "subsector", "segment"]
    assert df.values.tolist() == [
        ["Financeiro", "Bancos", "Bancos"],
        ["Petróleo", "Petróleo", "Exploração"],
    ]


def test_get_symbols_industry_sector(fake_company_datasets):
    assert brasa.queries.get_symbols("industry-sector") == ["Financeiro", "Petróleo"]


def test_get_symbols_equity_excludes_rights(fake_company_datasets):
    symbols = brasa.queries.get_symbols("equity")
    assert sorted(symbols) == ["AAPL34", "ITUB3", "ITUB4", "PETR4"]


def test_get_symbols_equity_by_sector(fake_company_datasets):
    symbols = brasa.queries.get_symbols("equity", sector="Financeiro")
    assert sorted(symbols) == ["AAPL34", "ITUB3", "ITUB4"]


@pytest.fixture
def fake_index_and_fund_datasets(monkeypatch):
    d1, d2, d3 = (
        pd.Timestamp(x).date() for x in ["2024-12-02", "2024-12-20", "2025-01-06"]
    )
    datasets = {
        "b3-indexes-composition": pads.dataset(
            pyarrow.table(
                {
                    "refdate": [d1, d2, d2, d3, d3],
                    "indexes": ["IBOV", "IBOV", "IBOV", "IBOV", "SMLL"],
                    "symbol": ["OLD3", "PETR4", "VALE3", "PETR4", "SMAL3"],
                    "year": [2024, 2024, 2024, 2025, 2025],
                    "end_month": [12, 12, 12, 4, 4],
                }
            )
        ),
        "b3-listed-funds": pads.dataset(
            pyarrow.table(
                {
                    "refdate": [d1, d2, d3, d3],
                    "symbol": ["BOVA11", "IVVB11", "HGLG11", "IMAB11"],
                    "fund_type": ["ETF", "ETF", "FII", "ETF-RF"],
                }
            )
        ),
    }
    monkeypatch.setattr(brasa.queries, "get_dataset", lambda name, **kw: datasets[name])


def test_get_symbols_equity_by_index_uses_latest_date(fake_index_and_fund_datasets):
    assert brasa.queries.get_symbols("equity", index="IBOV") == ["PETR4"]


def test_get_symbols_equity_by_index_and_end_month(fake_index_and_fund_datasets):
    symbols = brasa.queries.get_symbols("equity", index="IBOV", end_month="2024-12-31")
    assert sorted(symbols) == ["PETR4", "VALE3"]
    assert (
        brasa.queries.get_symbols("equity", index="IBOV", end_month="2010-12-31") == []
    )


def test_get_symbols_funds_latest_date_per_type(fake_index_and_fund_datasets):
    # ETF's latest date (2024-12-20) is older than the dataset's (2025-01-06).
    assert brasa.queries.get_symbols("etf") == ["IVVB11"]
    assert brasa.queries.get_symbols("fii") == ["HGLG11"]
    assert brasa.queries.get_symbols("fietf") == ["IMAB11"]

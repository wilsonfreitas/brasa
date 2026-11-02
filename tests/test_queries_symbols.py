"""Symbol and industry-sector helpers read datasets that templates produce (WIL-206)."""

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

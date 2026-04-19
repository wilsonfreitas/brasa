import io
import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from brasa.downloaders import bcb_currency_download
from brasa.downloaders.downloaders import BCBCurrencyDownloader
from brasa.engine import (
    CacheManager,
    MarketDataTemplate,
    download_marketdata,
    process_marketdata,
    retrieve_template,
)
from brasa.fieldsets import Fieldset


def test_bcb_currency_downloader_serializes_dataframe_to_json():
    df = pd.DataFrame(
        [
            {
                "dataHoraCotacao": "2025-01-02 10:08:23.123",
                "cotacaoCompra": 6.1234,
                "cotacaoVenda": 6.1240,
                "paridadeCompra": 1.0,
                "paridadeVenda": 1.0,
                "tipoBoletim": "Abertura",
            }
        ]
    )

    fake_query = MagicMock()
    fake_query.parameters.return_value.collect.return_value = df
    fake_endpoint = MagicMock()
    fake_endpoint.query.return_value = fake_query
    fake_ptax = MagicMock()
    fake_ptax.get_endpoint.return_value = fake_endpoint

    with patch(
        "brasa.downloaders.downloaders.PTAX", return_value=fake_ptax
    ) as mock_ptax:
        downloader = BCBCurrencyDownloader(
            currency="USD", start=date(2025, 1, 2), end=date(2025, 1, 2)
        )
        result = downloader.download()

    mock_ptax.assert_called_once_with()
    fake_ptax.get_endpoint.assert_called_once_with("CotacaoMoedaPeriodo")
    fake_query.parameters.assert_called_once_with(
        moeda="USD", dataInicial="01/02/2025", dataFinalCotacao="01/02/2025"
    )
    assert isinstance(result, io.BytesIO)
    data = json.loads(result.read().decode("utf8"))
    assert len(data) == 1
    assert data[0]["dataHoraCotacao"] == "2025-01-02 10:08:23.123"
    assert data[0]["cotacaoVenda"] == 6.1240


def test_bcb_currency_downloader_returns_none_on_error():
    with patch("brasa.downloaders.downloaders.PTAX", side_effect=Exception("boom")):
        downloader = BCBCurrencyDownloader(
            currency="USD", start=date(2025, 1, 1), end=date(2025, 1, 1)
        )
        assert downloader.download() is None


def test_bcb_currency_download_helper_is_exported():
    assert callable(bcb_currency_download)


def test_load_bcb_currency_template():
    tpl = MarketDataTemplate("templates/bcb/bcb-currency.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    assert tpl.id == "bcb-currency"


def test_bcb_currency_template_fields():
    tpl = MarketDataTemplate("templates/bcb/bcb-currency.yaml")

    assert isinstance(tpl.fields, Fieldset)
    expected = {
        "refdate": "datetime",
        "currency": "string",
        "bid": "numeric",
        "ask": "numeric",
        "parity_bid": "numeric",
        "parity_ask": "numeric",
        "bulletin_type": "string",
        "downloaded_at": "datetime",
    }
    assert len(tpl.fields) == len(expected)
    for name, type_name in expected.items():
        assert tpl.fields[name].type_name == type_name, name


def test_retrieve_bcb_currency_template():
    tpl = retrieve_template("bcb-currency")
    assert tpl is not None
    assert tpl.id == "bcb-currency"


@pytest.mark.integration
def test_bcb_currency_download_and_process_usd():
    """Integration: download USD intraday PTAX quotes for a small window and process."""
    download_marketdata(
        "bcb-currency",
        currency="USD",
        start=date(2025, 1, 2),
        end=date(2025, 1, 10),
    )
    process_marketdata("bcb-currency")

    man = CacheManager()
    ds_path = Path(man.db_path("input/bcb-currency"))
    assert ds_path.exists(), f"Expected dataset at {ds_path}"


@pytest.mark.integration
def test_bcb_currency_download_multiple_currencies():
    """Integration: download multiple currencies via KwargsIterator."""
    download_marketdata(
        "bcb-currency",
        currency=["USD", "EUR"],
        start=date(2025, 1, 2),
        end=date(2025, 1, 3),
    )
    process_marketdata("bcb-currency")

    man = CacheManager()
    ds_path = Path(man.db_path("input/bcb-currency"))
    assert ds_path.exists()

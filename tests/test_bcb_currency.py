import io
import json
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from brasa.downloaders.downloaders import BCBCurrencyDownloader


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

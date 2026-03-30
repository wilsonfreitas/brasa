import io
import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from brasa.downloaders.downloaders import BCBSGSDownloader
from brasa.engine import (
    CacheManager,
    MarketDataTemplate,
    download_marketdata,
    process_etl,
    process_marketdata,
    retrieve_template,
)
from brasa.fieldsets import Fieldset

# Task 1: BCBSGSDownloader tests


def test_bcb_sgs_downloader_with_start_end():
    mock_json = json.dumps(
        [
            {"data": "02/01/2025", "valor": "12,13"},
            {"data": "03/01/2025", "valor": "12,13"},
        ]
    )

    with patch(
        "brasa.downloaders.downloaders.sgs.get_json", return_value=mock_json
    ) as mock_get:
        downloader = BCBSGSDownloader(
            code=4389, start=date(2025, 1, 2), end=date(2025, 1, 3)
        )
        result = downloader.download()

        mock_get.assert_called_once_with(
            4389, start=date(2025, 1, 2), end=date(2025, 1, 3)
        )
        assert result is not None
        assert isinstance(result, io.BytesIO)
        data = json.loads(result.read().decode("utf8"))
        assert len(data) == 2
        assert data[0]["data"] == "02/01/2025"


def test_bcb_sgs_downloader_returns_none_on_error():
    with patch(
        "brasa.downloaders.downloaders.sgs.get_json", side_effect=Exception("API error")
    ):
        downloader = BCBSGSDownloader(
            code=9999, start=date(2025, 1, 1), end=date(2025, 1, 1)
        )
        result = downloader.download()

        assert result is None


# Task 2: Template tests


def test_load_bcb_sgs_template():
    tpl = MarketDataTemplate("templates/bcb/bcb-sgs.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    assert tpl.id == "bcb-sgs"


def test_bcb_sgs_template_fields():
    tpl = MarketDataTemplate("templates/bcb/bcb-sgs.yaml")

    assert isinstance(tpl.fields, Fieldset)
    assert len(tpl.fields) == 3
    assert tpl.fields["refdate"].type_name == "date"
    assert tpl.fields["value"].type_name == "numeric"
    assert tpl.fields["code"].type_name == "integer"


def test_retrieve_bcb_sgs_template():
    tpl = retrieve_template("bcb-sgs")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "bcb-sgs"


# Task 3: Integration tests


@pytest.mark.integration
def test_bcb_sgs_download_and_process():
    """Integration test: download SGS series 4389 (CDI) for a small date range."""
    download_marketdata(
        "bcb-sgs", code=4389, start=date(2025, 1, 2), end=date(2025, 1, 10)
    )
    process_marketdata("bcb-sgs")

    man = CacheManager()
    ds_path = Path(man.db_path("input/bcb-sgs"))
    assert ds_path.exists(), f"Expected dataset at {ds_path}"


@pytest.mark.integration
def test_bcb_sgs_download_multiple_codes():
    """Integration test: download multiple SGS codes via KwargsIterator."""
    download_marketdata(
        "bcb-sgs", code=[4389, 1178], start=date(2025, 1, 2), end=date(2025, 1, 3)
    )
    process_marketdata("bcb-sgs")

    man = CacheManager()
    ds_path = Path(man.db_path("input/bcb-sgs"))
    assert ds_path.exists(), f"Expected dataset at {ds_path}"


# Task 5: ETL test


@pytest.mark.integration
def test_bcb_data_etl_reads_from_input():
    """Integration test: bcb-data ETL reads from input.bcb-sgs instead of calling python-bcb directly."""
    # First, download SGS data for the codes used by bcb-data
    codes = [4389, 1178, 432, 433, 189]
    download_marketdata(
        "bcb-sgs", code=codes, start=date(2025, 1, 2), end=date(2025, 1, 10)
    )
    process_marketdata("bcb-sgs")

    # Then run the ETL
    process_etl("bcb-data")

    man = CacheManager()
    ds_path = Path(man.db_path("staging/bcb-data"))
    assert ds_path.exists(), f"Expected dataset at {ds_path}"

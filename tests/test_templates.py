
from datetime import datetime
import os

import pytest
from brasa.templates import MarketDataTemplate, TemplateFields, download_marketdata, retrieve_template


def test_load_template():
    tpl = MarketDataTemplate("templates/CDIIDI.yaml")

    assert tpl.has_downloader
    assert ~ tpl.has_reader


def test_download_and_read():
    tpl = MarketDataTemplate("templates/CDIIDI.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    assert isinstance(tpl.fields, TemplateFields)
    assert len(tpl.fields) == 4
    assert tpl.fields["dataTaxa"].name == "dataTaxa"
    assert tpl.fields["dataTaxa"].description == "Data de divulgação da taxa DI"
    assert tpl.fields["dataTaxa"].handler.type == "Date"
    assert tpl.fields["dataTaxa"].handler.format == "%d/%m/%Y"

    fp, _ = tpl.downloader.download()
    assert fp is not None
    assert fp.readable()

    df = tpl.reader.read(fp)
    assert df is not None


def test_retrieve_temlate():
    tpl = retrieve_template("CDIIDI")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "CDIIDI"


def test_download_marketdata():
    dest = download_marketdata("CDIIDI")
    assert dest is not None
    assert os.path.exists(dest)


def test_download_marketdata_missing_args_error():
    with pytest.raises(ValueError) as exc_info:
        download_marketdata("OpcoesAcoesEmAberto")
    assert exc_info.value.args[0] == "Missing argument refdate"


def test_download_marketdata_with_refdate():
    dest = download_marketdata("OpcoesAcoesEmAberto", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert os.path.exists(dest)
    dest = download_marketdata("OpcoesAcoesEmAberto", refdate=datetime(2023, 5, 2))
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)

    dest = download_marketdata("NegociosBalcao", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert os.path.exists(dest)


def test_download_marketdata_with_refdate_and_unzip():
    dest = download_marketdata("COTAHIST_DAILY", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, list)
    assert len(dest) == 1
    assert os.path.exists(dest[0])


def test_download_marketdata_with_refdate_and_unzip_recursive_with_1_file():
    dest = download_marketdata("IndexReport", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, list)
    assert len(dest) == 1
    assert os.path.exists(dest[0])


def test_download_marketdata_with_refdate_and_unzip_recursive_with_many_files():
    dest = download_marketdata("PriceReport", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, list)
    assert len(dest) > 1
    assert all([os.path.exists(f) for f in dest])


def test_download_marketdata_b3_url_encoded():
    dest = download_marketdata("GetStockIndex")
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)


def test_download_marketdata_b3_url_encoded_with_null_argument():
    dest = download_marketdata("GetPortfolioDay_IndexStatistics", index="IBOV", year=2022)
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)

    dest = download_marketdata("GetListedSupplementCompany", issuingCompany="ABEV")
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)

    dest = download_marketdata("GetDetailsCompany", codeCVM="24910")
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)

    dest = download_marketdata("GetListedCashDividends", tradingName="ABEV")
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)

    dest = download_marketdata("GetTheoricalPortfolio", index="IBOV")
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)

    dest = download_marketdata("GetPortfolioDay", index="IBOV")
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)

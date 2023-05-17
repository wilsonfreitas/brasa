
from datetime import datetime
import os
import pandas as pd

import pytest
from brasa.engine import read_marketdata
from brasa.engine import CacheMetadata
from brasa.engine import MarketDataTemplate, TemplateFields, download_marketdata, retrieve_template


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
    assert len(df) > 0


def test_retrieve_temlate():
    tpl = retrieve_template("CDIIDI")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "CDIIDI"


def test_download_marketdata():
    dest = download_marketdata("CDIIDI")
    assert dest is not None
    assert os.path.exists(dest.downloaded_file_paths[0])


def test_download_marketdata_missing_args_error():
    with pytest.raises(ValueError) as exc_info:
        download_marketdata("OpcoesAcoesEmAberto")
    assert exc_info.value.args[0] == "Missing argument refdate"


def test_download_marketdata_with_refdate():
    dest = download_marketdata("OpcoesAcoesEmAberto", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])
    dest = download_marketdata("OpcoesAcoesEmAberto", refdate=datetime(2023, 5, 2))
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])

    dest = download_marketdata("NegociosBalcao", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])


def test_download_marketdata_with_refdate_and_unzip():
    dest = download_marketdata("COTAHIST_DAILY", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])


def test_download_marketdata_with_refdate_and_unzip_recursive_with_1_file():
    dest = download_marketdata("IndexReport", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert len(dest.downloaded_files) == 1
    assert os.path.exists(dest.downloaded_file_paths[0])


def test_download_marketdata_with_refdate_and_unzip_recursive_with_many_files():
    dest = download_marketdata("PriceReport", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert len(dest.downloaded_files) == 3
    assert os.path.exists(dest.downloaded_file_paths[0])


def test_download_marketdata_b3_url_encoded():
    dest = download_marketdata("GetStockIndex")
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])


def test_download_marketdata_b3_url_encoded_with_null_argument():
    dest = download_marketdata("GetPortfolioDay_IndexStatistics", index="IBOV", year=2022)
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])

    dest = download_marketdata("GetListedSupplementCompany", issuingCompany="ABEV")
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])

    dest = download_marketdata("GetDetailsCompany", codeCVM="24910")
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])

    dest = download_marketdata("GetListedCashDividends", tradingName="ABEV")
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])

    dest = download_marketdata("GetTheoricalPortfolio", index="IBOV")
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])

    dest = download_marketdata("GetPortfolioDay", index="IBOV")
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])


def test_read_marketdata():
    dest = "data/CDIIDI_2019-09-22.json"
    df = read_marketdata("CDIIDI", dest, parse_fields=False)
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 4)

    dest = "data/NegociosBalcao.csv"
    df = read_marketdata("NegociosBalcao", dest, parse_fields=False)
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (8, 11)


def test_read_marketdata_with_parsers():
    dest = "data/CDIIDI_2019-09-22.json"
    df = read_marketdata("CDIIDI", dest, parse_fields=True)
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (1, 4)
    assert df["dataTaxa"].dtype == "datetime64[ns]"

    dest = "data/NegociosBalcao.csv"
    df = read_marketdata("NegociosBalcao", dest, parse_fields=True)
    assert df is not None
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (8, 11)


def test_download_settlement_prices():
    dest = download_marketdata("b3-futures-settlement-prices", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, CacheMetadata)
    assert os.path.exists(dest.downloaded_file_paths[0])


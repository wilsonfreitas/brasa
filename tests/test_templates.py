
from datetime import datetime
import os
import pandas as pd

import pytest
from brasa.engine import CacheManager, read_marketdata
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
    meta = CacheMetadata("CDIIDI")
    download_marketdata(meta)
    man = CacheManager()
    man.save_meta(meta)
    assert len(meta.downloaded_files) == 1

    meta2 = man.load_meta("CDIIDI")
    assert meta2.timestamp == meta.timestamp
    assert meta2.template == meta.template


def test_download_marketdata_missing_args_error():
    with pytest.raises(ValueError) as exc_info:
        meta = CacheMetadata("OpcoesAcoesEmAberto")
        download_marketdata(meta)
    assert exc_info.value.args[0] == "Missing argument refdate"


def test_download_marketdata_with_refdate():
    meta = CacheMetadata("OpcoesAcoesEmAberto")
    download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1
    
    meta = CacheMetadata("OpcoesAcoesEmAberto")
    download_marketdata(meta, refdate=datetime(2023, 5, 2))
    assert len(meta.downloaded_files) == 1

    meta = CacheMetadata("NegociosBalcao")
    download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1


def test_download_marketdata_with_refdate_and_unzip():
    meta = CacheMetadata("COTAHIST_DAILY")
    download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1


def test_download_marketdata_with_refdate_and_unzip_recursive_with_1_file():
    meta = CacheMetadata("IndexReport")
    download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1


def test_download_marketdata_with_refdate_and_unzip_recursive_with_many_files():
    meta = CacheMetadata("PriceReport")
    download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 3


def test_download_marketdata_b3_url_encoded():
    meta = CacheMetadata("GetStockIndex")
    download_marketdata(meta)
    assert len(meta.downloaded_files) == 1


def test_download_marketdata_b3_url_encoded_with_null_argument():
    meta = CacheMetadata("GetPortfolioDay_IndexStatistics")
    download_marketdata(meta, index="IBOV", year=2022)
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("GetListedSupplementCompany")
    download_marketdata(meta, issuingCompany="ABEV")
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("GetDetailsCompany")
    download_marketdata(meta, codeCVM="24910")
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("GetListedCashDividends")
    download_marketdata(meta, tradingName="ABEV")
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("b3-theoretical-portfolio")
    download_marketdata(meta, index="IBOV")
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("GetPortfolioDay")
    download_marketdata(meta, index="IBOV")
    assert len(meta.downloaded_files) == 1

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


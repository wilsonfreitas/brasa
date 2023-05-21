
from datetime import datetime
import os
import pandas as pd

import pytest
from brasa.engine import CacheManager, get_marketdata, read_marketdata
from brasa.engine import CacheMetadata
from brasa.engine import MarketDataTemplate, TemplateFields, download_marketdata, retrieve_template


def test_load_template():
    tpl = MarketDataTemplate("templates/b3-cdi.yaml")

    assert tpl.has_downloader
    assert ~ tpl.has_reader


def test_template_load_fields():
    tpl = MarketDataTemplate("templates/b3-cdi.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    assert isinstance(tpl.fields, TemplateFields)
    assert len(tpl.fields) == 4
    assert tpl.fields["dataTaxa"].name == "dataTaxa"
    assert tpl.fields["dataTaxa"].description == "Data de divulgação da taxa DI"
    assert tpl.fields["dataTaxa"].handler.type == "Date"
    assert tpl.fields["dataTaxa"].handler.format == "%d/%m/%Y"


def test_retrieve_temlate():
    tpl = retrieve_template("b3-cdi")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "b3-cdi"


def test_get_marketdata():
    df = get_marketdata("b3-futures-settlement-prices", refdate=datetime(2023, 5, 19))
    assert isinstance(df, pd.DataFrame)
    df = get_marketdata("b3-cdi")
    assert isinstance(df, dict)


def test_save_empty_metadata():
    meta = CacheMetadata("b3-cdi")
    assert meta.id == "63142dbef63c0537fb3c2f37dac2fbb6"
    
    man = CacheManager()
    assert not man.has_meta(meta)
    man.save_meta(meta)
    assert man.has_meta(meta)

    man.remove_meta(meta)


def test_metadata_fulfilment():
    meta = CacheMetadata("b3-cdi")
    assert len(meta.downloaded_files) == 0

    download_marketdata(meta)
    assert len(meta.downloaded_files) == 1

    man = CacheManager()
    man.save_meta(meta)

    df = read_marketdata(meta)
    assert df is not None
    man.save_meta(meta)

    tpl = retrieve_template("b3-cdi")
    meta2 = CacheMetadata("b3-cdi")
    meta2.extra_key = tpl.downloader.extra_key

    man.load_meta(meta2)
    assert meta2.timestamp == meta.timestamp
    assert meta2.template == meta.template
    assert meta2.downloaded_files == meta.downloaded_files
    assert meta2.processed_files == meta.processed_files

    man.remove_meta(meta)


def test_download_marketdata_missing_args_error():
    with pytest.raises(ValueError) as exc_info:
        meta = CacheMetadata("b3-companies-options")
        download_marketdata(meta)
    assert exc_info.value.args[0] == "Missing argument refdate"


def test_download_marketdata_with_refdate():
    meta = CacheMetadata("b3-companies-options")
    download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1
    
    meta = CacheMetadata("b3-companies-options")
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
    df = read_marketdata("b3-cdi", dest, parse_fields=False)
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
    df = read_marketdata("b3-cdi", dest, parse_fields=True)
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


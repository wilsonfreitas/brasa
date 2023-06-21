
from datetime import datetime

import pytest
from brasa.engine import CacheMetadata, _download_marketdata


def test_download_marketdata_missing_args_error():
    with pytest.raises(ValueError) as exc_info:
        meta = CacheMetadata("b3-companies-options")
        _download_marketdata(meta)
    assert exc_info.value.args[0] == "Missing argument refdate"


def test_download_marketdata_with_refdate():
    meta = CacheMetadata("b3-companies-options")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1
    
    meta = CacheMetadata("b3-companies-options")
    _download_marketdata(meta, refdate=datetime(2023, 5, 2))
    assert len(meta.downloaded_files) == 1

    meta = CacheMetadata("b3-otc-trade-information")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1


def test_download_marketdata_with_refdate_and_unzip():
    meta = CacheMetadata("b3-cotahist-daily")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1


def test_download_marketdata_with_refdate_and_unzip_recursive_with_1_file():
    meta = CacheMetadata("b3-index-report")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1


def test_download_marketdata_with_refdate_and_unzip_recursive_with_many_files():
    meta = CacheMetadata("b3-price-report")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 3


def test_download_marketdata_b3_url_encoded():
    meta = CacheMetadata("b3-indexes-composition")
    _download_marketdata(meta)
    assert len(meta.downloaded_files) == 1


def test_download_marketdata_b3_url_encoded_with_null_argument():
    meta = CacheMetadata("b3-indexes-historical-prices")
    _download_marketdata(meta, index="IBOV", year=2022)
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("b3-company-info")
    _download_marketdata(meta, issuingCompany="ABEV")
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("b3-company-details")
    _download_marketdata(meta, codeCVM="24910")
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("b3-cash-dividends")
    _download_marketdata(meta, tradingName="ABEV")
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("b3-indexes-theoretical-portfolio")
    _download_marketdata(meta, index="IBOV")
    assert len(meta.downloaded_files) == 1
    meta = CacheMetadata("b3-indexes-theoretical-portfolio-with-sectors")
    _download_marketdata(meta, index="IBOV")
    assert len(meta.downloaded_files) == 1


def test_download_settlement_prices():
    meta = CacheMetadata("b3-futures-settlement-prices")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1

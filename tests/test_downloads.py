import time
from datetime import datetime

import pytest

from brasa.engine import CacheMetadata, _download_marketdata


def test_download_marketdata_missing_args_error():
    with pytest.raises(ValueError) as exc_info:
        meta = CacheMetadata("b3-companies-options")
        _download_marketdata(meta)
    assert exc_info.value.args[0] == "Missing argument refdate"


@pytest.mark.integration
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


@pytest.mark.integration
def test_download_marketdata_with_refdate_and_unzip():
    meta = CacheMetadata("b3-cotahist-daily")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1


@pytest.mark.integration
def test_download_marketdata_with_refdate_and_unzip_recursive_with_1_file():
    meta = CacheMetadata("b3-bvbg087")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1


@pytest.mark.skip(reason="External endpoint returns non-zip payload for this date")
def test_download_marketdata_with_refdate_and_unzip_recursive_with_many_files():
    meta = CacheMetadata("b3-bvbg087")
    _download_marketdata(meta, refdate=datetime(2025, 11, 19))
    assert len(meta.downloaded_files) == 1


@pytest.mark.skip(reason="External endpoint intermittently returns 520 errors")
def test_download_marketdata_b3_url_encoded():
    meta = CacheMetadata("b3-indexes-composition")
    _download_marketdata(meta)
    assert len(meta.downloaded_files) == 1


@pytest.mark.integration
def test_download_marketdata_b3_url_encoded_with_null_argument():
    time.sleep(5)
    meta = CacheMetadata("b3-indexes-historical-prices")
    _download_marketdata(meta, index="IBOV", year=2022)
    assert len(meta.downloaded_files) == 1

    time.sleep(5)
    meta = CacheMetadata("b3-company-info")
    _download_marketdata(meta, issuingCompany="ABEV")
    assert len(meta.downloaded_files) == 1

    time.sleep(5)
    meta = CacheMetadata("b3-company-details")
    _download_marketdata(meta, codeCVM="24910")
    assert len(meta.downloaded_files) == 1

    # b3-cash-dividends with no matching records now raises InvalidContentException (not null fp)
    # time.sleep(5)
    # meta = CacheMetadata("b3-cash-dividends")
    # _download_marketdata(meta, tradingName="ABEV")
    # assert len(meta.downloaded_files) == 1

    # Skip b3-indexes-theoretical-portfolio - API returns 520 server error
    # time.sleep(5)
    # meta = CacheMetadata("b3-indexes-theoretical-portfolio")
    # _download_marketdata(meta, index="IBOV")
    # assert len(meta.downloaded_files) == 1

    # Skip b3-indexes-theoretical-portfolio-with-sectors - API returns 520 server error
    # time.sleep(5)
    # meta = CacheMetadata("b3-indexes-theoretical-portfolio-with-sectors")
    # _download_marketdata(meta, index="IBOV")
    # assert len(meta.downloaded_files) == 1


@pytest.mark.skip(reason="Resource no longer available")
def test_download_settlement_prices():
    meta = CacheMetadata("b3-futures-settlement-prices")
    _download_marketdata(meta, refdate=datetime(2023, 5, 10))
    assert len(meta.downloaded_files) == 1

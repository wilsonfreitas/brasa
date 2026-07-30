import io
import time
from datetime import datetime

import pytest

from brasa.engine import CacheMetadata
from brasa.engine.download import _download_marketdata
from brasa.engine.exceptions import DownloadException
from brasa.engine.template import MarketDataDownloader


@pytest.mark.integration
def test_download_marketdata_with_refdate():
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


@pytest.mark.integration
def test_zip_download_checksum_is_stable_across_requests():
    """Two downloads of the same B3 zip must produce the same checksum,
    even though B3 regenerates the outer container between requests.

    The first call populates the raw folder. The second call is expected
    to raise DuplicatedFolderException because the content-based checksum
    matches — which is exactly what proves stability.
    """
    from brasa.engine.exceptions import DuplicatedFolderException

    meta1 = CacheMetadata("b3-bvbg086")
    _download_marketdata(meta1, refdate=datetime(2024, 3, 28))
    first_checksum = meta1.download_checksum
    assert first_checksum != ""

    time.sleep(5)
    meta2 = CacheMetadata("b3-bvbg086")
    with pytest.raises(DuplicatedFolderException):
        _download_marketdata(meta2, refdate=datetime(2024, 3, 28))
    assert meta2.download_checksum == first_checksum

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


# --- merged from tests/test_acquisition_override.py ---


def _make_downloader(**extra):
    spec = {"function": "brasa.downloaders.simple_download", "args": {}}
    spec.update(extra)
    return MarketDataDownloader(spec)


def test_download_uses_acquisition_function_override():
    from brasa.downloaders import simple_download

    called = {}

    def fake(md, **kw):
        called["yes"] = True
        return io.BytesIO(b"data"), {"src": "local"}

    dl = _make_downloader()
    fp, response, _retry = dl.download(acquisition_function=fake)

    assert called.get("yes") is True
    assert response == {"src": "local"}
    # shared object must not be mutated
    assert dl.download_function is simple_download


def test_download_retry_override_zero_means_no_retry():
    dl = _make_downloader(retry_attempts=5)
    attempts = {"n": 0}

    def failing(md, **kw):
        attempts["n"] += 1
        raise DownloadException("boom")

    with pytest.raises(DownloadException):
        dl.download(acquisition_function=failing, retry_attempts=0)

    assert attempts["n"] == 1

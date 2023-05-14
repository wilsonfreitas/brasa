
from typing import IO
from brasa.downloaders.downloaders import B3URLEncodedDownloader, DatetimeDownloader, SettlementPricesDownloader, SimpleDownloader


def simple_download(url, verify_ssl, **kwargs) -> IO | None:
    downloader = SimpleDownloader(url, verify_ssl, **kwargs)
    return downloader.download(), downloader.response


def datetime_download(url, verify_ssl, **kwargs) -> IO | None:
    downloader = DatetimeDownloader(url, verify_ssl, **kwargs)
    return downloader.download(), downloader.response


def b3_url_encoded_download(url, verify_ssl, **kwargs) -> IO | None:
    downloader = B3URLEncodedDownloader(url, verify_ssl, **kwargs)
    return downloader.download(), downloader.response


def settlement_prices_download(url, verify_ssl, **kwargs) -> IO | None:
    downloader = SettlementPricesDownloader(url, verify_ssl, **kwargs)
    return downloader.download(), downloader.response
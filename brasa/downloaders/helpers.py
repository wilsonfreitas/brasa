
from typing import IO
from brasa.downloaders.downloaders import B3URLEncodedDownloader, DatetimeDownloader, SettlementPricesDownloader, SimpleDownloader
from brasa.engine import MarketDataDownloader


def simple_download(md_downloader: MarketDataDownloader, **kwargs) -> tuple[IO | None, dict[str, str]]:
    downloader = SimpleDownloader(md_downloader.url, md_downloader.verify_ssl, **kwargs)
    return downloader.download(), dict(downloader.response.headers)


def datetime_download(md_downloader: MarketDataDownloader, **kwargs) -> tuple[IO | None, dict[str, str]]:
    downloader = DatetimeDownloader(md_downloader.url, md_downloader.verify_ssl, **kwargs)
    return downloader.download(), dict(downloader.response.headers)


def b3_url_encoded_download(md_downloader: MarketDataDownloader, **kwargs) -> tuple[IO | None, dict[str, str]]:
    downloader = B3URLEncodedDownloader(md_downloader.url, md_downloader.verify_ssl, **kwargs)
    return downloader.download(), dict(downloader.response.headers)


def settlement_prices_download(md_downloader: MarketDataDownloader, **kwargs) -> tuple[IO | None, dict[str, str]]:
    downloader = SettlementPricesDownloader(md_downloader.url, md_downloader.verify_ssl, **kwargs)
    return downloader.download(), dict(downloader.response.headers)
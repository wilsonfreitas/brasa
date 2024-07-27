import json
import io
from typing import IO
from brasa.downloaders.downloaders import (
    B3URLEncodedDownloader,
    DatetimeDownloader,
    SettlementPricesDownloader,
    SimpleDownloader,
    B3FilesURLDownloader,
    B3PagedURLEncodedDownloader,
)
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


def b3_paged_url_encoded_download(md_downloader: MarketDataDownloader, **kwargs) -> tuple[IO | None, dict[str, str]]:
    downloader = B3PagedURLEncodedDownloader(md_downloader.url, md_downloader.verify_ssl, **kwargs)
    return downloader.download(), dict(downloader.response.headers)


def settlement_prices_download(md_downloader: MarketDataDownloader, **kwargs) -> tuple[IO | None, dict[str, str]]:
    downloader = SettlementPricesDownloader(md_downloader.url, md_downloader.verify_ssl, **kwargs)
    return downloader.download(), dict(downloader.response.headers)


def b3_files_download(md_downloader: MarketDataDownloader, **kwargs) -> tuple[IO | None, dict[str, str]]:
    downloader = B3FilesURLDownloader(md_downloader.url, md_downloader.verify_ssl, **kwargs)
    return downloader.download(), dict(downloader.response.headers)


def validate_empty_file(fname: str) -> None:
    fp = open(fname, "rb")
    fp.seek(0, io.SEEK_END)
    size = fp.tell()
    fp.close()
    if size == 0:
        raise Exception("Downloaded file is empty")


def validate_json_empty_file(fname: str) -> None:
    fp = open(fname, "rb")
    if fp.readlines() == []:
        fp.close()
        raise Exception("JSON file is empty")
    fp.seek(0)
    obj = json.load(fp)
    fp.close()
    if len(obj) == 0:
        raise Exception("JSON file is empty")

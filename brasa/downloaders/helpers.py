
from typing import IO
from brasa.downloaders.downloaders import B3URLEncodedDownloader, DatetimeDownloader, SimpleDownloader


def simple_download(url, **kwargs) -> IO | None:
    downloader = SimpleDownloader(url=url, verify_ssl=kwargs["verify_ssl"])
    return downloader.download(), downloader.response


def datetime_download(url, **kwargs) -> IO | None:
    downloader = DatetimeDownloader(url=url, verify_ssl=kwargs["verify_ssl"], refdate=kwargs["refdate"])
    return downloader.download(), downloader.response


def b3_url_encoded_download(url, **kwargs) -> IO | None:
    downloader = B3URLEncodedDownloader(url=url, verify_ssl=kwargs["verify_ssl"])
    return downloader.download(), downloader.response



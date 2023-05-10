
from typing import IO
from brasa.downloaders.downloaders import DatetimeDownloader, SimpleDownloader


def simple_download(url, **kwargs) -> IO | None:
    downloader = SimpleDownloader(url=url, verify_ssl=kwargs["verify_ssl"])
    return downloader.download()


def datetime_download(url, **kwargs) -> IO | None:
    downloader = DatetimeDownloader(url=url, verify_ssl=kwargs["verify_ssl"], refdate=kwargs["refdate"])
    return downloader.download()



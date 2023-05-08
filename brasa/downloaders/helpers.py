
from typing import IO
from brasa.downloaders.downloaders import DatetimeDownloader, SimpleDownloader
from brasa.templates import MarketDataTemplate


def simple_download(template: MarketDataTemplate, **kwargs) -> IO | None:
    url = template.downloader["url"]
    downloader = SimpleDownloader(url=url, verify_ssl=template.verify_ssl)
    return downloader.download()


def datetime_download(template: MarketDataTemplate, **kwargs) -> IO | None:
    url = template.downloader["url"]
    downloader = DatetimeDownloader(url=url, verify_ssl=template.verify_ssl, refdate=kwargs["refdate"])
    return downloader.download()



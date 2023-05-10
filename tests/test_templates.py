
from brasa.templates import MarketDataTemplate, MarketDataDownloader


def test_load_template():
    tpl = MarketDataTemplate("templates/CDIIDI.yaml")

    assert tpl.has_downloader
    assert ~ tpl.has_reader


def test_download():
    tpl = MarketDataTemplate("templates/CDIIDI.yaml")

    assert tpl.has_downloader
    assert ~ tpl.has_reader

    dnd = MarketDataDownloader(tpl.downloader)
    fp = dnd.download()
    assert fp is not None
    assert fp.readable()


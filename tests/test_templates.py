
from brasa.templates import MarketDataTemplate


def test_load_template():
    tpl = MarketDataTemplate("templates/CDIIDI.yaml")

    assert tpl.has_downloader
    assert ~ tpl.has_reader
    assert callable(tpl.downloader)


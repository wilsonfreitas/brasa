
from datetime import datetime
import os
from brasa.templates import MarketDataTemplate, MarketDataDownloader, download_marketdata, retrieve_template


def test_load_template():
    tpl = MarketDataTemplate("templates/CDIIDI.yaml")

    assert tpl.has_downloader
    assert ~ tpl.has_reader


def test_download():
    tpl = MarketDataTemplate("templates/CDIIDI.yaml")

    assert tpl.has_downloader
    assert ~ tpl.has_reader

    dnd = MarketDataDownloader(tpl.downloader)
    fp, _ = dnd.download()
    assert fp is not None
    assert fp.readable()


def test_retrieve_temlate():
    tpl = retrieve_template("CDIIDI")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "CDIIDI"


def test_download_marketdata():
    dest = download_marketdata("CDIIDI")
    assert dest is not None
    assert os.path.exists(dest)


def test_download_marketdata_with_refdate():
    dest = download_marketdata("OpcoesAcoesEmAberto", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert os.path.exists(dest)
    dest = download_marketdata("OpcoesAcoesEmAberto", refdate=datetime(2023, 5, 2))
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)


def test_download_marketdata_with_refdate_and_unzip():
    dest = download_marketdata("COTAHIST_DAILY", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, list)
    assert len(dest) == 1
    assert os.path.exists(dest[0])


def test_download_marketdata_with_refdate_and_unzip_recursive_with_1_file():
    dest = download_marketdata("IndexReport", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, list)
    assert len(dest) == 1
    assert os.path.exists(dest[0])


def test_download_marketdata_with_refdate_and_unzip_recursive_with_many_files():
    dest = download_marketdata("PriceReport", refdate=datetime(2023, 5, 10))
    assert dest is not None
    assert isinstance(dest, list)
    assert len(dest) > 1
    assert all([os.path.exists(f) for f in dest])


def test_download_marketdata_b3_url_encoded():
    dest = download_marketdata("GetStockIndex")
    assert dest is not None
    assert isinstance(dest, str)
    assert os.path.exists(dest)


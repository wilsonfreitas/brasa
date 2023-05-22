
from datetime import datetime
import pandas as pd

from brasa.engine import CacheManager, get_marketdata, read_marketdata
from brasa.engine import CacheMetadata
from brasa.engine import MarketDataTemplate, TemplateFields, download_marketdata, retrieve_template


def test_load_template():
    tpl = MarketDataTemplate("templates/b3-cdi.yaml")

    assert tpl.has_downloader
    assert ~ tpl.has_reader


def test_template_load_fields():
    tpl = MarketDataTemplate("templates/b3-cdi.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    assert isinstance(tpl.fields, TemplateFields)
    assert len(tpl.fields) == 4
    assert tpl.fields["dataTaxa"].name == "dataTaxa"
    assert tpl.fields["dataTaxa"].description == "Data de divulgação da taxa DI"
    assert tpl.fields["dataTaxa"].handler.type == "Date"
    assert tpl.fields["dataTaxa"].handler.format == "%d/%m/%Y"


def test_retrieve_temlate():
    tpl = retrieve_template("b3-cdi")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "b3-cdi"


def test_get_marketdata():
    df = get_marketdata("b3-futures-settlement-prices", refdate=datetime(2023, 5, 19))
    assert isinstance(df, pd.DataFrame)
    df = get_marketdata("b3-cdi")
    assert isinstance(df, dict)


def test_save_empty_metadata():
    meta = CacheMetadata("b3-cdi")
    assert meta.id == "63142dbef63c0537fb3c2f37dac2fbb6"
    
    man = CacheManager()
    assert not man.has_meta(meta)
    man.save_meta(meta)
    assert man.has_meta(meta)

    man.remove_meta(meta)


def test_metadata_fulfilment():
    meta = CacheMetadata("b3-cdi")
    assert len(meta.downloaded_files) == 0

    download_marketdata(meta)
    assert len(meta.downloaded_files) == 1

    man = CacheManager()
    man.save_meta(meta)

    df = read_marketdata(meta)
    assert df is not None
    man.save_meta(meta)

    tpl = retrieve_template("b3-cdi")
    meta2 = CacheMetadata("b3-cdi")
    meta2.extra_key = tpl.downloader.extra_key

    man.load_meta(meta2)
    assert meta2.timestamp == meta.timestamp
    assert meta2.template == meta.template
    assert meta2.downloaded_files == meta.downloaded_files
    assert meta2.processed_files == meta.processed_files

    man.remove_meta(meta)

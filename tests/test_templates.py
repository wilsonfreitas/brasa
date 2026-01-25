from datetime import datetime

import pandas as pd
import pytest

from brasa.engine import (
    CacheManager,
    CacheMetadata,
    MarketDataTemplate,
    _download_marketdata,
    _read_marketdata,
    get_marketdata,
    retrieve_template,
)
from brasa.fieldsets import Fieldset


def test_load_template():
    tpl = MarketDataTemplate("templates/bcb-sgs-data.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader


def test_template_load_fields():
    tpl = MarketDataTemplate("templates/bcb-sgs-data.yaml")

    assert tpl.has_downloader
    assert tpl.has_reader
    # Template.fields is now a Fieldset
    assert isinstance(tpl.fields, Fieldset)
    assert len(tpl.fields) == 3
    assert tpl.fields["refdate"].name == "refdate"
    assert tpl.fields["refdate"].description == "Data de referência"
    # Field now has type_name instead of handler
    assert tpl.fields["refdate"].type_name == "date"
    assert tpl.fields["value"].type_name == "numeric"
    assert tpl.fields["code"].type_name == "integer"


def test_retrieve_temlate():
    tpl = retrieve_template("bcb-sgs-data")
    assert tpl is not None
    assert isinstance(tpl, MarketDataTemplate)
    assert tpl.id == "bcb-sgs-data"


@pytest.mark.skip(
    reason="External API issues: www2.bmf.com.br and www2.cetip.com.br are unreachable or have changed"
)
def test_get_marketdata():
    df = get_marketdata("b3-futures-settlement-prices", refdate=datetime(2023, 5, 19))
    assert isinstance(df, pd.DataFrame)
    df = get_marketdata("bcb-sgs-data", code=12, refdate=datetime(2023, 5, 19))
    assert isinstance(df, dict)


def test_save_empty_metadata():
    meta = CacheMetadata("bcb-sgs-data")
    assert meta.id == "fef009a135f746ed3216a0b87358422f"

    man = CacheManager()
    assert not man.has_meta(meta)
    man.save_meta(meta)
    assert man.has_meta(meta)

    man.remove_meta(meta)


@pytest.mark.skip(reason="External API issue: SGS endpoint is unstable in CI")
def test_metadata_fulfilment():
    meta = CacheMetadata("bcb-sgs-data")
    assert len(meta.downloaded_files) == 0

    _download_marketdata(meta)
    assert len(meta.downloaded_files) == 1

    man = CacheManager()
    man.save_meta(meta)

    df = _read_marketdata(meta)
    assert df is not None
    man.save_meta(meta)

    tpl = retrieve_template("bcb-sgs-data")
    meta2 = CacheMetadata("bcb-sgs-data")
    meta2.extra_key = tpl.downloader.extra_key

    man.load_meta(meta2)
    assert meta2.timestamp == meta.timestamp
    assert meta2.template == meta.template
    assert meta2.downloaded_files == meta.downloaded_files
    assert meta2.processed_files == meta.processed_files

    man.remove_meta(meta)


from datetime import datetime

import pandas as pd
from brasa.engine import FieldHandlerFactory


def test_field_handler_factory():
    factory = FieldHandlerFactory()
    handler = factory.create(None)
    assert handler.parse("a") == "a"
    assert handler.parse("123") == 123
    assert handler.parse("123.123") == 123.123

    handler = factory.create({"type": "character"})
    assert handler.parse("a") == "a"
    assert handler.parse("123") == "123"
    assert handler.parse("123.123") == "123.123"

    handler = factory.create({"type": "numeric"})
    assert handler.parse("a") == None
    assert handler.parse("123") == 123
    assert handler.parse("123.123") == 123.123

    handler = factory.create({"type": "numeric", "format": "pt-br"})
    assert handler.parse("a") == None
    assert handler.parse("123") == 123
    assert handler.parse("123,123") == 123.123
    assert handler.parse("123.123,123") == 123_123.123

    handler = factory.create({"type": "Date", "format": "%d/%m/%Y"})
    assert handler.parse("a") == None
    assert handler.parse("02/04/2022") == datetime(2022, 4, 2)


def test_field_handler_with_series():
    factory = FieldHandlerFactory()
    s = pd.Series(["a", "123", "123.123"])
    
    handler = factory.create(None)
    assert all(handler.parse(s) == pd.Series(["a", 123, 123.123]))

    handler = factory.create({"type": "character"})
    assert all(handler.parse(s) == s)

    handler = factory.create({"type": "numeric"})
    parsed = handler.parse(s)
    assert parsed.isna().sum() == 1
    assert all(parsed[~parsed.isna()].reset_index(drop=True) == pd.Series([123, 123.123]))

    s = pd.Series(["a", "123", "123,123", "123.123,123"])
    handler = factory.create({"type": "numeric", "format": "pt-br"})
    parsed = handler.parse(s)
    assert parsed.isna().sum() == 1
    assert all(parsed[~parsed.isna()].reset_index(drop=True) == pd.Series([123, 123.123, 123_123.123]))

    s = pd.Series(["a", "02/04/2022"])
    handler = factory.create({"type": "Date", "format": "%d/%m/%Y"})
    parsed = handler.parse(s)
    assert parsed.isna().sum() == 1
    assert all(parsed[~parsed.isna()].reset_index(drop=True) == pd.Series([datetime(2022, 4, 2)]))


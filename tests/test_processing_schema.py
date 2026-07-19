"""_get_schema_from_fields must warn when schema generation fails (audit Q5.4)."""

import logging

from brasa.engine.processing import _get_schema_from_fields


class BrokenFields:
    def __iter__(self):
        raise RuntimeError("broken fields")

    def __len__(self):
        return 1


def test_returns_none_and_warns_on_failure(caplog):
    with caplog.at_level(logging.WARNING, logger="brasa.engine.processing"):
        result = _get_schema_from_fields(BrokenFields())
    assert result is None
    assert any("schema" in rec.message.lower() for rec in caplog.records)


def test_returns_none_quietly_for_empty_fields(caplog):
    with caplog.at_level(logging.WARNING, logger="brasa.engine.processing"):
        assert _get_schema_from_fields(None) is None
    assert not caplog.records

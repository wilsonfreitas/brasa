"""Tests for FieldsetReader path handling (audit Q8.5)."""

import io
from pathlib import Path

import pytest

from brasa.fieldsets import Fieldset
from brasa.fieldsets.adapters.unified_reader import FieldsetReader


@pytest.fixture
def reader():
    return FieldsetReader(Fieldset(), verbose_warnings=False)


def test_get_filepath_accepts_str_and_path(reader, tmp_path):
    f = tmp_path / "data.csv"
    f.write_text("a,b\n1,2\n")
    assert reader._get_filepath(str(f)) == f
    assert reader._get_filepath(f) == f


def test_get_filepath_returns_none_for_buffer(reader):
    assert reader._get_filepath(io.StringIO("a,b\n")) is None


def test_get_filepath_missing_file_raises(reader, tmp_path):
    with pytest.raises(FileNotFoundError):
        reader._get_filepath(tmp_path / "missing.csv")

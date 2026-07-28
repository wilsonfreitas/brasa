"""Typed exceptions for content validation and parser failures (audit Q2.7)."""

import pytest

from brasa.downloaders.helpers import validate_empty_file, validate_json_empty_file
from brasa.engine.exceptions import InvalidContentException


def test_validate_empty_file_raises_invalid_content(tmp_path):
    f = tmp_path / "empty.bin"
    f.write_bytes(b"")
    with pytest.raises(InvalidContentException):
        validate_empty_file(str(f))


def test_validate_empty_file_passes_on_content(tmp_path):
    f = tmp_path / "data.bin"
    f.write_bytes(b"x")
    validate_empty_file(str(f))


def test_validate_json_empty_file_raises_invalid_content(tmp_path):
    for content in (b"", b"{}", b"[]"):
        f = tmp_path / "data.json"
        f.write_bytes(content)
        with pytest.raises(InvalidContentException):
            validate_json_empty_file(str(f))


def test_validate_json_empty_file_passes_on_content(tmp_path):
    f = tmp_path / "data.json"
    f.write_bytes(b'{"a": 1}')
    validate_json_empty_file(str(f))


def test_validate_json_empty_results_raises_invalid_content(tmp_path):
    for content in (
        b'{"page": {"totalPages": 1}, "results": []}',
        b'{"page": {"totalPages": 1}, "results": null}',
    ):
        f = tmp_path / "data.json"
        f.write_bytes(content)
        with pytest.raises(InvalidContentException):
            validate_json_empty_file(str(f))


def test_validate_json_non_empty_results_passes(tmp_path):
    f = tmp_path / "data.json"
    f.write_bytes(b'{"results": [{"a": 1}]}')
    validate_json_empty_file(str(f))


def test_validate_json_without_results_key_passes(tmp_path):
    for content in (b'{"a": 1}', b'[{"a": 1}]'):
        f = tmp_path / "data.json"
        f.write_bytes(content)
        validate_json_empty_file(str(f))

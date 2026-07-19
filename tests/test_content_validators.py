"""Typed exceptions for content validation and parser failures (audit Q2.7)."""

import io

import pytest

from brasa.downloaders.helpers import validate_empty_file, validate_json_empty_file
from brasa.engine.exceptions import CorruptedContentException, InvalidContentException
from brasa.parsers.b3.bvbg086 import BVBG086Parser


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


def test_bvbg086_parser_raises_corrupted_content_on_missing_tag():
    xml = (
        b'<?xml version="1.0"?>'
        b"<root><doc><exchange xmlns=\"urn:bvmf.052.01.xsd\"></exchange></doc></root>"
    )
    parser = BVBG086Parser(io.BytesIO(xml))
    with pytest.raises(CorruptedContentException):
        parser.parse()

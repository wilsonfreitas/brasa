"""Tests for the fixed-width-field parser (brasa.parsers.fwf)."""

from brasa.parsers.fwf import Field, FWFFile, FWFRow, NumericField


class SampleRow(FWFRow):
    _pattern = r"^01"
    rectype = Field(2)
    name = Field(4)
    value = NumericField(6, dec=2)


class SampleFile(FWFFile):
    sample = SampleRow()


def test_fwf_file_parses_text_file_from_str_path(tmp_path):
    """Regression for audit Q8.2: text-mode lines are str, so the
    bytes-only decode left _line unassigned and raised NameError."""
    fwf = tmp_path / "sample.txt"
    fwf.write_text("01ABCD001234\n01WXYZ000100\n", encoding="utf-8")

    parsed = SampleFile(str(fwf))

    df = parsed._tables["sample"]
    assert list(df["name"]) == ["ABCD", "WXYZ"]
    assert list(df["value"]) == [12.34, 1.0]


def test_fwf_file_parses_byte_iterator():
    lines = [b"01ABCD001234\n", b"01WXYZ000100\n"]

    parsed = SampleFile(lines)

    df = parsed._tables["sample"]
    assert list(df["name"]) == ["ABCD", "WXYZ"]
    assert list(df["value"]) == [12.34, 1.0]

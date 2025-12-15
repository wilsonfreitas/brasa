"""
Tests for BVBG087Parser to ensure parsing logic remains correct.

These tests validate the parser before migration to the pipeline framework,
ensuring backward compatibility and correct data extraction.
"""

import zipfile
from pathlib import Path

import pandas as pd
import pytest

from brasa.parsers.b3.bvbg087 import BVBG087Parser


@pytest.fixture
def bvbg087_test_file():
    """Get the path to the test BVBG087 file."""
    # Data is in project root's data folder
    data_dir = Path(__file__).parent.parent / "data"
    zip_path = data_dir / "IR210423.zip"

    if not zip_path.exists():
        pytest.skip(f"Test data file not found: {zip_path}")

    return zip_path


@pytest.fixture
def parser(bvbg087_test_file):
    """Create a parser instance from the test file."""
    with zipfile.ZipFile(bvbg087_test_file, "r") as zf:
        # Get the XML file name from the zip
        xml_files = [n for n in zf.namelist() if n.endswith(".xml")]
        assert len(xml_files) == 1, "Expected exactly one XML file in zip"

        with zf.open(xml_files[0]) as f:
            return BVBG087Parser(f)


class TestBVBG087ParserBasic:
    """Basic parser functionality tests."""

    def test_parser_returns_dict(self, parser):
        """Test that parser returns dict of DataFrames."""
        assert isinstance(parser.data, dict)

    def test_parser_has_all_dataset_keys(self, parser):
        """Test that parser returns all expected dataset keys."""
        expected_keys = {"IndxInf", "IOPVInf", "BDRInf"}
        # Parser may not have all keys if data is not present in file
        # but it should only have valid keys
        assert (
            set(parser.data.keys()).issubset(expected_keys)
            or set(parser.data.keys()) == expected_keys
        )

    def test_parser_data_are_dataframes(self, parser):
        """Test that all values in parser.data are DataFrames."""
        for key, df in parser.data.items():
            assert isinstance(df, pd.DataFrame), f"{key} is not a DataFrame"


class TestBVBG087ParserIndxInf:
    """Tests for IndxInf (indexes) dataset."""

    def test_indxinf_exists(self, parser):
        """Test that IndxInf dataset exists."""
        assert "IndxInf" in parser.data

    def test_indxinf_has_expected_columns(self, parser):
        """Test that IndxInf DataFrame has expected columns."""
        df = parser.data["IndxInf"]
        expected_columns = [
            "trade_date",
            "index_type",
            "ticker_symbol",
            "security_id",
            "security_proprietary",
            "security_market",
            "asset_desc",
            "settlement_price",
            "open_price",
            "min_price",
            "max_price",
            "average_price",
            "close_price",
            "last_price",
            "oscillation_val",
            "rising_shares_number",
            "falling_shares_number",
            "stable_shares_number",
        ]
        for col in expected_columns:
            assert col in df.columns, f"Column '{col}' not found in IndxInf"

    def test_indxinf_trade_date_populated(self, parser):
        """Test that trade_date is populated in IndxInf."""
        df = parser.data["IndxInf"]
        if len(df) > 0:
            assert df["trade_date"].notna().all(), "trade_date has null values"
            # Check date format (YYYY-MM-DD)
            assert df["trade_date"].iloc[0] == "2021-04-23"

    def test_indxinf_has_data(self, parser):
        """Test that IndxInf has actual data rows."""
        df = parser.data["IndxInf"]
        assert len(df) > 0, "IndxInf DataFrame is empty"


class TestBVBG087ParserIOPVInf:
    """Tests for IOPVInf (IOPV - Indicative Optimized Portfolio Value) dataset."""

    def test_iopvinf_exists(self, parser):
        """Test that IOPVInf dataset exists."""
        assert "IOPVInf" in parser.data

    def test_iopvinf_has_expected_columns(self, parser):
        """Test that IOPVInf DataFrame has expected columns."""
        df = parser.data["IOPVInf"]
        expected_columns = [
            "trade_date",
            "index_type",
            "ticker_symbol",
            "security_id",
            "security_proprietary",
            "security_market",
            "open_price",
            "min_price",
            "max_price",
            "average_price",
            "close_price",
            "last_price",
            "oscillation_val",
        ]
        for col in expected_columns:
            assert col in df.columns, f"Column '{col}' not found in IOPVInf"

    def test_iopvinf_trade_date_populated(self, parser):
        """Test that trade_date is populated in IOPVInf."""
        df = parser.data["IOPVInf"]
        if len(df) > 0:
            assert df["trade_date"].notna().all(), "trade_date has null values"


class TestBVBG087ParserBDRInf:
    """Tests for BDRInf (BDR reference prices) dataset."""

    def test_bdrinf_exists(self, parser):
        """Test that BDRInf dataset exists."""
        assert "BDRInf" in parser.data

    def test_bdrinf_has_expected_columns(self, parser):
        """Test that BDRInf DataFrame has expected columns."""
        df = parser.data["BDRInf"]
        expected_columns = [
            "trade_date",
            "index_type",
            "ticker_symbol",
            "security_id",
            "security_proprietary",
            "security_market",
            "ref_price",
        ]
        for col in expected_columns:
            assert col in df.columns, f"Column '{col}' not found in BDRInf"


class TestBVBG087ParserATTRS:
    """Tests for ATTRS class variable correctness."""

    def test_attrs_has_all_tags(self):
        """Test that ATTRS has all expected tag keys."""
        expected_tags = {"IndxInf", "IOPVInf", "BDRInf"}
        assert set(BVBG087Parser.ATTRS.keys()) == expected_tags

    def test_attrs_indxinf_has_required_fields(self):
        """Test that IndxInf ATTRS has required field mappings."""
        attrs = BVBG087Parser.ATTRS["IndxInf"]
        required_fields = ["ticker_symbol", "security_id", "settlement_price"]
        for field in required_fields:
            assert field in attrs, f"Field '{field}' not in IndxInf ATTRS"

    def test_attrs_iopvinf_has_required_fields(self):
        """Test that IOPVInf ATTRS has required field mappings."""
        attrs = BVBG087Parser.ATTRS["IOPVInf"]
        required_fields = ["ticker_symbol", "security_id", "last_price"]
        for field in required_fields:
            assert field in attrs, f"Field '{field}' not in IOPVInf ATTRS"

    def test_attrs_bdrinf_has_required_fields(self):
        """Test that BDRInf ATTRS has required field mappings."""
        attrs = BVBG087Parser.ATTRS["BDRInf"]
        required_fields = ["ticker_symbol", "security_id", "ref_price"]
        for field in required_fields:
            assert field in attrs, f"Field '{field}' not in BDRInf ATTRS"


class TestBVBG087ParserMode:
    """Tests for parser file mode."""

    def test_parser_mode_is_binary(self):
        """Test that parser mode is set to binary read."""
        assert BVBG087Parser.mode == "rb"

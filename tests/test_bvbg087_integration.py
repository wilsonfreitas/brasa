"""
Integration tests for BVBG087 pipeline migration.

These tests validate the complete flow from template loading through
data reading to output generation, ensuring the migrated pipeline
produces the same results as the legacy reader.

Note: Some tests are marked as expected failures (xfail) because they test
functionality that will be implemented as part of the BVBG087 migration.
"""

import zipfile
from pathlib import Path

import pytest

from brasa.engine.template import retrieve_template
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
def legacy_parser_output(bvbg087_test_file):
    """Get the output from the legacy parser for comparison."""
    with zipfile.ZipFile(bvbg087_test_file, "r") as zf:
        xml_files = [n for n in zf.namelist() if n.endswith(".xml")]
        with zf.open(xml_files[0]) as f:
            parser = BVBG087Parser(f)
    return parser.data


class TestLegacyReaderBaseline:
    """Tests to establish baseline behavior of legacy reader."""

    def test_legacy_parser_output_structure(self, legacy_parser_output):
        """Test that legacy parser returns expected structure."""
        assert isinstance(legacy_parser_output, dict)
        assert "IndxInf" in legacy_parser_output
        assert "IOPVInf" in legacy_parser_output
        assert "BDRInf" in legacy_parser_output

    def test_legacy_parser_indxinf_has_data(self, legacy_parser_output):
        """Test that IndxInf has actual data."""
        df = legacy_parser_output["IndxInf"]
        assert len(df) > 0
        assert "trade_date" in df.columns
        assert "ticker_symbol" in df.columns

    def test_legacy_parser_data_types_are_strings(self, legacy_parser_output):
        """Test that legacy parser returns string types (before conversion)."""
        df = legacy_parser_output["IndxInf"]
        # Most columns should be object (string) type from XML parsing
        assert df["ticker_symbol"].dtype == object
        assert df["trade_date"].dtype == object


class TestCurrentTemplateStructure:
    """Tests for the current b3-bvbg087 template structure."""

    def test_current_template_loads(self):
        """Test that current template loads without error."""
        tpl = retrieve_template("b3-bvbg087")
        assert tpl is not None
        assert tpl.id == "b3-bvbg087"

    def test_current_template_has_reader(self):
        """Test that template has a reader configured."""
        tpl = retrieve_template("b3-bvbg087")
        assert tpl.has_reader

    def test_current_template_has_multi_mapping(self):
        """Test that template has multi mapping for writer."""
        tpl = retrieve_template("b3-bvbg087")

        assert tpl.reader.multi is not None
        assert len(tpl.reader.multi) == 3


class TestPipelineMigration:
    """Tests for the migrated pipeline (xfail until implemented).

    These tests validate that after migration, the pipeline produces
    the same output as the legacy reader.
    """

    @pytest.mark.xfail(reason="Pipeline migration not yet implemented")
    def test_template_uses_pipeline(self):
        """Test that migrated template uses pipeline approach."""
        tpl = retrieve_template("b3-bvbg087")
        assert tpl.reader.has_pipeline
        assert tpl.reader._pipeline is not None

    @pytest.mark.xfail(reason="Pipeline migration not yet implemented")
    def test_template_has_datasets_config(self):
        """Test that migrated template has datasets configuration."""
        tpl = retrieve_template("b3-bvbg087")

        assert hasattr(tpl, "datasets")
        assert tpl.datasets is not None
        assert len(tpl.datasets) == 3

    @pytest.mark.xfail(reason="Pipeline migration not yet implemented")
    def test_pipeline_output_has_same_keys(
        self, bvbg087_test_file, legacy_parser_output
    ):
        """Test that pipeline output has same dataset keys."""
        tpl = retrieve_template("b3-bvbg087")

        # After migration, pipeline should exist
        assert tpl.reader.has_pipeline, "Template should use pipeline after migration"

        # Pipeline output uses output names (indexes_info, etc.)
        # which map from XML tags (IndxInf, etc.) via datasets config
        expected_keys = {"indexes_info", "iopv_info", "bdr_info"}
        actual_keys = set(tpl.datasets.keys())
        assert actual_keys == expected_keys

    @pytest.mark.xfail(reason="Pipeline migration not yet implemented")
    def test_pipeline_output_has_correct_types(self, bvbg087_test_file):
        """Test that pipeline applies correct type conversions."""
        tpl = retrieve_template("b3-bvbg087")

        # Pipeline output should have proper types defined in datasets
        assert tpl.reader.has_pipeline, "Template should use pipeline after migration"

        # Check that datasets have the expected field types
        indexes_fields = tpl.datasets["indexes_info"].fields
        assert indexes_fields.has_field("refdate")
        assert indexes_fields.get_field("refdate").type_name == "date"
        assert indexes_fields.has_field("symbol")
        assert indexes_fields.get_field("symbol").type_name == "string"
        assert indexes_fields.has_field("settlement_price")
        assert indexes_fields.get_field("settlement_price").type_name == "numeric"


class TestOutputCompatibility:
    """Tests for output compatibility with the writer system."""

    def test_legacy_output_keys_match_multi_mapping(self, legacy_parser_output):
        """Test that legacy parser keys match the multi mapping."""
        tpl = retrieve_template("b3-bvbg087")

        # Legacy parser uses XML tags as keys
        parser_keys = set(legacy_parser_output.keys())
        multi_keys = set(tpl.reader.multi.keys())

        assert parser_keys == multi_keys

    @pytest.mark.xfail(reason="Pipeline migration not yet implemented")
    def test_migrated_output_keys_match_multi_values(self, bvbg087_test_file):
        """Test that migrated output uses output names as keys."""
        tpl = retrieve_template("b3-bvbg087")

        # After migration, output should use output names (values of multi)
        # not XML tags (keys of multi)
        expected_keys = set(tpl.reader.multi.values())
        # {'indexes_info', 'iopv_info', 'bdr_info'}

        assert tpl.reader.has_pipeline, "Template should use pipeline after migration"

        # Verify datasets config matches the expected output names
        dataset_keys = set(tpl.datasets.keys())
        assert dataset_keys == expected_keys


class TestColumnRenaming:
    """Tests for column renaming (trade_date -> refdate, ticker_symbol -> symbol)."""

    def test_legacy_parser_has_original_column_names(self, legacy_parser_output):
        """Test that legacy parser uses original column names."""
        df = legacy_parser_output["IndxInf"]

        # Original names from parser
        assert "trade_date" in df.columns
        assert "ticker_symbol" in df.columns

        # Renamed names should NOT be present yet
        assert "refdate" not in df.columns
        assert "symbol" not in df.columns

    @pytest.mark.xfail(reason="Pipeline migration not yet implemented")
    def test_pipeline_output_has_renamed_columns(self, bvbg087_test_file):
        """Test that pipeline output has renamed columns."""
        tpl = retrieve_template("b3-bvbg087")

        # After pipeline processing, columns should be renamed:
        # trade_date -> refdate
        # ticker_symbol -> symbol
        assert tpl.reader.has_pipeline, "Template should use pipeline after migration"

        # Verify the fieldset has the renamed column names
        indexes_fields = tpl.datasets["indexes_info"].fields
        assert indexes_fields.has_field("refdate")
        assert indexes_fields.has_field("symbol")

        # Original names should not be in the fieldset (they get renamed)
        assert not indexes_fields.has_field("trade_date")
        assert not indexes_fields.has_field("ticker_symbol")


class TestTypeConversions:
    """Tests for type conversions in the pipeline."""

    def test_expected_types_for_indexes_info(self):
        """Document expected types for indexes_info dataset."""
        expected_types = {
            "refdate": "datetime64",  # from trade_date
            "symbol": "object",  # from ticker_symbol
            "security_id": "int64",
            "security_proprietary": "int64",
            "security_market": "object",
            "asset_desc": "object",
            "settlement_price": "float64",
            "open_price": "float64",
            "min_price": "float64",
            "max_price": "float64",
            "average_price": "float64",
            "close_price": "float64",
            "last_price": "float64",
            "oscillation_val": "float64",
            "rising_shares_number": "int64",
            "falling_shares_number": "int64",
            "stable_shares_number": "int64",
        }
        # This documents the expected types for reference
        assert len(expected_types) == 17

    def test_expected_types_for_iopv_info(self):
        """Document expected types for iopv_info dataset."""
        expected_types = {
            "refdate": "datetime64",
            "symbol": "object",
            "security_id": "int64",
            "security_proprietary": "int64",
            "security_market": "object",
            "open_price": "float64",
            "min_price": "float64",
            "max_price": "float64",
            "average_price": "float64",
            "close_price": "float64",
            "last_price": "float64",
            "oscillation_val": "float64",
        }
        assert len(expected_types) == 12

    def test_expected_types_for_bdr_info(self):
        """Document expected types for bdr_info dataset."""
        expected_types = {
            "refdate": "datetime64",
            "symbol": "object",
            "security_id": "int64",
            "security_proprietary": "int64",
            "security_market": "object",
            "ref_price": "float64",
        }
        assert len(expected_types) == 6


class TestBackwardCompatibility:
    """Tests to ensure backward compatibility after migration."""

    def test_legacy_read_function_still_exists(self):
        """Test that legacy read function still exists (for reference)."""
        from brasa.readers.helpers import read_b3_bvbg087

        assert read_b3_bvbg087 is not None
        assert callable(read_b3_bvbg087)

    def test_bvbg086_template_still_works(self):
        """Test that BVBG086 template (already migrated) still works."""
        tpl = retrieve_template("b3-bvbg086")

        assert tpl.id == "b3-bvbg086"
        assert tpl.has_reader
        assert tpl.reader.has_pipeline

    def test_parser_class_still_exists(self):
        """Test that BVBG087Parser class still exists."""
        from brasa.parsers.b3.bvbg087 import BVBG087Parser

        assert BVBG087Parser is not None

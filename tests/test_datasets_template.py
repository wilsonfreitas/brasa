"""
Tests for MarketDataTemplate with datasets configuration.

These tests validate the new `datasets` section parsing in templates,
which is needed for multi-output templates like BVBG087.

Note: Some tests are marked as expected failures (xfail) because they test
functionality that will be implemented as part of the BVBG087 migration.
"""

import pytest

from brasa.engine.template import retrieve_template
from brasa.fieldsets import Fieldset


class TestTemplateWithSingleFields:
    """Tests for templates with single `fields` section (existing behavior)."""

    def test_template_with_fields_list(self):
        """Test loading template with fields as a list."""
        tpl = retrieve_template("b3-bvbg086")

        assert hasattr(tpl, "fields")
        assert isinstance(tpl.fields, Fieldset)
        assert len(tpl.fields) > 0

    def test_template_fields_passed_to_reader(self):
        """Test that fields are passed to the reader."""
        tpl = retrieve_template("b3-bvbg086")

        assert tpl.reader.fields is not None
        assert isinstance(tpl.reader.fields, Fieldset)

    def test_template_no_datasets_attribute(self):
        """Test that single-field templates don't have datasets."""
        tpl = retrieve_template("b3-bvbg086")

        # Should not have datasets attribute or it should be None/empty
        datasets = getattr(tpl, "datasets", None)
        assert datasets is None or len(datasets) == 0


class TestTemplateWithDatasets:
    """Tests for templates with `datasets` section (new behavior).

    These tests are marked as xfail because the datasets functionality
    will be implemented as part of the BVBG087 migration.
    """

    @pytest.mark.xfail(reason="datasets parsing not yet implemented")
    def test_template_with_datasets_dict(self):
        """Test loading template with datasets configuration."""
        tpl = retrieve_template("b3-bvbg087")

        assert hasattr(tpl, "datasets")
        assert tpl.datasets is not None
        assert len(tpl.datasets) == 3

    @pytest.mark.xfail(reason="datasets parsing not yet implemented")
    def test_datasets_has_expected_keys(self):
        """Test that datasets has the expected output names as keys."""
        tpl = retrieve_template("b3-bvbg087")

        expected_keys = {"indexes_info", "iopv_info", "bdr_info"}
        assert set(tpl.datasets.keys()) == expected_keys

    @pytest.mark.xfail(reason="datasets parsing not yet implemented")
    def test_datasets_have_tag_attribute(self):
        """Test that each dataset config has its XML tag."""
        tpl = retrieve_template("b3-bvbg087")

        # Check tag mapping
        assert tpl.datasets["indexes_info"].tag == "IndxInf"
        assert tpl.datasets["iopv_info"].tag == "IOPVInf"
        assert tpl.datasets["bdr_info"].tag == "BDRInf"

    @pytest.mark.xfail(reason="datasets parsing not yet implemented")
    def test_datasets_have_fieldset(self):
        """Test that each dataset has a Fieldset."""
        tpl = retrieve_template("b3-bvbg087")

        for name, config in tpl.datasets.items():
            assert hasattr(config, "fields"), f"Dataset '{name}' missing fields"
            assert isinstance(config.fields, Fieldset), (
                f"Dataset '{name}' fields is not a Fieldset"
            )
            assert len(config.fields) > 0, f"Dataset '{name}' has empty fieldset"

    @pytest.mark.xfail(reason="datasets parsing not yet implemented")
    def test_datasets_fieldset_has_correct_types(self):
        """Test that fieldset fields have correct type definitions."""
        tpl = retrieve_template("b3-bvbg087")

        # Check indexes_info fieldset
        indexes_fieldset = tpl.datasets["indexes_info"].fields
        assert indexes_fieldset.has_field("refdate")
        assert indexes_fieldset.get_field("refdate").type_name == "date"

        assert indexes_fieldset.has_field("symbol")
        assert indexes_fieldset.get_field("symbol").type_name == "string"

        assert indexes_fieldset.has_field("settlement_price")
        assert indexes_fieldset.get_field("settlement_price").type_name == "numeric"

    @pytest.mark.xfail(reason="datasets parsing not yet implemented")
    def test_datasets_builds_multi_mapping(self):
        """Test that reader.multi is built from datasets config.

        After migration, reader.multi should be auto-generated from datasets,
        not manually specified in the template YAML.
        """
        tpl = retrieve_template("b3-bvbg087")

        # After migration, datasets should exist and multi should be derived from it
        assert hasattr(tpl, "datasets") and tpl.datasets is not None
        assert len(tpl.datasets) == 3

        # reader.multi should map tag -> output_name (derived from datasets)
        assert tpl.reader.multi is not None
        assert len(tpl.reader.multi) == 3

        # Check mapping (tag -> output_name)
        assert tpl.reader.multi.get("IndxInf") == "indexes_info"
        assert tpl.reader.multi.get("IOPVInf") == "iopv_info"
        assert tpl.reader.multi.get("BDRInf") == "bdr_info"


class TestTemplateMutualExclusivity:
    """Tests for fields vs datasets mutual exclusivity."""

    def test_single_fields_template_structure(self):
        """Test that single-field template has fields but no datasets."""
        tpl = retrieve_template("b3-bvbg086")

        assert tpl.fields is not None
        assert len(tpl.fields) > 0

        datasets = getattr(tpl, "datasets", None)
        assert datasets is None or len(datasets) == 0

    @pytest.mark.xfail(reason="datasets parsing not yet implemented")
    def test_datasets_template_structure(self):
        """Test that datasets template has datasets but fields is None/empty."""
        tpl = retrieve_template("b3-bvbg087")

        assert tpl.datasets is not None
        assert len(tpl.datasets) > 0

        # When using datasets, top-level fields should be None or empty
        # (fields are inside each dataset)
        assert tpl.fields is None or len(tpl.fields) == 0


class TestDatasetConfigClass:
    """Tests for the DatasetConfig dataclass.

    These tests validate the DatasetConfig class that will be created
    to hold dataset configuration.
    """

    @pytest.mark.xfail(reason="DatasetConfig not yet implemented")
    def test_dataset_config_import(self):
        """Test that DatasetConfig can be imported."""
        from brasa.engine.template import DatasetConfig

        assert DatasetConfig is not None

    @pytest.mark.xfail(reason="DatasetConfig not yet implemented")
    def test_dataset_config_attributes(self):
        """Test DatasetConfig has required attributes."""
        from brasa.engine.template import DatasetConfig

        fieldset = Fieldset()
        config = DatasetConfig(name="test_dataset", tag="TestTag", fields=fieldset)

        assert config.name == "test_dataset"
        assert config.tag == "TestTag"
        assert config.fields is fieldset


class TestBackwardCompatibility:
    """Tests to ensure existing templates still work after changes."""

    def test_bvbg086_template_loads(self):
        """Test that BVBG086 template loads correctly."""
        tpl = retrieve_template("b3-bvbg086")

        assert tpl.id == "b3-bvbg086"
        assert tpl.has_reader
        assert tpl.reader.has_pipeline

    def test_bvbg086_template_has_fields(self):
        """Test that BVBG086 template has fields."""
        tpl = retrieve_template("b3-bvbg086")

        assert tpl.fields is not None
        assert len(tpl.fields) > 0

    def test_cdi_template_loads(self):
        """Test that simple template loads correctly."""
        tpl = retrieve_template("bcb-sgs-data")

        assert tpl.id == "bcb-sgs-data"
        assert tpl.has_downloader

    def test_futures_settlement_template_loads(self):
        """Test that futures settlement template loads correctly."""
        tpl = retrieve_template("b3-futures-settlement-prices")

        assert tpl.id == "b3-futures-settlement-prices"
        assert tpl.has_reader

    def test_current_bvbg087_template_loads(self):
        """Test that current BVBG087 template loads (before migration)."""
        tpl = retrieve_template("b3-bvbg087")

        assert tpl.id == "b3-bvbg087"
        assert tpl.has_reader
        # Currently uses function-based reader
        assert tpl.reader.read_function is not None or tpl.reader.has_pipeline

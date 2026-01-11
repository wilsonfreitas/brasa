"""Tests for the data layer functionality.

These tests validate the DataLayer enum, layer parsing in templates,
and layer-aware path resolution in CacheManager and queries.
"""

import pytest

from brasa.engine import (
    CacheManager,
    DataLayer,
    MarketDataWriter,
    retrieve_template,
)
from brasa.engine.layers import (
    DEFAULT_ETL_LAYER,
    DEFAULT_LAYER,
    LAYER_ORDER,
)


class TestDataLayerEnum:
    """Tests for the DataLayer enum."""

    def test_data_layer_values(self):
        """Test that DataLayer has the expected values."""
        assert DataLayer.RAW.value == "raw"
        assert DataLayer.INPUT.value == "input"
        assert DataLayer.STAGING.value == "staging"
        assert DataLayer.CURATED.value == "curated"

    def test_data_layer_from_string(self):
        """Test converting strings to DataLayer enum."""
        assert DataLayer.from_string("input") == DataLayer.INPUT
        assert DataLayer.from_string("INPUT") == DataLayer.INPUT
        assert DataLayer.from_string("staging") == DataLayer.STAGING
        assert DataLayer.from_string("curated") == DataLayer.CURATED
        assert DataLayer.from_string("raw") == DataLayer.RAW

    def test_data_layer_from_string_default(self):
        """Test that None/empty string defaults to INPUT."""
        assert DataLayer.from_string(None) == DataLayer.INPUT
        assert DataLayer.from_string("") == DataLayer.INPUT

    def test_data_layer_from_string_invalid(self):
        """Test that invalid layer raises ValueError."""
        with pytest.raises(ValueError, match="Invalid layer"):
            DataLayer.from_string("invalid")

    def test_data_layer_str(self):
        """Test string representation of DataLayer."""
        assert str(DataLayer.INPUT) == "input"
        assert str(DataLayer.STAGING) == "staging"

    def test_layer_order(self):
        """Test that layer order is correctly defined."""
        assert len(LAYER_ORDER) == 4
        assert LAYER_ORDER[0] == DataLayer.RAW
        assert LAYER_ORDER[1] == DataLayer.INPUT
        assert LAYER_ORDER[2] == DataLayer.STAGING
        assert LAYER_ORDER[3] == DataLayer.CURATED

    def test_default_layers(self):
        """Test default layer values."""
        assert DEFAULT_LAYER == DataLayer.INPUT
        assert DEFAULT_ETL_LAYER == DataLayer.STAGING


class TestMarketDataWriterLayer:
    """Tests for layer parsing in MarketDataWriter."""

    def test_writer_default_layer(self):
        """Test that writer defaults to INPUT layer."""
        writer = MarketDataWriter({"partitioning": ["refdate"]})
        assert writer.layer == DataLayer.INPUT

    def test_writer_explicit_layer_input(self):
        """Test explicit input layer."""
        writer = MarketDataWriter({"layer": "input", "partitioning": []})
        assert writer.layer == DataLayer.INPUT

    def test_writer_explicit_layer_staging(self):
        """Test explicit staging layer."""
        writer = MarketDataWriter({"layer": "staging", "partitioning": []})
        assert writer.layer == DataLayer.STAGING

    def test_writer_explicit_layer_curated(self):
        """Test explicit curated layer."""
        writer = MarketDataWriter({"layer": "curated", "partitioning": []})
        assert writer.layer == DataLayer.CURATED

    def test_writer_layer_setter_string(self):
        """Test setting layer with string."""
        writer = MarketDataWriter({"partitioning": []})
        writer.layer = "staging"
        assert writer.layer == DataLayer.STAGING

    def test_writer_layer_setter_enum(self):
        """Test setting layer with enum."""
        writer = MarketDataWriter({"partitioning": []})
        writer.layer = DataLayer.CURATED
        assert writer.layer == DataLayer.CURATED


class TestTemplateLayer:
    """Tests for layer handling in templates."""

    def test_template_with_reader_has_default_input_layer(self):
        """Test that templates with readers default to INPUT layer."""
        tpl = retrieve_template("b3-bvbg086")
        assert hasattr(tpl, "writer")
        assert tpl.writer.layer == DataLayer.INPUT

    def test_etl_template_defaults_to_staging(self):
        """Test that ETL templates default to STAGING layer."""
        tpl = retrieve_template("b3-cotahist")
        assert tpl.is_etl
        assert hasattr(tpl, "writer")
        assert tpl.writer.layer == DataLayer.STAGING

    def test_template_with_explicit_writer_layer(self):
        """Test that templates respect explicit layer in writer."""
        # b3-futures-di1-consolidated has writer section and is an ETL template
        tpl = retrieve_template("b3-futures-di1-consolidated")
        assert hasattr(tpl, "writer")
        # This template is ETL so defaults to staging
        assert tpl.writer.layer == DataLayer.STAGING


class TestCacheManagerLayerPaths:
    """Tests for layer-aware path resolution in CacheManager."""

    def test_db_folder_includes_layer(self):
        """Test that db_folder includes layer in path."""
        tpl = retrieve_template("b3-bvbg086")
        man = CacheManager()
        folder = man.db_folder(tpl)
        assert "input" in folder
        assert "b3-bvbg086" in folder

    def test_db_folder_etl_template_staging(self):
        """Test that ETL templates use staging layer in path."""
        tpl = retrieve_template("b3-cotahist")
        man = CacheManager()
        folder = man.db_folder(tpl)
        assert "staging" in folder
        assert "b3-cotahist" in folder

    def test_db_folders_includes_layer(self):
        """Test that db_folders includes layer in paths."""
        # Use a template with datasets or multi output
        try:
            tpl = retrieve_template("b3-bvbg087")
            if tpl.datasets:
                man = CacheManager()
                folders = man.db_folders(tpl)
                for _name, folder in folders.items():
                    assert "input" in folder or "staging" in folder
        except ValueError:
            # Template may not exist in all test environments
            pytest.skip("b3-bvbg087 template not available")


class TestGetDatasetLayer:
    """Tests for get_dataset layer resolution."""

    def test_get_template_layer_input(self):
        """Test getting layer from input template."""
        from brasa.queries import get_template_layer

        layer = get_template_layer("b3-bvbg086")
        assert layer == "input"

    def test_get_template_layer_etl(self):
        """Test getting layer from ETL template."""
        from brasa.queries import get_template_layer

        layer = get_template_layer("b3-cotahist")
        assert layer == "staging"

    def test_get_template_layer_not_found(self):
        """Test that nonexistent template returns None."""
        from brasa.queries import get_template_layer

        layer = get_template_layer("nonexistent-template")
        assert layer is None


class TestWriterDatasetAttribute:
    """Tests for the dataset attribute in MarketDataWriter."""

    def test_writer_default_dataset_empty(self):
        """Test that writer.dataset defaults to empty when no template_id."""
        writer = MarketDataWriter({"partitioning": []})
        # Without template_id, dataset is empty string
        assert writer.dataset == ""

    def test_writer_default_dataset_from_template_id(self):
        """Test that writer.dataset defaults to template_id."""
        writer = MarketDataWriter({"partitioning": []}, template_id="my-template")
        assert writer.dataset == "my-template"

    def test_writer_explicit_dataset(self):
        """Test explicit dataset attribute."""
        writer = MarketDataWriter(
            {"dataset": "my-dataset", "partitioning": []},
            template_id="my-template",
        )
        assert writer.dataset == "my-dataset"

    def test_writer_dataset_setter(self):
        """Test setting dataset attribute."""
        writer = MarketDataWriter({"partitioning": []}, template_id="my-template")
        writer.dataset = "new-dataset"
        assert writer.dataset == "new-dataset"

    def test_template_dataset_defaults_to_id(self):
        """Test that template.writer.dataset defaults to template.id."""
        tpl = retrieve_template("b3-bvbg086")
        assert tpl.writer.dataset == "b3-bvbg086"

    def test_get_template_dataset_function(self):
        """Test get_template_dataset function."""
        from brasa.queries import get_template_dataset

        dataset = get_template_dataset("b3-bvbg086")
        assert dataset == "b3-bvbg086"

    def test_get_template_dataset_not_found(self):
        """Test get_template_dataset for nonexistent template."""
        from brasa.queries import get_template_dataset

        dataset = get_template_dataset("nonexistent-template")
        assert dataset is None


class TestCacheManagerDatasetPaths:
    """Tests for dataset-aware path resolution in CacheManager."""

    def test_db_folder_uses_dataset_name(self):
        """Test that db_folder uses writer.dataset."""
        tpl = retrieve_template("b3-bvbg086")
        man = CacheManager()
        folder = man.db_folder(tpl)
        # Should use writer.dataset (which defaults to template.id)
        assert "b3-bvbg086" in folder
        assert "input" in folder

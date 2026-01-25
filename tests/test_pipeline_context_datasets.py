"""
Tests for PipelineContext with datasets support.

These tests validate the new `datasets` attribute in PipelineContext,
which is needed for multi-output pipelines like BVBG087.

Note: Some tests are marked as expected failures (xfail) because they test
functionality that will be implemented as part of the BVBG087 migration.
"""

from unittest.mock import MagicMock

import pytest

from brasa.engine.pipeline.context import PipelineContext
from brasa.fieldsets import Fieldset


@pytest.fixture
def mock_meta():
    """Create a mock CacheMetadata object."""
    meta = MagicMock()
    meta.downloaded_files = ["test_file.xml"]
    meta.template = "test-template"
    return meta


@pytest.fixture
def sample_fieldset():
    """Create a sample Fieldset for testing."""
    from brasa.fieldsets import Field

    fieldset = Fieldset()
    fieldset.add_fields(
        Field(name="refdate", description="Reference date", type_definition="date"),
        Field(name="symbol", description="Symbol", type_definition="string"),
        Field(name="price", description="Price", type_definition="numeric"),
    )
    return fieldset


class TestPipelineContextBasic:
    """Tests for basic PipelineContext functionality (existing behavior)."""

    def test_context_creation(self, mock_meta):
        """Test that PipelineContext can be created."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
        )

        assert context.meta is mock_meta
        assert context.reader_config == {}

    def test_context_with_single_fieldset(self, mock_meta, sample_fieldset):
        """Test that context works with single fields attribute."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            fields=sample_fieldset,
        )

        assert context.fields is sample_fieldset
        assert len(context.fields) == 3

    def test_context_store_and_get_result(self, mock_meta):
        """Test storing and retrieving intermediate results."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
        )

        context.store_result("test_key", "test_value")
        assert context.get_result("test_key") == "test_value"
        assert context.get_result("nonexistent", "default") == "default"

    def test_context_get_config(self, mock_meta):
        """Test getting reader configuration values."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={"encoding": "utf-8", "decimal": ","},
        )

        assert context.get_config("encoding") == "utf-8"
        assert context.get_config("decimal") == ","
        assert context.get_config("nonexistent", "default") == "default"


class TestPipelineContextDatasets:
    """Tests for PipelineContext with datasets support.

    These tests are marked as xfail because the datasets functionality
    will be implemented as part of the BVBG087 migration.
    """

    @pytest.mark.xfail(reason="datasets attribute not yet implemented")
    def test_context_accepts_datasets(self, mock_meta):
        """Test that PipelineContext accepts datasets parameter."""
        # This will require importing DatasetConfig once implemented
        from brasa.engine.template import DatasetConfig

        datasets = {
            "indexes_info": DatasetConfig(
                name="indexes_info", tag="IndxInf", fields=Fieldset()
            )
        }

        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            datasets=datasets,
        )

        assert context.datasets is not None
        assert "indexes_info" in context.datasets

    @pytest.mark.xfail(reason="datasets attribute not yet implemented")
    def test_context_datasets_default_none(self, mock_meta):
        """Test that datasets defaults to None."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
        )

        assert context.datasets is None

    @pytest.mark.xfail(reason="get_dataset_fieldset not yet implemented")
    def test_context_get_dataset_fieldset(self, mock_meta, sample_fieldset):
        """Test getting fieldset for a specific dataset."""
        from brasa.engine.template import DatasetConfig

        datasets = {
            "indexes_info": DatasetConfig(
                name="indexes_info", tag="IndxInf", fields=sample_fieldset
            )
        }

        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            datasets=datasets,
        )

        fieldset = context.get_dataset_fieldset("indexes_info")
        assert fieldset is sample_fieldset

    @pytest.mark.xfail(reason="get_dataset_fieldset not yet implemented")
    def test_context_get_dataset_fieldset_fallback(self, mock_meta, sample_fieldset):
        """Test that get_dataset_fieldset falls back to fields if dataset not found."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            fields=sample_fieldset,
            datasets=None,
        )

        # When dataset not found, should return the single fieldset
        fieldset = context.get_dataset_fieldset("nonexistent")
        assert fieldset is sample_fieldset

    @pytest.mark.xfail(reason="get_dataset_tag not yet implemented")
    def test_context_get_dataset_tag(self, mock_meta):
        """Test getting the source tag for a dataset."""
        from brasa.engine.template import DatasetConfig

        datasets = {
            "indexes_info": DatasetConfig(
                name="indexes_info", tag="IndxInf", fields=Fieldset()
            )
        }

        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            datasets=datasets,
        )

        assert context.get_dataset_tag("indexes_info") == "IndxInf"
        assert context.get_dataset_tag("nonexistent") is None

    @pytest.mark.xfail(reason="get_tag_to_dataset_mapping not yet implemented")
    def test_context_get_tag_to_dataset_mapping(self, mock_meta):
        """Test getting the tag to dataset name mapping."""
        from brasa.engine.template import DatasetConfig

        datasets = {
            "indexes_info": DatasetConfig(
                name="indexes_info", tag="IndxInf", fields=Fieldset()
            ),
            "iopv_info": DatasetConfig(
                name="iopv_info", tag="IOPVInf", fields=Fieldset()
            ),
            "bdr_info": DatasetConfig(name="bdr_info", tag="BDRInf", fields=Fieldset()),
        }

        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            datasets=datasets,
        )

        mapping = context.get_tag_to_dataset_mapping()

        assert mapping == {
            "IndxInf": "indexes_info",
            "IOPVInf": "iopv_info",
            "BDRInf": "bdr_info",
        }

    @pytest.mark.xfail(reason="get_tag_to_dataset_mapping not yet implemented")
    def test_context_get_tag_to_dataset_mapping_empty(self, mock_meta):
        """Test that mapping is empty when no datasets configured."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
            datasets=None,
        )

        mapping = context.get_tag_to_dataset_mapping()
        assert mapping == {}


class TestPipelineContextBackwardCompatibility:
    """Tests to ensure existing pipeline context usage still works."""

    def test_context_without_datasets_works(self, mock_meta, sample_fieldset):
        """Test that context without datasets attribute works."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={"encoding": "utf-8"},
            fields=sample_fieldset,
        )

        # All existing functionality should still work
        assert context.encoding == "utf-8"
        assert context.fields is sample_fieldset

    def test_context_intermediate_results_still_work(self, mock_meta):
        """Test that intermediate results functionality still works."""
        context = PipelineContext(
            meta=mock_meta,
            reader_config={},
        )

        context.store_result("key1", "value1")
        context.store_result("key2", {"nested": "dict"})

        assert context.get_result("key1") == "value1"
        assert context.get_result("key2") == {"nested": "dict"}

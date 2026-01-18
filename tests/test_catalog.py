"""Tests for the Dataset Catalog functionality.

This module tests the DatasetCatalog class and related functions
for managing dataset metadata independently from templates.
"""

from datetime import datetime

import pyarrow as pa

from brasa.engine.catalog import (
    DatasetCatalog,
    DatasetInfo,
    MigrationReport,
    _infer_hive_partitioning,
    sync_catalog_from_disk,
)


class TestDatasetCatalog:
    """Tests for the DatasetCatalog singleton class."""

    def test_catalog_initialization(self):
        """Test that DatasetCatalog initializes correctly."""
        catalog = DatasetCatalog()
        # Should be able to get connection without error
        with catalog._connection as conn:
            c = conn.cursor()
            c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='dataset_catalog'"
            )
            result = c.fetchone()
            assert result is not None
            assert result[0] == "dataset_catalog"

    def test_register_and_get_dataset(self):
        """Test registering and retrieving a dataset."""
        catalog = DatasetCatalog()

        # Create a simple schema
        schema = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("name", pa.string()),
                pa.field("value", pa.float64()),
            ]
        )

        # Register dataset
        catalog.register_dataset(
            layer="input",
            dataset_name="test-dataset",
            schema=schema,
            partitioning=["id"],
            source_template="test-template",
        )

        # Retrieve and verify
        info = catalog.get_dataset_info("input", "test-dataset")
        assert info is not None
        assert info.layer == "input"
        assert info.dataset_name == "test-dataset"
        assert info.partitioning == ["id"]
        assert info.source_template == "test-template"
        assert len(info.schema) == 3
        assert info.schema.field("id").type == pa.int64()
        assert info.schema.field("name").type == pa.string()
        assert info.schema.field("value").type == pa.float64()

    def test_register_dataset_update_existing(self):
        """Test that re-registering a dataset updates it."""
        catalog = DatasetCatalog()

        schema1 = pa.schema([pa.field("id", pa.int64())])
        schema2 = pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("new_field", pa.string()),
            ]
        )

        # Register initial
        catalog.register_dataset(
            layer="staging",
            dataset_name="update-test",
            schema=schema1,
            partitioning=["id"],
        )

        # Update
        catalog.register_dataset(
            layer="staging",
            dataset_name="update-test",
            schema=schema2,
            partitioning=["id", "new_field"],
            source_template="new-template",
        )

        # Verify update
        info = catalog.get_dataset_info("staging", "update-test")
        assert len(info.schema) == 2
        assert info.partitioning == ["id", "new_field"]
        assert info.source_template == "new-template"

    def test_get_nonexistent_dataset(self):
        """Test that getting a nonexistent dataset returns None."""
        catalog = DatasetCatalog()
        info = catalog.get_dataset_info("nonexistent", "dataset")
        assert info is None

    def test_list_datasets(self):
        """Test listing all datasets."""
        catalog = DatasetCatalog()

        schema = pa.schema([pa.field("id", pa.int64())])

        # Register datasets in different layers
        catalog.register_dataset("input", "list-test-1", schema)
        catalog.register_dataset("staging", "list-test-2", schema)
        catalog.register_dataset("input", "list-test-3", schema)

        # List all
        all_datasets = catalog.list_datasets()
        list_test_datasets = [d for d in all_datasets if "list-test" in d.dataset_name]
        assert len(list_test_datasets) >= 3

        # List by layer
        input_datasets = catalog.list_datasets(layer="input")
        input_list_test = [d for d in input_datasets if "list-test" in d.dataset_name]
        assert len(input_list_test) >= 2

    def test_remove_dataset(self):
        """Test removing a dataset from the catalog."""
        catalog = DatasetCatalog()

        schema = pa.schema([pa.field("id", pa.int64())])
        catalog.register_dataset("curated", "remove-test", schema)

        # Verify exists
        assert catalog.get_dataset_info("curated", "remove-test") is not None

        # Remove
        result = catalog.remove_dataset("curated", "remove-test")
        assert result is True

        # Verify removed
        assert catalog.get_dataset_info("curated", "remove-test") is None

    def test_remove_nonexistent_dataset(self):
        """Test removing a nonexistent dataset returns False."""
        catalog = DatasetCatalog()
        result = catalog.remove_dataset("nonexistent", "dataset")
        assert result is False

    def test_dataset_exists(self):
        """Test checking if a dataset exists."""
        catalog = DatasetCatalog()

        schema = pa.schema([pa.field("id", pa.int64())])
        catalog.register_dataset("input", "exists-test", schema)

        assert catalog.dataset_exists("input", "exists-test") is True
        assert catalog.dataset_exists("input", "nonexistent") is False

    def test_dataset_id_format(self):
        """Test that dataset IDs are correctly formatted."""
        catalog = DatasetCatalog()
        dataset_id = catalog._make_dataset_id("input", "my-dataset")
        assert dataset_id == "input/my-dataset"


class TestSchemaSerializatiton:
    """Tests for schema serialization/deserialization."""

    def test_schema_roundtrip_basic(self):
        """Test basic schema serialization roundtrip."""
        original = pa.schema(
            [
                pa.field("int_col", pa.int64()),
                pa.field("str_col", pa.string()),
                pa.field("float_col", pa.float64()),
                pa.field("bool_col", pa.bool_()),
            ]
        )

        json_str = DatasetCatalog._schema_to_json(original)
        restored = DatasetCatalog._schema_from_json(json_str)

        assert len(restored) == len(original)
        for i in range(len(original)):
            assert restored.field(i).name == original.field(i).name
            assert restored.field(i).type == original.field(i).type

    def test_schema_roundtrip_complex_types(self):
        """Test schema serialization with complex types."""
        original = pa.schema(
            [
                pa.field("date_col", pa.date32()),
                pa.field("timestamp_col", pa.timestamp("us")),
                pa.field("list_col", pa.list_(pa.int32())),
                pa.field("nullable_col", pa.string(), nullable=True),
                pa.field("not_null_col", pa.int64(), nullable=False),
            ]
        )

        json_str = DatasetCatalog._schema_to_json(original)
        restored = DatasetCatalog._schema_from_json(json_str)

        assert restored.field("date_col").type == pa.date32()
        assert restored.field("timestamp_col").type == pa.timestamp("us")
        assert restored.field("list_col").type == pa.list_(pa.int32())


class TestHivePartitioningInference:
    """Tests for Hive partitioning inference."""

    def test_single_partition(self, tmp_path):
        """Test inferring single partition column."""
        dataset_dir = tmp_path / "dataset"
        parquet_file = dataset_dir / "refdate=2024-01-15" / "data.parquet"

        result = _infer_hive_partitioning(dataset_dir, parquet_file)
        assert result == ["refdate"]

    def test_multiple_partitions(self, tmp_path):
        """Test inferring multiple partition columns."""
        dataset_dir = tmp_path / "dataset"
        parquet_file = (
            dataset_dir / "year=2024" / "month=01" / "day=15" / "data.parquet"
        )

        result = _infer_hive_partitioning(dataset_dir, parquet_file)
        assert result == ["year", "month", "day"]

    def test_no_partitions(self, tmp_path):
        """Test when there are no partitions."""
        dataset_dir = tmp_path / "dataset"
        parquet_file = dataset_dir / "data.parquet"

        result = _infer_hive_partitioning(dataset_dir, parquet_file)
        assert result == []

    def test_mixed_folders(self, tmp_path):
        """Test with some folders not being partitions."""
        dataset_dir = tmp_path / "dataset"
        parquet_file = dataset_dir / "subfolder" / "year=2024" / "data.parquet"

        result = _infer_hive_partitioning(dataset_dir, parquet_file)
        assert result == ["year"]


class TestMigrationReport:
    """Tests for the MigrationReport class."""

    def test_migration_report_initialization(self):
        """Test MigrationReport initializes with empty lists."""
        report = MigrationReport()
        assert report.registered == []
        assert report.skipped == []
        assert report.errors == []
        assert report.warnings == []
        assert report.total_scanned == 0

    def test_add_methods(self):
        """Test adding items to the report."""
        report = MigrationReport()

        report.add_registered("input/dataset1")
        report.add_skipped("input/dataset2", "Already registered")
        report.add_error("input/dataset3", "Read error")
        report.add_warning("Unknown layer: custom")

        assert len(report.registered) == 1
        assert len(report.skipped) == 1
        assert len(report.errors) == 1
        assert len(report.warnings) == 1
        assert report.total_scanned == 3

    def test_summary(self):
        """Test summary generation."""
        report = MigrationReport()
        report.add_registered("input/dataset1")
        report.add_registered("input/dataset2")
        report.add_skipped("input/dataset3", "reason")

        summary = report.summary()
        assert "Registered: 2" in summary
        assert "Skipped:    1" in summary

    def test_to_dict(self):
        """Test conversion to dictionary."""
        report = MigrationReport()
        report.add_registered("input/dataset1")
        report.add_error("input/dataset2", "error msg")

        result = report.to_dict()
        assert result["registered"] == ["input/dataset1"]
        assert result["errors"][0]["id"] == "input/dataset2"
        assert result["errors"][0]["error"] == "error msg"
        assert result["total_scanned"] == 2


class TestSyncCatalogFromDisk:
    """Tests for the sync_catalog_from_disk function."""

    def test_sync_empty_db_folder(self):
        """Test syncing when db folder is empty."""
        report = sync_catalog_from_disk()
        # Should complete without error
        assert isinstance(report, MigrationReport)

    def test_sync_with_layer_filter(self):
        """Test syncing with a specific layer filter."""
        report = sync_catalog_from_disk(layer="input")
        assert isinstance(report, MigrationReport)

    def test_sync_dry_run(self):
        """Test dry run mode."""
        report = sync_catalog_from_disk(dry_run=True)
        # In dry run, nothing should be registered
        assert len(report.registered) == 0


class TestDatasetInfo:
    """Tests for the DatasetInfo dataclass."""

    def test_datasetinfo_creation(self):
        """Test creating a DatasetInfo object."""
        schema = pa.schema([pa.field("id", pa.int64())])
        now = datetime.now()

        info = DatasetInfo(
            id="input/test",
            layer="input",
            dataset_name="test",
            schema=schema,
            partitioning=["id"],
            source_template="template",
            created_at=now,
            updated_at=now,
        )

        assert info.id == "input/test"
        assert info.layer == "input"
        assert info.dataset_name == "test"
        assert len(info.schema) == 1
        assert info.partitioning == ["id"]
        assert info.source_template == "template"

    def test_datasetinfo_defaults(self):
        """Test DatasetInfo with default values."""
        schema = pa.schema([pa.field("id", pa.int64())])

        info = DatasetInfo(
            id="input/test",
            layer="input",
            dataset_name="test",
            schema=schema,
        )

        assert info.partitioning == []
        assert info.source_template is None
        assert isinstance(info.created_at, datetime)
        assert isinstance(info.updated_at, datetime)

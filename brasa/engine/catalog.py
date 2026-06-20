"""Dataset catalog for managing dataset metadata.

This module provides the DatasetCatalog singleton class that stores schema
and metadata information independently from templates, enabling proper
schema retrieval for datasets in all layers (input, staging, curated).
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from .cache import CacheManager
from .core import Singleton
from .layers import DataLayer
from .resources import package_path


@dataclass
class DatasetInfo:
    """Metadata information for a registered dataset.

    Attributes:
        id: Unique identifier in the format 'layer/dataset_name'.
        layer: Data layer (input, staging, curated).
        dataset_name: Name of the dataset.
        schema: PyArrow schema for the dataset.
        partitioning: List of partition column names.
        source_template: Source template ID if applicable.
        created_at: Timestamp when the dataset was first registered.
        updated_at: Timestamp of the last update.
    """

    id: str
    layer: str
    dataset_name: str
    schema: pa.Schema
    partitioning: list[str] = field(default_factory=list)
    source_template: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class MigrationReport:
    """Report of migration operation results.

    Tracks statistics for the sync-catalog operation, including
    registered datasets, skipped entries, and errors encountered.
    """

    registered: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)
    errors: list[tuple[str, str]] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    would_register: list[tuple[str, pa.Schema, list[str]]] = field(default_factory=list)

    @property
    def total_scanned(self) -> int:
        """Total number of datasets scanned."""
        return len(self.registered) + len(self.skipped) + len(self.errors)

    def add_registered(self, dataset_id: str) -> None:
        """Record a successfully registered dataset."""
        self.registered.append(dataset_id)

    def add_skipped(self, dataset_id: str, reason: str) -> None:
        """Record a skipped dataset with reason."""
        self.skipped.append((dataset_id, reason))

    def add_error(self, dataset_id: str, error: str) -> None:
        """Record an error during migration."""
        self.errors.append((dataset_id, error))

    def add_warning(self, message: str) -> None:
        """Record a warning message."""
        self.warnings.append(message)

    def add_would_register(
        self, dataset_id: str, schema: pa.Schema, partitioning: list[str]
    ) -> None:
        """Record a dataset that would be registered in dry-run mode."""
        self.would_register.append((dataset_id, schema, partitioning))

    def summary(self) -> str:
        """Generate a summary string of the migration results."""
        return (
            f"Migration complete:\n"
            f"  Registered: {len(self.registered)}\n"
            f"  Skipped:    {len(self.skipped)}\n"
            f"  Errors:     {len(self.errors)}\n"
            f"  Warnings:   {len(self.warnings)}"
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert report to a dictionary for JSON serialization."""
        return {
            "registered": self.registered,
            "skipped": [{"id": s[0], "reason": s[1]} for s in self.skipped],
            "errors": [{"id": e[0], "error": e[1]} for e in self.errors],
            "warnings": self.warnings,
            "would_register": [
                {
                    "id": w[0],
                    "schema_columns": len(w[1]) if w[1] else 0,
                    "partitioning": w[2],
                }
                for w in self.would_register
            ],
            "total_scanned": self.total_scanned,
        }


class DatasetCatalog(Singleton):
    """Singleton manager for the dataset catalog.

    Handles registration, retrieval, and management of dataset metadata
    in the SQLite database. The catalog stores schema information
    independently from templates, enabling proper schema retrieval for
    datasets in all data layers.

    Example:
        >>> catalog = DatasetCatalog()
        >>> catalog.register_dataset(
        ...     layer="input",
        ...     dataset_name="b3-cotahist",
        ...     schema=my_schema,
        ...     partitioning=["refdate"],
        ...     source_template="b3-cotahist",
        ... )
        >>> info = catalog.get_dataset_info("input", "b3-cotahist")
        >>> print(info.schema)
    """

    def init(self) -> None:
        """Initialize the dataset catalog.

        Creates the catalog table if it doesn't exist.
        """
        self._initialized = False

    def _ensure_initialized(self) -> None:
        """Ensure the catalog table exists in the database."""
        if not self._initialized:
            self._create_catalog_table()
            self._initialized = True

    def _create_catalog_table(self) -> None:
        """Create the dataset_catalog table if it doesn't exist."""
        man = CacheManager()
        db_conn = sqlite3.connect(database=man.cache_path(man.meta_db_filename))
        c = db_conn.cursor()
        sql_path = package_path("sql", "create-dataset-catalog.sql")
        with sql_path.open() as f:
            c.executescript(f.read())
        db_conn.commit()
        db_conn.close()

    @property
    def _connection(self) -> sqlite3.Connection:
        """Get a connection to the metadata database."""
        self._ensure_initialized()
        man = CacheManager()
        return sqlite3.connect(database=man.cache_path(man.meta_db_filename))

    @staticmethod
    def _schema_to_json(schema: pa.Schema) -> str:
        """Serialize a PyArrow schema to JSON string.

        Uses IPC serialization to preserve full schema fidelity,
        then base64 encodes for JSON storage.

        Args:
            schema: PyArrow schema to serialize.

        Returns:
            JSON string representation of the schema.
        """
        # Use IPC format for lossless serialization
        sink = pa.BufferOutputStream()
        writer = pa.ipc.new_stream(sink, schema)
        writer.close()
        serialized = sink.getvalue().to_pybytes()

        import base64

        return json.dumps(
            {
                "format": "ipc",
                "data": base64.b64encode(serialized).decode("ascii"),
            }
        )

    @staticmethod
    def _schema_from_json(json_str: str) -> pa.Schema:
        """Deserialize a PyArrow schema from JSON string.

        Args:
            json_str: JSON string representation of the schema.

        Returns:
            Reconstructed PyArrow schema.
        """
        import base64

        data = json.loads(json_str)

        if data.get("format") == "ipc":
            serialized = base64.b64decode(data["data"])
            reader = pa.ipc.open_stream(serialized)
            return reader.schema

        # Fallback for legacy format (field definitions)
        if "fields" in data:
            fields = []
            for f in data["fields"]:
                dtype = getattr(pa, f["type"])()
                fields.append(
                    pa.field(f["name"], dtype, nullable=f.get("nullable", True))
                )
            return pa.schema(fields)

        raise ValueError(f"Unknown schema format: {json_str[:100]}")

    @staticmethod
    def _make_dataset_id(layer: str, dataset_name: str) -> str:
        """Create a unique dataset identifier.

        Args:
            layer: Data layer (input, staging, curated).
            dataset_name: Name of the dataset.

        Returns:
            Unique identifier in the format 'layer/dataset_name'.
        """
        return f"{layer}/{dataset_name}"

    def register_dataset(
        self,
        layer: str,
        dataset_name: str,
        schema: pa.Schema,
        partitioning: list[str] | None = None,
        source_template: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
    ) -> None:
        """Register a dataset in the catalog.

        If the dataset already exists, its metadata will be updated.

        Args:
            layer: Data layer (input, staging, curated).
            dataset_name: Name of the dataset.
            schema: PyArrow schema for the dataset.
            partitioning: Optional list of partition column names.
            source_template: Optional source template ID.
            created_at: Optional creation timestamp (defaults to now).
            updated_at: Optional update timestamp (defaults to now).
        """
        dataset_id = self._make_dataset_id(layer, dataset_name)
        schema_json = self._schema_to_json(schema)
        partitioning_str = ",".join(partitioning) if partitioning else ""
        now = datetime.now().isoformat()
        created = created_at.isoformat() if created_at else now
        updated = updated_at.isoformat() if updated_at else now

        with closing(self._connection) as conn, conn:
            c = conn.cursor()
            c.execute("SELECT id FROM dataset_catalog WHERE id = ?", (dataset_id,))
            if c.fetchone():
                # Update existing entry
                c.execute(
                    """UPDATE dataset_catalog
                       SET schema_json = ?, partitioning = ?, source_template = ?, updated_at = ?
                       WHERE id = ?""",
                    (
                        schema_json,
                        partitioning_str,
                        source_template,
                        updated,
                        dataset_id,
                    ),
                )
            else:
                # Insert new entry
                c.execute(
                    """INSERT INTO dataset_catalog
                       (id, layer, dataset_name, schema_json, partitioning, source_template, created_at, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        dataset_id,
                        layer,
                        dataset_name,
                        schema_json,
                        partitioning_str,
                        source_template,
                        created,
                        updated,
                    ),
                )

    def get_dataset_info(self, layer: str, dataset_name: str) -> DatasetInfo | None:
        """Retrieve dataset metadata from the catalog.

        Args:
            layer: Data layer (input, staging, curated).
            dataset_name: Name of the dataset.

        Returns:
            DatasetInfo object if found, None otherwise.
        """
        dataset_id = self._make_dataset_id(layer, dataset_name)

        with closing(self._connection) as conn, conn:
            c = conn.cursor()
            c.execute("SELECT * FROM dataset_catalog WHERE id = ?", (dataset_id,))
            row = c.fetchone()

        if not row:
            return None

        return DatasetInfo(
            id=row[0],
            layer=row[1],
            dataset_name=row[2],
            schema=self._schema_from_json(row[3]),
            partitioning=row[4].split(",") if row[4] else [],
            source_template=row[5],
            created_at=datetime.fromisoformat(row[6]),
            updated_at=datetime.fromisoformat(row[7]),
        )

    def list_datasets(self, layer: str | None = None) -> list[DatasetInfo]:
        """List all registered datasets.

        Args:
            layer: Optional filter by data layer.

        Returns:
            List of DatasetInfo objects.
        """
        with closing(self._connection) as conn, conn:
            c = conn.cursor()
            if layer:
                c.execute(
                    "SELECT * FROM dataset_catalog WHERE layer = ? ORDER BY layer, dataset_name",
                    (layer,),
                )
            else:
                c.execute("SELECT * FROM dataset_catalog ORDER BY layer, dataset_name")
            rows = c.fetchall()

        return [
            DatasetInfo(
                id=row[0],
                layer=row[1],
                dataset_name=row[2],
                schema=self._schema_from_json(row[3]),
                partitioning=row[4].split(",") if row[4] else [],
                source_template=row[5],
                created_at=datetime.fromisoformat(row[6]),
                updated_at=datetime.fromisoformat(row[7]),
            )
            for row in rows
        ]

    def remove_dataset(self, layer: str, dataset_name: str) -> bool:
        """Remove a dataset from the catalog.

        Args:
            layer: Data layer (input, staging, curated).
            dataset_name: Name of the dataset.

        Returns:
            True if a dataset was removed, False otherwise.
        """
        dataset_id = self._make_dataset_id(layer, dataset_name)

        with closing(self._connection) as conn, conn:
            c = conn.cursor()
            c.execute("DELETE FROM dataset_catalog WHERE id = ?", (dataset_id,))
            return c.rowcount > 0

    def dataset_exists(self, layer: str, dataset_name: str) -> bool:
        """Check if a dataset exists in the catalog.

        Args:
            layer: Data layer (input, staging, curated).
            dataset_name: Name of the dataset.

        Returns:
            True if the dataset exists, False otherwise.
        """
        dataset_id = self._make_dataset_id(layer, dataset_name)

        with closing(self._connection) as conn, conn:
            c = conn.cursor()
            c.execute("SELECT 1 FROM dataset_catalog WHERE id = ?", (dataset_id,))
            return c.fetchone() is not None


def _infer_hive_partitioning(dataset_dir: Path, parquet_file: Path) -> list[str]:
    """Infer partition columns from Hive-style folder structure.

    Example:
        dataset_dir = /db/input/b3-cotahist
        parquet_file = /db/input/b3-cotahist/refdate=2024-01-15/part-0.parquet

        Returns: ["refdate"]

    Args:
        dataset_dir: Root directory of the dataset.
        parquet_file: Path to a parquet file within the dataset.

    Returns:
        List of partition column names.
    """
    partitioning = []

    # Get relative path from dataset_dir to parquet_file's parent
    try:
        rel_path = parquet_file.parent.relative_to(dataset_dir)

        for part in rel_path.parts:
            # Hive partitioning uses "column=value" format
            if "=" in part:
                col_name = part.split("=")[0]
                partitioning.append(col_name)
    except ValueError:
        # parquet_file is not under dataset_dir
        pass

    return partitioning


def _process_dataset_directory(
    catalog: DatasetCatalog,
    layer_name: str,
    dataset_dir: Path,
    report: MigrationReport,
    dry_run: bool,
    force: bool,
) -> None:
    """Process a single dataset directory for catalog sync.

    Args:
        catalog: The DatasetCatalog instance.
        layer_name: Name of the data layer.
        dataset_dir: Path to the dataset directory.
        report: MigrationReport to update.
        dry_run: If True, only report what would be done.
        force: If True, overwrite existing catalog entries.
    """
    dataset_id = f"{layer_name}/{dataset_dir.name}"

    # Check if already registered
    existing = catalog.get_dataset_info(layer_name, dataset_dir.name)
    if existing and not force:
        report.add_skipped(dataset_id, "Already registered")
        return

    # Find parquet files
    parquet_files = list(dataset_dir.rglob("*.parquet"))
    if not parquet_files:
        report.add_skipped(dataset_id, "No parquet files found")
        return

    try:
        # Read schema from first parquet file
        # Use the deepest file to ensure we get data file, not partition metadata
        deepest_file = max(parquet_files, key=lambda p: len(p.parts))
        pq_file = pq.ParquetFile(deepest_file)
        schema = pq_file.schema_arrow

        # Infer partitioning from folder structure
        partitioning = _infer_hive_partitioning(dataset_dir, deepest_file)

        # Get timestamps from file system
        stat = deepest_file.stat()
        created_at = datetime.fromtimestamp(stat.st_ctime)
        updated_at = datetime.fromtimestamp(stat.st_mtime)

        if dry_run:
            report.add_would_register(dataset_id, schema, partitioning)
        else:
            # Register in catalog
            catalog.register_dataset(
                layer=layer_name,
                dataset_name=dataset_dir.name,
                schema=schema,
                partitioning=partitioning,
                source_template=None,  # Cannot recover from disk
                created_at=created_at,
                updated_at=updated_at,
            )
            report.add_registered(dataset_id)

    except Exception as e:
        report.add_error(dataset_id, str(e))


def sync_catalog_from_disk(
    layer: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> MigrationReport:
    """Scan db/ folder and register untracked datasets in the catalog.

    This function discovers existing parquet datasets that were created
    before the Dataset Catalog was implemented and registers them
    by inferring metadata from the parquet files themselves.

    Args:
        layer: Optional layer filter (input, staging, curated).
               If None, scans all layers.
        dry_run: If True, report what would be done without making changes.
        force: If True, overwrite existing catalog entries.

    Returns:
        MigrationReport with statistics and any errors encountered.
    """
    catalog = DatasetCatalog()
    man = CacheManager()

    report = MigrationReport()
    db_root = Path(man.db_path(""))

    if not db_root.exists():
        report.add_warning(f"Database root does not exist: {db_root}")
        return report

    valid_layers = {
        layer_enum.value for layer_enum in DataLayer if layer_enum != DataLayer.RAW
    }

    # Enumerate all layer directories
    for layer_dir in sorted(db_root.iterdir()):
        if not layer_dir.is_dir():
            continue
        if layer_dir.name not in valid_layers:
            if layer_dir.name != "brasa.duckdb":  # Ignore the DuckDB file
                report.add_warning(f"Skipping unknown layer: {layer_dir.name}")
            continue
        if layer and layer_dir.name != layer:
            continue

        # Enumerate all dataset directories within layer
        for dataset_dir in sorted(layer_dir.iterdir()):
            if not dataset_dir.is_dir():
                continue

            _process_dataset_directory(
                catalog, layer_dir.name, dataset_dir, report, dry_run, force
            )

    return report

---
goal: Implement a Dataset Catalog to track schema metadata independently from templates
version: 1.0
date_created: 2025-01-18
last_updated: 2025-01-18
owner: brasa-team
status: 'Planned'
tags: ['feature', 'architecture', 'metadata', 'schema']
---

# Introduction

![Status: Planned](https://img.shields.io/badge/status-Planned-blue)

This plan addresses a fundamental architectural issue where the `get_dataset()` function mixes the concepts of templates and datasets. Currently, when `use_template_schema=False` is used (e.g., when querying datasets from staging/curated layers), the schema information is lost, resulting in incorrect data reading (all NaN values).

The solution is to create a **Dataset Catalog** that stores metadata about all datasets in the `db` folder, including:
- Dataset name and location (layer/path)
- PyArrow schema (stored as JSON or Parquet metadata)
- Partitioning information
- Source template (if applicable)
- Creation/modification timestamps

This decouples the schema from templates, allowing datasets to be queried correctly regardless of whether they originated from a template or an ETL process.

## 1. Requirements & Constraints

- **REQ-001**: Dataset catalog must store schema information independently from templates
- **REQ-002**: Catalog must support all data layers (input, staging, curated)
- **REQ-003**: Schema must be automatically saved when writing datasets via `save_partitioned_parquet_file()` or ETL pipelines
- **REQ-004**: `get_dataset()` must be able to retrieve schema from catalog when `use_template_schema=False`
- **REQ-005**: Catalog must be queryable via CLI commands (list datasets, describe schema)
- **REQ-006**: Backwards compatibility: existing datasets without catalog entries should still work (with degraded functionality)
- **SEC-001**: Catalog metadata must be stored in the existing cache structure (`.brasa-cache/meta/`)
- **CON-001**: Must use SQLite for metadata storage (consistent with existing `meta.db`)
- **CON-002**: Schema storage format must support PyArrow schema serialization/deserialization
- **CON-003**: Must not break existing template-based workflows
- **GUD-001**: Follow existing code patterns (Singleton for managers, snake_case, type hints)
- **GUD-002**: Use pathlib.Path instead of string paths
- **PAT-001**: Follow existing CacheManager patterns for database operations

## 2. Implementation Steps

### Implementation Phase 1: Database Schema and DatasetCatalog Class

- GOAL-001: Create the database schema and core DatasetCatalog class for managing dataset metadata

| Task     | Description                                                                                                                                                                                                                                     | Completed | Date |
| -------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-001 | Create SQL migration file `sql/create-dataset-catalog.sql` with table schema: `dataset_catalog(id TEXT PRIMARY KEY, layer TEXT, dataset_name TEXT, schema_json TEXT, partitioning TEXT, source_template TEXT, created_at TEXT, updated_at TEXT)` |           |      |
| TASK-002 | Create new module `brasa/engine/catalog.py` with `DatasetCatalog` class as Singleton                                                                                                                                                            |           |      |
| TASK-003 | Implement `DatasetCatalog.init()` method to create catalog table if not exists                                                                                                                                                                  |           |      |
| TASK-004 | Implement `DatasetCatalog.register_dataset(layer, dataset_name, schema, partitioning, source_template)` method                                                                                                                                  |           |      |
| TASK-005 | Implement `DatasetCatalog.get_dataset_info(layer, dataset_name)` method returning a dataclass with all metadata                                                                                                                                 |           |      |
| TASK-006 | Implement `DatasetCatalog.list_datasets(layer=None)` method to list all registered datasets                                                                                                                                                     |           |      |
| TASK-007 | Implement `DatasetCatalog.remove_dataset(layer, dataset_name)` method for cleanup                                                                                                                                                               |           |      |
| TASK-008 | Implement schema serialization: `_schema_to_json(schema: pa.Schema) -> str` and `_schema_from_json(json_str: str) -> pa.Schema`                                                                                                                 |           |      |

### Implementation Phase 2: Integration with Data Writing Operations

- GOAL-002: Integrate DatasetCatalog with all data writing operations to automatically register datasets

| Task     | Description                                                                                                                                                         | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-009 | Update `brasa/engine/processing.py:save_partitioned_parquet_file()` to register dataset in catalog after writing                                                    |           |      |
| TASK-010 | Update `brasa/engine/pipeline/etl_executor.py:execute_and_write()` to register dataset in catalog after ETL write                                                   |           |      |
| TASK-011 | Update `brasa/queries.py:write_dataset()` to register dataset in catalog after writing                                                                              |           |      |
| TASK-012 | Add `source_template` parameter tracking in all write operations to record lineage                                                                                  |           |      |

### Implementation Phase 3: Update get_dataset to Use Catalog

- GOAL-003: Modify `get_dataset()` to use the catalog for schema retrieval when `use_template_schema=False`

| Task     | Description                                                                                                                                                                                                                           | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-013 | Create helper function `get_catalog_schema(layer, dataset_name) -> tuple[pa.Schema, ds.Partitioning]` in `brasa/queries.py`                                                                                                           |           |      |
| TASK-014 | Update `get_dataset()` to first check catalog when `use_template_schema=False` and `layer` is provided                                                                                                                                |           |      |
| TASK-015 | Add new parameter `use_catalog_schema: bool = True` to `get_dataset()` for explicit catalog usage control                                                                                                                             |           |      |
| TASK-016 | Implement fallback logic: catalog → template → raw parquet metadata (for backwards compatibility)                                                                                                                                     |           |      |

### Implementation Phase 4: CLI Commands for Dataset Management

- GOAL-004: Add CLI commands for listing and inspecting datasets in the catalog

| Task     | Description                                                                                                                                                                                                        | Completed | Date |
| -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------- | ---- |
| TASK-017 | Add `brasa.cli` subcommand `list-datasets` with optional `--layer` filter to show all registered datasets                                                                                                          |           |      |
| TASK-018 | Add `brasa.cli` subcommand `describe-dataset <layer.dataset>` to show schema, partitioning, and metadata                                                                                                           |           |      |
| TASK-019 | Update existing `head` command to use catalog schema when available                                                                                                                                                |           |      |
| TASK-020 | Add `brasa.cli` subcommand `sync-catalog` to scan `db/` folder and register any untracked datasets (for migration of existing data)                                                                                |           |      |

### Implementation Phase 5: Migration and Testing

- GOAL-005: Migrate existing datasets and add comprehensive tests

| Task     | Description                                                                                                                                                                                              | Completed | Date |
| -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-021 | Create migration script/command to scan existing `db/` folder and populate catalog from parquet metadata                                                                                                 |           |      |
| TASK-022 | Update `CacheManager.init()` to also initialize DatasetCatalog                                                                                                                                           |           |      |
| TASK-023 | Write unit tests for DatasetCatalog CRUD operations in `tests/test_catalog.py`                                                                                                                           |           |      |
| TASK-024 | Write integration tests for `get_dataset()` with catalog schema retrieval                                                                                                                                |           |      |
| TASK-025 | Write integration tests for ETL pipeline catalog registration                                                                                                                                            |           |      |
| TASK-026 | Update documentation in `docs/ARCHITECTURE.md` to describe the Dataset Catalog component                                                                                                                 |           |      |

### Implementation Phase 6: Public API Exports

- GOAL-006: Export DatasetCatalog and related functions in public API

| Task     | Description                                                                                                             | Completed | Date |
| -------- | ----------------------------------------------------------------------------------------------------------------------- | --------- | ---- |
| TASK-027 | Export `DatasetCatalog` in `brasa/engine/__init__.py`                                                                   |           |      |
| TASK-028 | Add `list_datasets()`, `describe_dataset()` convenience functions in `brasa/queries.py`                                 |           |      |
| TASK-029 | Export new functions in `brasa/__init__.py`                                                                             |           |      |

## 3. Alternatives

- **ALT-001**: Store schema in parquet file metadata - Rejected because it requires reading parquet files to get schema, and doesn't support storing additional metadata like source template or partitioning info.
- **ALT-002**: Create a separate JSON/YAML catalog file per layer - Rejected because it fragments metadata storage and is inconsistent with the existing SQLite-based approach.
- **ALT-003**: Extend existing `cache_metadata` table - Rejected because that table tracks download operations, not dataset definitions. Mixing concerns would complicate the schema.
- **ALT-004**: Store schema alongside parquet files as `_schema.json` - Rejected because it requires file system traversal and doesn't provide centralized querying capabilities.
- **ALT-005**: Infer schema from parquet files at read time - Rejected because parquet metadata may not preserve the exact intended schema (e.g., integer vs float), and it doesn't provide partitioning info.

## 4. Dependencies

- **DEP-001**: PyArrow library (already installed) - for schema serialization via `schema.to_pandas_dtype()` or IPC format
- **DEP-002**: SQLite (built-in Python) - for catalog storage
- **DEP-003**: Existing CacheManager infrastructure - for database path resolution

## 5. Files

- **FILE-001**: `sql/create-dataset-catalog.sql` - New SQL file for catalog table creation
- **FILE-002**: `brasa/engine/catalog.py` - New module containing DatasetCatalog class
- **FILE-003**: `brasa/engine/__init__.py` - Update exports to include DatasetCatalog
- **FILE-004**: `brasa/engine/processing.py` - Update `save_partitioned_parquet_file()` to register datasets
- **FILE-005**: `brasa/engine/pipeline/etl_executor.py` - Update `execute_and_write()` to register datasets
- **FILE-006**: `brasa/engine/cache.py` - Update `CacheManager.init()` to initialize catalog
- **FILE-007**: `brasa/queries.py` - Update `get_dataset()` and add catalog-related functions
- **FILE-008**: `brasa/cli.py` - Add new CLI commands for dataset management
- **FILE-009**: `brasa/__init__.py` - Export new public functions
- **FILE-010**: `tests/test_catalog.py` - New test file for catalog operations
- **FILE-011**: `docs/ARCHITECTURE.md` - Update documentation

## 6. Testing

- **TEST-001**: Test DatasetCatalog initialization creates table correctly
- **TEST-002**: Test `register_dataset()` with valid schema and partitioning
- **TEST-003**: Test `get_dataset_info()` returns correct metadata
- **TEST-004**: Test `list_datasets()` with and without layer filter
- **TEST-005**: Test `remove_dataset()` deletes entry correctly
- **TEST-006**: Test schema serialization roundtrip (PyArrow → JSON → PyArrow)
- **TEST-007**: Test `get_dataset()` retrieves schema from catalog when `use_template_schema=False`
- **TEST-008**: Test `get_dataset()` fallback behavior when dataset not in catalog
- **TEST-009**: Test ETL pipeline automatically registers dataset in catalog
- **TEST-010**: Test `sync-catalog` command discovers and registers existing datasets
- **TEST-011**: Test CLI `list-datasets` and `describe-dataset` commands output

## 7. Risks & Assumptions

- **RISK-001**: Performance impact of additional database write on every dataset save - Mitigated by using SQLite with small payload (single row per dataset)
- **RISK-002**: Schema serialization may not preserve all PyArrow type details - Mitigated by using IPC serialization format which is lossless
- **RISK-003**: Migration of existing datasets may fail for corrupted parquet files - Mitigated by implementing graceful error handling with logging
- **ASSUMPTION-001**: All datasets have consistent schema within their partition structure
- **ASSUMPTION-002**: The metadata database (`meta.db`) is always accessible and writable
- **ASSUMPTION-003**: PyArrow schema IPC serialization is stable across versions

## 8. Migration Specification

This section details the migration strategy for existing datasets that were created before the Dataset Catalog was implemented.

### 8.1 Migration Overview

Existing datasets in the `db/` folder need to be registered in the catalog to enable proper schema retrieval. Since these datasets were written without catalog entries, the migration process must **infer metadata from the parquet files themselves**.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                     MIGRATION FLOW DIAGRAM                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  .brasa-cache/db/                                                       │
│  ├── input/                                                             │
│  │   ├── b3-cotahist/                                                   │
│  │   │   └── refdate=2024-01-15/                                        │
│  │   │       └── part-0.parquet  ─────┐                                 │
│  │   └── b3-indexes-historical-prices/│                                 │
│  │       └── index=IBOV/year=2024/    │                                 │
│  │           └── part-0.parquet  ─────┼──┐                              │
│  └── staging/                         │  │                              │
│      └── b3-indexes-historical-prices/│  │                              │
│          └── index=IBOV/year=2024/    │  │                              │
│              └── part-0.parquet  ─────┼──┼──┐                           │
│                                       │  │  │                           │
│                    ┌──────────────────┘  │  │                           │
│                    │  ┌──────────────────┘  │                           │
│                    │  │  ┌──────────────────┘                           │
│                    ▼  ▼  ▼                                              │
│              ┌─────────────────┐                                        │
│              │  sync-catalog   │                                        │
│              │    command      │                                        │
│              └────────┬────────┘                                        │
│                       │                                                 │
│         ┌─────────────┼─────────────┐                                   │
│         ▼             ▼             ▼                                   │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐                           │
│  │ Read schema│ │ Read schema│ │ Read schema│                           │
│  │ from file  │ │ from file  │ │ from file  │                           │
│  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘                           │
│        │              │              │                                  │
│        ▼              ▼              ▼                                  │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐                           │
│  │  Detect    │ │  Detect    │ │  Detect    │                           │
│  │partitioning│ │partitioning│ │partitioning│                           │
│  └─────┬──────┘ └─────┬──────┘ └─────┬──────┘                           │
│        │              │              │                                  │
│        └──────────────┼──────────────┘                                  │
│                       ▼                                                 │
│              ┌─────────────────┐                                        │
│              │ dataset_catalog │                                        │
│              │    (SQLite)     │                                        │
│              └─────────────────┘                                        │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 8.2 What Can Be Recovered from Parquet Files

| Metadata Field     | Recovery Method                                   | Accuracy      |
|--------------------|---------------------------------------------------|---------------|
| `layer`            | Extracted from folder path (`db/{layer}/...`)     | ✅ Exact      |
| `dataset_name`     | Extracted from folder path                        | ✅ Exact      |
| `schema`           | Read from parquet file metadata                   | ⚠️ Close*     |
| `partitioning`     | Inferred from Hive-style folder structure         | ✅ Exact      |
| `source_template`  | Cannot be recovered                               | ❌ NULL       |
| `created_at`       | File system creation time (if available)          | ⚠️ Approximate|
| `updated_at`       | File system modification time                     | ⚠️ Approximate|

*Schema accuracy note: Parquet stores the schema but may have subtle differences:
- `int64` vs `Int64` (nullable)
- String encoding details may be lost
- Custom type parameters (decimal places, date formats) are not preserved

### 8.3 Migration Algorithm

```python
def sync_catalog_from_disk(
    layer: str | None = None,
    dry_run: bool = False,
    force: bool = False,
) -> MigrationReport:
    """
    Scan db/ folder and register untracked datasets in the catalog.

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

    # Step 1: Enumerate all layer directories
    for layer_dir in db_root.iterdir():
        if not layer_dir.is_dir():
            continue
        if layer_dir.name not in ("input", "staging", "curated"):
            report.add_warning(f"Skipping unknown layer: {layer_dir.name}")
            continue
        if layer and layer_dir.name != layer:
            continue

        # Step 2: Enumerate all dataset directories within layer
        for dataset_dir in layer_dir.iterdir():
            if not dataset_dir.is_dir():
                continue

            dataset_id = f"{layer_dir.name}/{dataset_dir.name}"

            # Step 3: Check if already registered
            existing = catalog.get_dataset_info(layer_dir.name, dataset_dir.name)
            if existing and not force:
                report.add_skipped(dataset_id, "Already registered")
                continue

            # Step 4: Find parquet files
            parquet_files = list(dataset_dir.rglob("*.parquet"))
            if not parquet_files:
                report.add_skipped(dataset_id, "No parquet files found")
                continue

            try:
                # Step 5: Read schema from first parquet file
                # Use the deepest file to ensure we get data file, not partition metadata
                deepest_file = max(parquet_files, key=lambda p: len(p.parts))
                pq_file = pq.ParquetFile(deepest_file)
                schema = pq_file.schema_arrow

                # Step 6: Infer partitioning from folder structure
                partitioning = _infer_hive_partitioning(dataset_dir, deepest_file)

                # Step 7: Get timestamps from file system
                stat = deepest_file.stat()
                created_at = datetime.fromtimestamp(stat.st_ctime)
                updated_at = datetime.fromtimestamp(stat.st_mtime)

                if dry_run:
                    report.add_would_register(dataset_id, schema, partitioning)
                else:
                    # Step 8: Register in catalog
                    catalog.register_dataset(
                        layer=layer_dir.name,
                        dataset_name=dataset_dir.name,
                        schema=schema,
                        partitioning=partitioning,
                        source_template=None,  # Unknown for migrated data
                        created_at=created_at,
                        updated_at=updated_at,
                    )
                    report.add_registered(dataset_id)

            except Exception as e:
                report.add_error(dataset_id, str(e))

    return report


def _infer_hive_partitioning(
    dataset_dir: Path,
    parquet_file: Path
) -> list[str]:
    """
    Infer partition columns from Hive-style folder structure.

    Example:
        dataset_dir = /db/input/b3-cotahist
        parquet_file = /db/input/b3-cotahist/refdate=2024-01-15/part-0.parquet

        Returns: ["refdate"]

    Example with multiple partitions:
        dataset_dir = /db/staging/b3-indexes-historical-prices
        parquet_file = /db/staging/b3-indexes-historical-prices/index=IBOV/year=2024/part-0.parquet

        Returns: ["index", "year"]
    """
    partitioning = []

    # Get relative path from dataset_dir to parquet_file's parent
    rel_path = parquet_file.parent.relative_to(dataset_dir)

    for part in rel_path.parts:
        # Hive partitioning uses "column=value" format
        if "=" in part:
            col_name = part.split("=")[0]
            partitioning.append(col_name)

    return partitioning
```

### 8.4 MigrationReport Structure

```python
@dataclass
class MigrationReport:
    """Report of migration operation results."""

    registered: list[str] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (id, reason)
    errors: list[tuple[str, str]] = field(default_factory=list)   # (id, error)
    warnings: list[str] = field(default_factory=list)
    would_register: list[tuple[str, pa.Schema, list[str]]] = field(default_factory=list)

    @property
    def total_scanned(self) -> int:
        return len(self.registered) + len(self.skipped) + len(self.errors)

    def summary(self) -> str:
        return (
            f"Migration complete:\n"
            f"  Registered: {len(self.registered)}\n"
            f"  Skipped:    {len(self.skipped)}\n"
            f"  Errors:     {len(self.errors)}\n"
            f"  Warnings:   {len(self.warnings)}"
        )
```

### 8.5 CLI Commands for Migration

```bash
# Basic migration - register all untracked datasets
uv run python -m brasa.cli sync-catalog

# Migration with layer filter
uv run python -m brasa.cli sync-catalog --layer staging

# Dry-run to preview changes
uv run python -m brasa.cli sync-catalog --dry-run

# Force re-registration (overwrite existing entries)
uv run python -m brasa.cli sync-catalog --force

# Verbose output showing schema details
uv run python -m brasa.cli sync-catalog --verbose

# Output as JSON for programmatic processing
uv run python -m brasa.cli sync-catalog --format json
```

### 8.6 Expected CLI Output

```
$ uv run python -m brasa.cli sync-catalog --dry-run

Scanning .brasa-cache/db/ for untracked datasets...

Would register:
  [input] b3-cotahist
    Schema: 15 columns (refdate: date32, symbol: string, ...)
    Partitioning: [refdate]

  [input] b3-indexes-historical-prices
    Schema: 15 columns (day: int64, month01: float64, ...)
    Partitioning: [index, year]

  [staging] b3-indexes-historical-prices
    Schema: 15 columns (day: int64, month01: float64, ...)
    Partitioning: [index, year]

Skipped (already registered):
  [input] b3-indexes-current-portfolio

Summary:
  Would register: 3
  Skipped: 1
  Errors: 0

Run without --dry-run to apply changes.
```

### 8.7 Migration Limitations and Warnings

| Limitation | Impact | Mitigation |
|------------|--------|------------|
| `source_template` is NULL | Cannot trace lineage back to original template | Document in metadata that dataset was migrated |
| Schema may differ slightly | Type coercion issues possible | Log warnings when schema doesn't match template (if template exists) |
| Timestamps from filesystem | May not reflect actual data creation time | Use filesystem times as best-effort approximation |
| Corrupted parquet files | Migration will fail for that dataset | Log error and continue with other datasets |
| Empty dataset folders | Cannot extract schema | Skip and log warning |
| Non-Hive partitioning | Partitioning won't be detected | Assume no partitioning, log warning |

### 8.8 Post-Migration Validation

After migration, users can validate the catalog:

```bash
# List all registered datasets
uv run python -m brasa.cli list-datasets

# Verify a specific dataset can be loaded with correct schema
uv run python -m brasa.cli head staging.b3-indexes-historical-prices

# Compare catalog schema with template schema (if template exists)
uv run python -m brasa.cli describe-dataset input.b3-cotahist --compare-template
```

### 8.9 Migration Tasks (Detailed)

Expanding TASK-020 and TASK-021 with specific sub-tasks:

| Task       | Description                                                                                           | Completed | Date |
|------------|-------------------------------------------------------------------------------------------------------|-----------|------|
| TASK-020a  | Implement `_infer_hive_partitioning(dataset_dir, parquet_file) -> list[str]` function                 |           |      |
| TASK-020b  | Implement `MigrationReport` dataclass with statistics tracking                                        |           |      |
| TASK-020c  | Implement `sync_catalog_from_disk(layer, dry_run, force) -> MigrationReport` function                 |           |      |
| TASK-020d  | Add `sync-catalog` subcommand to CLI with `--layer`, `--dry-run`, `--force`, `--verbose` options      |           |      |
| TASK-020e  | Implement JSON output format for `sync-catalog` command                                               |           |      |
| TASK-021a  | Add `--compare-template` flag to `describe-dataset` to show schema differences                        |           |      |
| TASK-021b  | Create utility function `compare_schemas(catalog_schema, template_schema) -> list[Difference]`        |           |      |
| TASK-021c  | Document migration process in `docs/USER_GUIDE.md`                                                    |           |      |

## 9. Related Specifications / Further Reading

- [PyArrow Schema Serialization](https://arrow.apache.org/docs/python/ipc.html)
- [Hive Partitioning](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html#partition-discovery)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) - Current architecture documentation
- [brasa/engine/cache.py](brasa/engine/cache.py) - CacheManager implementation pattern
- [brasa/engine/layers.py](brasa/engine/layers.py) - DataLayer enum definition

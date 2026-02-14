# Plan: DuckDB SQL Interface for Layer-Based Datasets

**TL;DR**: Extend `BrasaDB` class in `brasa/queries.py` with layer-aware view creation and SQL execution. Views will use `layer.dataset` naming (e.g., `"input.b3-cotahist-daily"`) for explicit dataset access. Users call `BrasaSQLDB.create_all_views()` once per session, then execute SQL queries. The implementation leverages the existing `DatasetCatalog` to discover datasets and apply proper schema/partitioning metadata from parquet files, replacing the current limited view creation logic with full type-aware views.

## Steps

### 1. Extend `BrasaDB` class with `create_all_views()` method in `brasa/queries.py`

- Iterate through `DatasetCatalog.list_datasets()` to get all datasets with layer information
- For each layer/dataset pair, construct view name as `"{layer}.{dataset_name}"`
- Load parquet files from `CacheManager.db_path(f"{layer}/{dataset_name}")`
- Extract schema and partitioning from `get_catalog_schema()` (lines 227-254 in queries.py)
- Use `DuckDB.read_parquet(path, schema, partitioning).create_view(view_name)` to create typed views
- Log success/failure for each view with counter
- Add error handling for missing paths (gracefully skip if dataset directory doesn't exist)

### 2. Add `query()` static method to `BrasaDB` in `brasa/queries.py`

- Accept SQL string as parameter
- Call `BrasaDB.get_connection().sql(sql)`
- Return DuckDB relation (user calls `.df()` to convert to pandas or `.fetch_all()` for records)
- Document that views must exist first (requires calling `create_all_views()`)

### 3. Add convenience `list_tables()` static method in `brasa/queries.py`

- Query DuckDB's `information_schema.tables` for views named with "." (layer-prefixed)
- Return list of `layer.dataset` strings for discovery
- Filter to only user-created views (exclude DuckDB system tables)

### 4. Add module-level convenience functions at end of `brasa/queries.py`

- `create_all_views()` → delegates to `BrasaDB.create_all_views()`
- `sql(query_string)` → delegates to `BrasaDB.query()`
- `list_sql_tables()` → delegates to `BrasaDB.list_tables()`
- Matches existing pattern of module-level exports (see `__all__` at line 16)

### 5. Update `__all__` export list in `brasa/queries.py`

- Add `"BrasaDB"` (was already there, ensure it remains)
- Add new exports: `"create_all_views"`, `"sql"`, `"list_sql_tables"`

### 6. Handle edge cases in view creation

- Skip views if parquet directory contains no files
- Handle datasets with special characters in names (DuckDB requires quote escaping)
- Reconstruct partition schema from catalog to ensure filter-pushdown works
- Log warnings if catalog schema differs from actual parquet metadata

### 7. Export convenience functions in `brasa.__init__.py` (optional, for top-level API)

- Consider adding `sql()` function to public API for discoverability
- Allows users to `from brasa import sql` instead of `from brasa.queries import sql`

### 8. Enhance existing CLI `query` command in `brasa/cli.py`

The CLI already has a `query` command (line 151-427) that executes SQL directly. Integration points:

- **Add view initialization**: Check if views exist before query execution
  - Option A: Auto-create views on first query (seamless but slower first time)
  - Option B: Require `brasa create-views` command first (explicit, predictable)
  - **Recommendation**: Option A with progress output

- **Update `create-views` command** (currently at line 422):
  - Change `BrasaDB.create_views()` to `BrasaDB.create_all_views()`
  - Add layer filter option: `brasa create-views --layer input`
  - Display table of created views with success/failure status

- **Enhance `query` command output** (currently at line 427):
  - Add `--format` option as alias for `--output` for consistency
  - Add `--verbose` to show query plan or explain
  - Add `--list-tables` to show available tables directly in query
  - Support piping for shell integration: `brasa query "SELECT * FROM ..."" | head`

- **Add `list-tables` command** (new CLI command):
  - List all available layer.dataset tables
  - Show row count and estimated size for each
  - Filter by layer: `brasa list-tables --layer input`
  - Usage: `brasa list-tables` or `brasa query --list-tables`

## Verification

### Manual Testing in Python REPL

```python
from brasa.queries import BrasaDB, sql, list_sql_tables

# Create all views
BrasaDB.create_all_views()  # ✓ Should print "Created view: input.b3-cotahist-daily" etc.

# List available tables
tables = list_sql_tables()
assert "input.b3-cotahist-daily" in tables  # ✓ Check layer.dataset naming

# Execute query
result = sql('SELECT COUNT(*) as cnt FROM "input.b3-cotahist-daily"')
row = result.fetchone()
assert row[0] > 0  # ✓ Should have data

# Verify schema and partitioning work
df = sql('SELECT * FROM "input.b3-cotahist-daily" WHERE refdate = "2024-01-15" LIMIT 1').df()
assert len(df) <= 1  # ✓ Partition filter should work
```

### CLI Usage Examples

```bash
# Create all views (new simplified interface)
brasa create-views

# Create views for specific layer
brasa create-views --layer input

# List all available tables
brasa list-tables

# List tables for specific layer with sizes
brasa list-tables --layer staging --verbose

# Execute SQL query and display results
brasa query 'SELECT * FROM "input.b3-cotahist-daily" LIMIT 10'

# Save query results to file
brasa query 'SELECT * FROM "input.b3-cotahist-daily"' --output results.csv
brasa query 'SELECT * FROM "input.b3-cotahist-daily"' --output results.parquet

# Complex query with filtering
brasa query 'SELECT symbol, COUNT(*) as cnt FROM "input.b3-cotahist-daily" WHERE refdate >= "2024-01-01" GROUP BY symbol'

# Show query plan (will require --verbose flag)
brasa query 'SELECT * FROM "input.b3-cotahist-daily"' --verbose
```

### Pytest Tests in `tests/`

- `test_brasadb_create_views()`: Verify all views are created
- `test_brasadb_layer_naming()`: Verify views use `layer.dataset` pattern
- `test_brasadb_query_execution()`: Run sample queries and validate results
- `test_brasadb_schema_preservation()`: Verify schema types match catalog
- `test_brasadb_view_discovery()`: Test `list_sql_tables()` functionality

### Integration Test with Existing Functions

- Compare results from `get_dataset()` (PyArrow API) with `sql()` queries
- Verify both return same data for the same filters

## Decisions Rationale

**Layer.dataset naming**: Chosen over simple dataset naming to support queries like `SELECT * FROM "input.b3-cotahist-daily"` which makes layer context explicit and allows querying across layers.

**Explicit view creation**: Requires user call `create_all_views()` rather than lazy auto-create—keeps startup predictable, allows user to see what views are available.

**SQL-only API**: Starts with raw SQL to keep scope focused; DSL or query builder can be added later as `queries_sql_dsl.py`.

**Extend existing queries.py**: Keeps related functionality together rather than fragmenting; `BrasaDB` becomes the central DuckDB interface.

**Reuse DatasetCatalog**: Leverages existing catalog system to discover datasets and preserve schema metadata, avoiding duplicating logic.

**Graceful error handling**: Non-existent datasets are skipped with warning—allows catalog to have datasets that haven't been populated yet.

## Implementation Notes

### Python API vs CLI Integration

The implementation provides **two complementary interfaces**:

**Python API** (`brasa.queries.BrasaDB`):
- `BrasaDB.create_all_views()`: Create views programmatically
- `BrasaDB.query(sql)`: Execute SQL and get DuckDB relation
- Module-level shortcuts: `from brasa.queries import sql, list_sql_tables`
- Use case: Jupyter notebooks, data analysis scripts, applications

**CLI Interface** (`brasa` command):
- `brasa create-views`: Initialize views from command line
- `brasa query "SELECT ..."`: Execute SQL queries
- `brasa list-tables`: Discover available datasets
- Use case: Shell scripts, ad-hoc exploration, integration with other tools

Both interfaces share the same underlying `BrasaDB` class and DuckDB database—
changes made through one are immediately visible in the other.

### Key Code Locations

- `brasa/queries.py` line 23: `BrasaDB` class definition (add methods here)
- `brasa/queries.py` line 16: `__all__` export list (update with new exports)
- `brasa/queries.py` line 227-254: `get_catalog_schema()` function (reuse for schema extraction)
- `brasa/queries.py` line 735: End of file (add module-level convenience functions here)
- `brasa/engine/catalog.py`: `DatasetCatalog` class (use for dataset discovery)
- `brasa/engine/cache.py`: `CacheManager` class (use for path construction)
- `brasa/cli.py` line 151-427: Existing `query` command implementation (enhance for new views)
- `brasa/cli.py` line 422: `create-views` command (update to use `create_all_views()`)
- `brasa/cli.py` line 110-115: Add new `create-views` argument parser if layer filter needed

### Method Signatures

```python
@classmethod
def create_all_views(cls, layers: list[str] | None = None) -> dict[str, bool]:
    """Create DuckDB views for all datasets with layer.dataset naming.

    Args:
        layers: Optional list of layers to create views for (default: all)
               Valid values: 'raw', 'input', 'staging', 'curated'

    Returns:
        Dictionary mapping view_name -> success_bool

    Example:
        BrasaDB.create_all_views(['input', 'staging'])
        BrasaDB.create_all_views()  # All layers
    """

@classmethod
def query(cls, sql: str) -> duckdb.DuckDBPyRelation:
    """Execute SQL query on Brasa datasets.

    Args:
        sql: SQL query string

    Returns:
        DuckDB relation (.df() to convert to pandas, .fetch_all() for records)

    Example:
        result = BrasaDB.query('SELECT * FROM "input.b3-cotahist-daily" LIMIT 10')
        df = result.df()
    """

@classmethod
def list_tables(cls) -> list[str]:
    """List all available layer.dataset tables."""
```

### CLI Command Implementation

**`brasa create-views`** (update existing):
- Replace `BrasaDB.create_views()` with `BrasaDB.create_all_views()`
- Add optional `--layer` argument to filter layer creation
- Display status table: `✓ Created view: input.b3-cotahist-daily | Rows: 1,234,567`
- Show total created and any errors

**`brasa query`** (enhance existing):
- Keep backward compatibility with existing SQL interface
- Add progress/setup: If views don't exist, auto-create them with progress output
- Add `--list-tables` flag to show available tables instead of executing query
- Ensure output format options (CSV, JSON, Parquet, Excel) work with new layer.dataset tables

**`brasa list-tables`** (new optional convenience command):
- Call `BrasaDB.list_tables()` and format as table
- Add `--layer` filter option
- Add `--verbose` to show row counts and estimated sizes per dataset
- Example output:
  ```
  Layer    | Dataset Name                 | Rows  | Size
  ---------|------------------------------|-------|----------
  input    | b3-cotahist-daily            | 1.2M  | 245 MB
  input    | b3-equities-returns          | 542K  | 89 MB
  staging  | b3-indexes-composition       | 2.8K  | 1.2 MB
  ```



### Error Handling Strategy

1. **Missing parquet directory**: Log warning, skip view creation
2. **Schema mismatch**: Log warning, use parquet metadata as fallback
3. **Special characters in names**: Escape properly for DuckDB quoting
4. **Empty parquet folders**: Skip silently (dataset not yet populated)
5. **Corrupted parquet files**: Let DuckDB error bubble up (user should fix data)

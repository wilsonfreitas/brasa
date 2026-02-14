# Plan: DataFusion SQL Interface for Large-Scale Analytics

**TL;DR**: Create a separate `BrasaDataFusion` class (not extending `BrasaDB`) optimized for large analytical queries (>1GB datasets) with streaming support. Unlike DuckDB's persistent views, DataFusion uses session-scoped table registration with `catalog.schema.table` naming. Tables are registered on-demand per query session, leveraging DataFusion's streaming execution and vectorized batch processing. Target use cases: analytics aggregations, streaming pipelines, and queries requiring custom Python UDFs.

## Design Philosophy

- **Separation of concerns**: DataFusion is an alternative backend, not an extension of DuckDB
- **Session-scoped**: Fresh table registration per SessionContext (stateless, no persistent state)
- **Catalog hierarchy**: Use `{layer}.datasets.{dataset_name}` for table organization
- **Streaming-first**: Optimize for batch processing and lazy evaluation
- **Native API**: Use DataFusion's SessionContext conventions, not DuckDB style

## Steps

### 1. Create `brasa/queries_datafusion.py` module for DataFusion integration

- New file separate from `queries.py` to keep backends isolated
- Import `SessionContext`, `SessionConfig`, `RuntimeEnvBuilder` from `datafusion`
- Define `BrasaDataFusion` class (not inheriting from `BrasaDB`)
- Module structure:
  ```
  BrasaDataFusion:
    - create_session() → SessionContext with Brasa config
    - register_layer(layer_name) → Register all datasets in a layer
    - register_dataset(layer, dataset_name) → Register single dataset
    - query(sql) → Execute and return results
    - query_streaming(sql) → Return lazy DataFrame for streaming
  ```
- Add module-level convenience functions for API consistency

### 2. Implement `BrasaDataFusion.create_session()` class method

- Create `SessionConfig` with optimizations:
  - `with_target_partitions(num_cpus)`: CPU-count parallel execution
  - `set("datafusion.execution.parquet.pushdown_filters", "true")`: Filter pushdown
  - `set("datafusion.execution.repartition_joins", "true")`: Join optimization
  - `set("datafusion.execution.repartition_aggregations", "true")`: Agg optimization
  - `with_information_schema(True)`: Enable schema inspection

- Create `RuntimeEnvBuilder` with:
  - `with_disk_manager_os()`: Enable spilling to disk for large ops
  - `with_fair_spill_pool(size)`: Configure memory spilling budget

- Return fully configured `SessionContext` ready for table registration
- Log session creation with configuration for debugging

### 3. Implement `BrasaDataFusion.register_layer(layer_name)` method

- Accept optional `layer_name` to register all datasets in a specific layer
- Iterate through `DatasetCatalog.list_datasets(layer=layer_name)`
- For each dataset, call `register_dataset(layer, dataset_name)`
- Handle missing directories gracefully (skip with warning)
- Return dict of `table_name → success_bool`
- Output progress: `✓ Registered: input_b3_cotahist_daily`

**Table naming strategy**:
- Use `{layer}_{dataset_name}` pattern (underscores instead of dots)
- Reason: DataFusion's `catalog.schema.table` model doesn't support dots in table names
- Alternative: Use catalog.schema approach if user prefers explicit layer organization

### 4. Implement `BrasaDataFusion.register_dataset()` class method

- Parameters: `session: SessionContext`, `layer: str`, `dataset_name: str`
- Construct path: `CacheManager.db_path(f"{layer}/{dataset_name}")`
- Validate directory exists and contains parquet files
- Get schema from `DatasetCatalog` for type preservation
- Call `session.register_parquet(table_name, path)`
- Automatic Hive partition detection (DataFusion does this by default)
- Log table registration with row count estimate
- Return `bool` success indicator

### 5. Implement `BrasaDataFusion.query()` class method

- Parameters: `sql: str`, `layer: str | None = None`
- Create new `SessionContext` for stateless query execution
- Auto-register required layer if specified (e.g., if query uses `input_*` tables, register input layer)
- Execute `session.sql(sql_query)`
- Convert result based on return type needs:
  - Default: `to_pandas()` for consistency with DuckDB
  - Support `to_arrow_table()` option for zero-copy operations
  - Support `to_pylist()`/`to_pydict()` for structured data
- Error handling: Catch DataFusion exceptions and re-raise with context
- Return result (user calls `.to_pandas()`, `.to_arrow_table()`, etc.)

### 6. Implement `BrasaDataFusion.query_streaming()` class method

- Parameters: `sql: str`, `layer: str | None = None`
- Create SessionContext and register datasets
- Execute query and return **lazy** DataFrame (not materialized)
- User can iterate: `for batch in df.execute_stream()`
- Enable chunked processing for large result sets
- User responsibility to call `.collect()` or iterate

### 7. Add `BrasaDataFusion.session()` context manager (optional)

- Provides session lifetime management for multiple queries
- Reuses registered tables across queries in same session
- Example usage:
  ```python
  with BrasaDataFusion.session(['input', 'staging']) as ctx:
      df1 = ctx.sql("SELECT * FROM input_dataset1")
      df2 = ctx.sql("SELECT * FROM staging_dataset2 WHERE ...")
      # Join across datasets without re-registering
  ```

### 8. Add module-level convenience functions in `brasa/queries_datafusion.py`

- `create_session() → SessionContext`: Shortcut to `BrasaDataFusion.create_session()`
- `query(sql, layer=None)`: Shortcut to `BrasaDataFusion.query()`
- `query_streaming(sql, layer=None)`: Shortcut to streaming variant
- `list_tables(layer=None) → list[str]`: List registered tables (requires schema query)

### 9. Export from `brasa/__init__.py` (optional for public API)

- Add to `__all__`: `"BrasaDataFusion"`, `"datafusion_query"`
- Allows `from brasa import BrasaDataFusion` or `from brasa import datafusion_query`

### 10. Add dependencies to `pyproject.toml`

- `datafusion >= 0.43.0` as optional dependency
- Add to `[tool.poetry.extras]` for `analytics` extra: `datafusion`
- Installation: `poetry install -E analytics` or `poetry add --group dev datafusion`

### 11. Add DataFusion backend support to CLI in `brasa/cli.py`

The existing `query` command can be extended to support DataFusion as an optional backend:

- **Add `--engine` flag to `query` command:**
  - `brasa query --engine duckdb "SELECT ..."` (default, uses DuckDB)
  - `brasa query --engine datafusion "SELECT ..."` (uses DataFusion for large queries)
  - Recommendation: Make DuckDB default for backward compatibility

- **Create new `analytics-query` command (alternative):**
  - Dedicated command for DataFusion-optimized analytics
  - `brasa analytics-query "SELECT refdate, COUNT(*) FROM input_b3_cotahist_daily GROUP BY refdate"`
  - Auto-detects table layer and registers datasets
  - Better for streaming/large dataset use cases
  - Returns results as formatted table or file output

- **Add `--streaming` flag to `query`:**
  - `brasa query --engine datafusion --streaming "SELECT * FROM input_large_dataset"`
  - Processes in batches instead of materializing all results
  - Shows progress bar for batch iteration
  - Reduces memory footprint for large result sets

- **Add `--explain` flag:**
  - `brasa query --explain "SELECT ..."` (DataFusion only)
  - Show DataFusion query execution plan and optimization decisions
  - Useful for understanding performance characteristics

### 12. Update argument parser in `brasa/cli.py`

For `query` subcommand (line 151):
```python
parser_query = subparsers.add_parser(
    "query", help="execute SQL queries on brasa database"
)
parser_query.add_argument("sql_query", nargs=1, help="SQL query to be executed")
parser_query.add_argument(
    "--engine",
    choices=["duckdb", "datafusion"],
    default="duckdb",
    help="Query engine to use (default: duckdb)"
)
parser_query.add_argument(
    "--layer",
    help="Auto-register specified layer before query (datafusion only)"
)
parser_query.add_argument(
    "--streaming",
    action="store_true",
    help="Use streaming execution for large results (datafusion only)"
)
parser_query.add_argument(
    "--explain",
    action="store_true",
    help="Show query execution plan (datafusion only)"
)
```

For new `analytics-query` command (optional):
```python
parser_analytics_query = subparsers.add_parser(
    "analytics-query",
    help="execute analytics queries optimized for large datasets (DataFusion)"
)
parser_analytics_query.add_argument("sql_query", nargs=1, help="SQL query")
parser_analytics_query.add_argument(
    "--layer",
    help="Auto-register layer (input, staging, curated)"
)
parser_analytics_query.add_argument(
    "--streaming",
    action="store_true",
    help="Stream results in batches (default: true for large results)"
)
parser_analytics_query.add_argument(
    "-o", "--output",
    default="display",
    help="Output format: display, .csv, .json, .parquet, .xlsx"
)
```

### 13. Implement CLI command routing in `brasa/cli.py`

- In `main()` function, add condition:
  ```python
  elif args.command == "query":
      if args.engine == "datafusion":
          from brasa.queries_datafusion import BrasaDataFusion
          session = BrasaDataFusion.create_session()
          if args.layer:
              BrasaDataFusion.register_layer(session, args.layer)

          if args.explain:
              # Show query plan first
              plan = session.sql(args.sql_query[0]).explain()
              print(plan)

          if args.streaming:
              df = BrasaDataFusion.query_streaming(args.sql_query[0], args.layer)
              # Stream and display with progress
              for batch in df.execute_stream():
                  print(batch.to_pandas())
          else:
              df = BrasaDataFusion.query(args.sql_query[0], args.layer)
              print(df)
      else:
          # Existing DuckDB implementation
          q = BrasaDB.get_connection().sql(args.sql_query[0])
          output = args.output[0]
          # ... existing code ...

  elif args.command == "analytics-query":
      from brasa.queries_datafusion import BrasaDataFusion
      # Similar routing to query command above
  ```

## Key Design Decisions

**Session-scoped vs persistent**: DataFusion doesn't have persistent views in the same way DuckDB does. Each session is fresh, tables register on-demand. This is **intentional for stateless design**—any persistent state goes in `DatasetCatalog`, not DuckDB views.

**Naming convention**: Use `{layer}_{dataset_name}` instead of `input.b3-cotahist` because DataFusion enforces `catalog.schema.table` semantics. Dots would be misinterpreted as hierarchy.

**CLI integration strategy**: DataFusion can be integrated as an optional backend to the existing `brasa query` command via `--engine datafusion` flag, or as a dedicated `brasa analytics-query` command for analytics-focused workflows. This allows users to choose the right tool for their query:
- **DuckDB** (default): General SQL queries, persistent views, familiar API
- **DataFusion** (optional): Large analytical queries, streaming, explicit performance tuning

**Separate from DuckDB**: Not extending `BrasaDB` keeps backends independent. Users choose based on use case:
- **DuckDB** (via `brasa.queries.BrasaDB`): General SQL, persistent views, familiar API
- **DataFusion** (via `brasa.queries_datafusion.BrasaDataFusion`): Large queries, streaming, custom UDFs

## Verification

### Manual Testing in Python REPL

```python
from brasa.queries_datafusion import BrasaDataFusion, query

# Create and test session
session = BrasaDataFusion.create_session()
BrasaDataFusion.register_layer(session, "input")

# Execute analytics query
result = query('SELECT refdate, COUNT(*) as cnt FROM input_b3_cotahist_daily GROUP BY refdate')
df = result.to_pandas()
assert len(df) > 0  # ✓ Should have aggregated data

# Streaming large query
large_query = query_streaming('SELECT * FROM input_large_dataset')
batch_count = 0
for batch in large_query.execute_stream():
    batch_count += 1
assert batch_count > 0  # ✓ Streaming should process multiple batches

# Context manager usage
from brasa.queries_datafusion import session as df_session
with df_session(['input', 'staging']) as ctx:
    df1 = ctx.sql("SELECT COUNT(*) FROM input_dataset1").to_pandas()
    df2 = ctx.sql("SELECT COUNT(*) FROM staging_dataset1").to_pandas()
```

### CLI Usage Examples

```bash
# Use DataFusion with existing query command
brasa query --engine datafusion 'SELECT refdate, COUNT(*) FROM input_b3_cotahist_daily GROUP BY refdate'

# Large dataset query with streaming to avoid loading all in memory
brasa query --engine datafusion --streaming 'SELECT * FROM input_b3_cotahist_daily WHERE refdate >= "2024-01-01"'

# Show DataFusion query execution plan for optimization insights
brasa query --engine datafusion --explain 'SELECT * FROM input_b3_cotahist_daily WHERE symbol = "MGLU3"'

# Dedicated analytics query command (alternative)
brasa analytics-query 'SELECT symbol, COUNT(*) as trades FROM input_b3_cotahist_daily GROUP BY symbol ORDER BY trades DESC'

# Analytics query with output to file
brasa analytics-query 'SELECT * FROM input_b3_cotahist_daily LIMIT 1000' --output results.parquet

# Stream large query to CSV (batch by batch)
brasa analytics-query --streaming 'SELECT * FROM input_large_dataset' --output results.csv

# Auto-register specific layer before querying
brasa analytics-query --layer staging 'SELECT * FROM staging_some_dataset WHERE ...'
```

### Pytest Tests in `tests/`

- `test_datafusion_session_creation()`: Verify SessionContext is created with correct config
- `test_datafusion_register_layer()`: Verify all datasets in layer are registered
- `test_datafusion_register_dataset()`: Single dataset registration and validation
- `test_datafusion_query_execution()`: Execute sample queries and validate results
- `test_datafusion_query_streaming()`: Verify lazy execution and batch iteration
- `test_datafusion_table_naming()`: Confirm `layer_dataset` naming convention
- `test_datafusion_large_query()`: Test with dataset > 1GB (if available)
- `test_datafusion_context_manager()`: Verify session lifetime and cleanup
- `test_datafusion_error_handling()`: Missing tables, invalid SQL, type errors

### CLI Integration Tests in `tests/test_cli_datafusion.py`

- `test_cli_query_with_datafusion_engine()`: Execute `brasa query --engine datafusion "SELECT ..."`
- `test_cli_query_streaming_flag()`: Verify `--streaming` flag enables lazy execution
- `test_cli_query_explain_flag()`: Verify `--explain` shows query plan
- `test_cli_analytics_query_command()`: Test `brasa analytics-query` command
- `test_cli_analytics_query_with_layer()`: Test `--layer` flag for auto-registration
- `test_cli_analytics_query_output_formats()`: Test CSV, JSON, Parquet output formats
- `test_cli_query_engine_default()`: Ensure DuckDB remains default when `--engine` not specified
- `test_cli_compatibility()`: Verify DataFusion queries produce same results as DuckDB

### Comparison with DuckDB

- Run same query against both backends:
  ```python
  from brasa.queries import sql as duckdb_sql
  from brasa.queries_datafusion import query as datafusion_query

  duckdb_df = duckdb_sql('SELECT * FROM "input.b3-cotahist-daily" LIMIT 100').df()
  datafusion_df = datafusion_query('SELECT * FROM input_b3_cotahist_daily LIMIT 100').to_pandas()

  assert duckdb_df.shape == datafusion_df.shape  # ✓ Same results
  ```

## Implementation Notes

### Architecture Diagram

```
BrasaDataFusion (queries_datafusion.py)
├── create_session() → SessionContext
├── register_layer() → Register all datasets
├── register_dataset() → Register single dataset
├── query() → Execute and materialize
├── query_streaming() → Lazy execution
└── session() → Context manager for multi-query

DatasetCatalog (existing)
└── Provides schema and dataset metadata

CacheManager (existing)
└── Provides parquet file paths

CLI Routes (brasa/cli.py)
├── query --engine datafusion → Python API routing
├── query --engine duckdb → Existing DuckDB path (default)
└── analytics-query → Dedicated DataFusion command
```

### Python API vs CLI Integration

The implementation provides **two complementary interfaces**:

**Python API** (`brasa.queries_datafusion.BrasaDataFusion`):
- `BrasaDataFusion.create_session()`: Create configured SessionContext
- `BrasaDataFusion.query(sql)`: Execute and materialize results
- `BrasaDataFusion.query_streaming(sql)`: Lazy execution for large datasets
- Module-level shortcuts: `from brasa.queries_datafusion import query, query_streaming`
- Use case: Jupyter notebooks, data analysis scripts, applications

**CLI Interface** (`brasa` command):
- `brasa query --engine datafusion "SELECT ..."`: Use DataFusion for single query
- `brasa analytics-query "SELECT ..."`: Dedicated analytics command
- `--streaming`: Process results in batches (memory-efficient)
- `--explain`: Show query execution plan
- Use case: Shell scripts, ad-hoc exploration, integration with other tools

Both interfaces share the same underlying `BrasaDataFusion` class—no duplication of logic, clean separation of concerns.

### Code Locations

- `brasa/queries_datafusion.py` (new file): All DataFusion integration
- `brasa/__init__.py`: Export `BrasaDataFusion` (optional)
- `pyproject.toml`: Add `datafusion` dependency
- `tests/test_datafusion_queries.py` (new): Test suite
- `brasa/cli.py` (line 151): Enhance `query` command with `--engine` flag
- `brasa/cli.py` (line 110-115): Add argument parser for `analytics-query` command (optional)
- `brasa/cli.py` (routing logic): Add DataFusion backend selection in command handler
- `tests/test_cli_datafusion.py` (new): CLI integration tests

### Dependencies

```toml
[tool.poetry.dependencies]
# Existing
duckdb = "^1.4.0"
pyarrow = "^15.0.0"

# New optional dependency for analytics
datafusion = { version = "^0.43.0", optional = true }

[tool.poetry.extras]
analytics = ["datafusion"]
```

### Performance Characteristics

**DataFusion excels at:**
- Streaming/batched processing of large files (> 1GB)
- Vectorized SIMD operations on columnar data
- Memory-efficient aggregations with spilling
- Multi-threaded parallel execution across many CPUs
- Custom Python UDFs for domain-specific logic

**DuckDB remains better for:**
- Ad-hoc interactive queries
- Small-to-medium datasets
- Statistical/mathematical functions
- JSON/semi-structured data handling
- CLI integration

**Use case selection matrix:**

| Query Type | Size | DuckDB | DataFusion |
|-----------|------|--------|-----------|
| Time-series aggregation | < 500MB | ✅ DuckDB | DataFusion equivalent |
| Time-series aggregation | > 1GB | Possible | ✅ DataFusion better |
| JSON/semi-struct | Any | ✅ DuckDB | DataFusion limited |
| Large join | > 1GB | ✅ DuckDB fast | DataFusion parallel |
| Streaming pipeline | Any | No | ✅ DataFusion native |
| UDF analytics | Any | Limited | ✅ DataFusion native |
| Interactive REPL | Any | ✅ DuckDB | DataFusion simpler |

## Future Enhancements

1. **Ballista integration**: Use DataFusion's distributed engine for cluster queries
2. **Custom UDF registry**: Pre-load financial analysis functions
3. **Query optimizer hints**: Help DataFusion with domain-specific optimizations
4. **Federated queries**: Join DuckDB and DataFusion results
5. **Streaming sources**: Integrate with Kafka/event streams via DataFusion connectors

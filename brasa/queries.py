import json
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow
import pyarrow.compute as pc
import pyarrow.dataset as ds
from bizdays import Calendar, get_option, set_option

from .engine import CacheManager, DatasetCatalog, DatasetInfo, retrieve_template
from .fieldsets import PyArrowAdapter

__all__ = [
    "BrasaDB",
    "create_all_views",
    "describe",
    "describe_dataset",
    "get_dataset",
    "get_industry_sectors",
    "get_prices",
    "get_returns",
    "get_symbols",
    "get_template_dataset",
    "get_template_layer",
    "get_template_partitioning",
    "get_template_schema",
    "list_datasets",
    "list_sql_tables",
    "show",
    "sql",
    "write_dataset",
]


class BrasaDB:
    connection: duckdb.DuckDBPyConnection | None = None

    @classmethod
    def path(cls) -> str:
        man = CacheManager()
        return man.cache_path(man.duckdb_filename)

    @classmethod
    def get_connection(cls) -> duckdb.DuckDBPyConnection:
        if cls.connection is None:
            cls.connection = duckdb.connect(database=cls.path(), read_only=False)
        else:
            try:
                cls.connection.sql("SELECT 42")
            except duckdb.ConnectionException:
                cls.connection.close()
                cls.connection = duckdb.connect(database=cls.path(), read_only=False)
        return cls.connection

    @classmethod
    def create_view(cls, template: str) -> None:
        """Create a view for a single dataset (legacy method).

        Deprecated: Use create_all_views() instead for layer-aware view creation.
        """
        man = CacheManager()
        con = cls.get_connection()
        con.read_parquet(man.db_path(f"{template}/*.parquet")).create_view(template)

    @classmethod
    def create_views(cls) -> None:
        """Create views for all datasets (legacy method).

        Deprecated: Use create_all_views() instead for layer-aware view creation.
        """
        man = CacheManager()
        for p in Path(man.db_path("")).iterdir():
            try:
                p.name.index(".")
            except ValueError:
                cls.create_view(p.name)

    @classmethod
    def _create_single_view(
        cls,
        con: duckdb.DuckDBPyConnection,
        layer: str,
        dataset_name: str,
        dataset_info: "DatasetInfo",
        man: CacheManager,
    ) -> tuple[bool, str]:
        """Create a single view for a dataset.

        Handles both partitioned (hive) and non-partitioned parquet datasets.
        Partition columns are automatically exposed as queryable table columns.

        Returns:
            Tuple of (success, message).
        """
        view_name = f"{layer}.{dataset_name}"

        try:
            db_path = man.db_path(f"{layer}/{dataset_name}")
            parquet_path = Path(db_path)

            if not parquet_path.exists():
                msg = f"Dataset directory not found: {db_path}"
                return False, msg

            parquet_files = list(parquet_path.glob("**/*.parquet"))
            if not parquet_files:
                return False, "No parquet files found"

            # Use recursive glob pattern to find all parquet files.
            # This works for both:
            # - Non-partitioned: files in root directory
            # - Hive-partitioned: files in partition subdirectories (e.g., refdate=2024-01-15/)
            pattern = str(parquet_path / "**" / "*.parquet")

            if dataset_info.partitioning:
                # Enable hive_partitioning to expose partition columns as queryable columns
                con.execute(
                    f"""
                    CREATE OR REPLACE VIEW "{view_name}" AS
                    SELECT * FROM read_parquet('{pattern}', hive_partitioning=true)
                    """
                )
            else:
                # Non-partitioned dataset
                con.read_parquet(pattern).create_view(view_name)

            return True, f"Rows: ~{len(parquet_files)}"

        except Exception as e:
            return False, f"Error: {str(e)[:100]}"

    @classmethod
    def create_all_views(cls, layers: list[str] | None = None) -> dict[str, bool]:
        """Create DuckDB views for all datasets with layer.dataset naming.

        Discovers all datasets from the DatasetCatalog and creates views with
        proper schema and partitioning support. Views are named as "layer.dataset"
        (e.g., "input.b3-cotahist-daily") for explicit layer context.

        Args:
            layers: Optional list of specific layers to create views for.
                   If None, creates views for all layers.
                   Valid values: 'raw', 'input', 'staging', 'curated'.

        Returns:
            Dictionary mapping view_name -> success_bool.
            View names follow the pattern "layer.dataset_name".

        Example:
            # Create all views
            BrasaDB.create_all_views()

            # Create views only for input and staging layers
            BrasaDB.create_all_views(['input', 'staging'])
        """
        man = CacheManager()
        con = cls.get_connection()
        catalog = DatasetCatalog()

        results: dict[str, bool] = {}
        total_created = 0
        total_skipped = 0
        total_errors = 0

        # Get all datasets from catalog
        all_datasets = catalog.list_datasets(layer=None)

        # Filter by specified layers if provided
        if layers:
            datasets = [d for d in all_datasets if d.layer in layers]
        else:
            datasets = all_datasets

        if not datasets:
            print("No datasets found in catalog.")
            return results

        for dataset_info in datasets:
            layer = dataset_info.layer
            dataset_name = dataset_info.dataset_name
            view_name = f"{layer}.{dataset_name}"

            success, msg = cls._create_single_view(
                con, layer, dataset_name, dataset_info, man
            )

            if success:
                results[view_name] = True
                total_created += 1
                print(f"✓ Created view: {view_name:50s} | {msg}")
            else:
                results[view_name] = False
                if "not found" in msg or "No parquet" in msg:
                    total_skipped += 1
                    print(f"⊘ Skipped view: {view_name:50s} | {msg}")
                else:
                    total_errors += 1
                    print(f"✗ Failed view:  {view_name:50s} | {msg}")

        # Print summary
        print(
            f"\nSummary: {total_created} created, "
            f"{total_skipped} skipped, {total_errors} errors"
        )
        return results

    @classmethod
    def query(cls, sql: str) -> duckdb.DuckDBPyRelation:
        """Execute SQL query on Brasa datasets.

        Executes a SQL query against DuckDB and returns a relation object.
        The relation object supports methods like .df() to convert to pandas
        DataFrame, or .fetch_all() to get list of tuples.

        Note: Views must exist first. Call create_all_views() to initialize views.

        Args:
            sql: SQL query string. Can reference views named as "layer.dataset".

        Returns:
            DuckDB relation. Call .df() for pandas DataFrame or .fetch_all()
            for records.

        Example:
            # Create views first (usually done once per session)
            BrasaDB.create_all_views()

            # Execute query
            result = BrasaDB.query('SELECT COUNT(*) as cnt FROM "input.b3-cotahist-daily"')
            df = result.df()
            count = result.fetch_one()[0]
        """
        con = cls.get_connection()
        return con.sql(sql)

    @classmethod
    def list_tables(cls) -> list[str]:
        """List all available layer.dataset tables.

        Queries DuckDB's information_schema to discover all user-created views
        that follow the layer.dataset naming pattern.

        Returns:
            Sorted list of view names in "layer.dataset" format.

        Example:
            tables = BrasaDB.list_tables()
            for table in tables:
                print(table)  # e.g., "input.b3-cotahist-daily"
        """
        con = cls.get_connection()
        try:
            result = con.sql(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_type = 'VIEW'
                AND table_name LIKE '%.%'
                ORDER BY table_name
                """
            )
            df = result.df()
            return df["table_name"].tolist()
        except Exception:
            return []


# Module-level convenience functions
def create_all_views(layers: list[str] | None = None) -> dict[str, bool]:
    """Create all layer-aware views in the DuckDB database.

    Delegates to BrasaDB.create_all_views() for programmatic use.

    Args:
        layers: Optional list of specific layers to create views for.

    Returns:
        Dictionary mapping view_name -> success_bool.

    Example:
        create_all_views()  # Create all views
        create_all_views(['input', 'staging'])  # Specific layers
    """
    return BrasaDB.create_all_views(layers)


def sql(query_string: str) -> duckdb.DuckDBPyRelation:
    """Execute SQL query on Brasa datasets.

    Delegates to BrasaDB.query() for convenient SQL execution from REPL or scripts.

    Args:
        query_string: SQL query string with layer.dataset table references.

    Returns:
        DuckDB relation. Call .df() for pandas or .fetch_all() for records.

    Example:
        # Create views first
        create_all_views()

        # Execute query
        result = sql('SELECT * FROM "input.b3-cotahist-daily" LIMIT 10')
        df = result.df()
    """
    return BrasaDB.query(query_string)


def list_sql_tables() -> list[str]:
    """List all available layer.dataset tables.

    Delegates to BrasaDB.list_tables() for discovering available views.

    Returns:
        Sorted list of view names in "layer.dataset" format.

    Example:
        tables = list_sql_tables()
        print(f"Available tables: {tables}")
    """
    return BrasaDB.list_tables()


# def get_timeseries(symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
#     con = BrasaDB.get_connection()
#     _start = start.strftime("%Y-%m-%d")
#     _end = end.strftime("%Y-%m-%d")

#     res = con.sql(f"""
# with cal as (
#     select refdate
#     from calendar
#     where isbizday_B3 = true and refdate < today()
# ), ch as (
#     select refdate, symbol, close, distribution_id
#     from 'b3-cotahist'
#     where symbol = '{symbol}'
# ), minmax_dates as (
#     select
#         case when min(refdate) < '{_start}' then CAST('{_start}' as DATE) else min(refdate) end as min_date,
#         case when max(refdate) > '{_end}' then CAST('{end}' as DATE) else max(refdate) end as max_date
#     from ch
# )
# select * from (
#     select cal.refdate, ch.symbol, ch.close, ch.distribution_id
#     from cal
#     left join ch on cal.refdate = ch.refdate
#     where cal.refdate between (select min_date from minmax_dates) and (select max_date from minmax_dates)
# )
# order by refdate
# """)
#     return res.fetchdf().pivot(index="refdate", columns="symbol", values="close")


def get_returns(
    symbols: str | list[str], start=None, end=None, calendar="B3"
) -> pd.DataFrame:
    if isinstance(symbols, str):
        symbols = [symbols]
    if start is None:
        start = datetime(2000, 1, 1)
    if end is None:
        end = datetime.today()
    df = (
        get_dataset("brasa-returns")
        .filter(pc.field("symbol").isin(symbols))
        .filter(pc.field("refdate") >= start)
        .filter(pc.field("refdate") <= end)
        .to_table()
        .sort_by("refdate")
        .to_pandas()
    )
    df = df.pivot_table(values="returns", index="refdate", columns="symbol")
    df.index.name = None
    df.columns.name = None

    bizdays_mode = get_option("mode")
    set_option("mode", "pandas")
    cal = Calendar.load(calendar)
    idx = cal.seq(df.index[0], df.index[-1])
    set_option("mode", bizdays_mode)
    df = df.reindex(idx)
    return df


def get_prices(
    symbols: str | list[str],
    start=None,
    end=None,
    calendar: str = "B3",
    columns: str | list[str] = "close",
) -> pd.DataFrame:
    if isinstance(symbols, str):
        symbols = [symbols]
    if isinstance(columns, str):
        columns = [columns]
    all_names = ["refdate", "symbol"]
    all_names.extend(columns)
    if start is None:
        start = datetime(2000, 1, 1)
    if end is None:
        end = datetime.today()
    df = (
        get_dataset("brasa-prices")
        .filter(pc.field("symbol").isin(symbols))
        .filter(pc.field("refdate") >= start)
        .filter(pc.field("refdate") <= end)
        .scanner(columns=all_names)
        .to_table()
        .sort_by("refdate")
        .to_pandas()
    )
    if len(columns) == 1:
        df = df.pivot_table(values=columns[0], index="refdate", columns="symbol")
    else:
        df = df.pivot_table(values=columns, index="refdate", columns="symbol")
    df.index.name = None
    df.columns.name = None

    bizdays_mode = get_option("mode")
    set_option("mode", "pandas")
    cal = Calendar.load(calendar)
    idx = cal.seq(df.index[0], df.index[-1])
    set_option("mode", bizdays_mode)
    df = df.reindex(idx)
    return df


def get_template_schema(name: str) -> pyarrow.Schema | None:
    """Get PyArrow schema from a template's field definitions.

    Args:
        name: The template name (same as dataset name).

    Returns:
        PyArrow Schema if the template has fields defined, None otherwise.
    """
    try:
        template = retrieve_template(name)
        if hasattr(template, "fields") and template.fields is not None:
            adapter = PyArrowAdapter(template.fields, verbose_warnings=False)
            return adapter.get_target_schema()
    except ValueError:
        # Template not found
        pass
    return None


def get_template_partitioning(
    name: str, schema: pyarrow.Schema | None = None
) -> ds.Partitioning | None:
    """Get partitioning from a template's writer configuration.

    Args:
        name: The template name (same as dataset name).
        schema: Optional schema to extract partition field types from.

    Returns:
        PyArrow Partitioning object if partitioning is defined, None otherwise.
    """
    try:
        template = retrieve_template(name)
        if hasattr(template, "writer") and template.writer is not None:
            partitioning_cols = getattr(template.writer, "partitioning", None)
            if partitioning_cols:
                # Build partitioning with proper types from schema
                if schema is not None:
                    partition_fields = []
                    for col in partitioning_cols:
                        if col in schema.names:
                            partition_fields.append(schema.field(col))
                        else:
                            # Default to string if not in schema
                            partition_fields.append(
                                pyarrow.field(col, pyarrow.string())
                            )
                    return ds.partitioning(
                        pyarrow.schema(partition_fields), flavor="hive"
                    )
                else:
                    # Return simple partitioning without type info
                    return ds.partitioning(flavor="hive")
    except ValueError:
        # Template not found
        pass
    return None


def get_template_layer(name: str) -> str | None:
    """Get the data layer from a template's writer configuration.

    Args:
        name: The template name (same as dataset name).

    Returns:
        The layer string (e.g., 'input', 'staging', 'curated') if defined,
        None otherwise.
    """
    try:
        template = retrieve_template(name)
        if hasattr(template, "writer") and template.writer is not None:
            return template.writer.layer.value
    except ValueError:
        # Template not found
        pass
    return None


def get_template_dataset(template_name: str) -> str | None:
    """Get the dataset name from a template's writer configuration.

    The dataset name is used to determine the output folder path.
    If not explicitly set, it defaults to the template ID.

    Args:
        template_name: The template name to look up.

    Returns:
        The dataset name if the template exists, None otherwise.
    """
    try:
        template = retrieve_template(template_name)
        if hasattr(template, "writer") and template.writer is not None:
            return template.writer.dataset
    except ValueError:
        # Template not found
        pass
    return None


def get_catalog_schema(
    layer: str, dataset_name: str
) -> tuple[pyarrow.Schema | None, ds.Partitioning | None]:
    """Get schema and partitioning from the dataset catalog.

    Args:
        layer: The data layer (input, staging, curated).
        dataset_name: The name of the dataset.

    Returns:
        A tuple of (schema, partitioning) if found in catalog, (None, None) otherwise.
        The schema includes partition columns if they are not already present.
    """
    catalog = DatasetCatalog()
    info = catalog.get_dataset_info(layer, dataset_name)

    if info is None:
        return None, None

    schema = info.schema
    partitioning = None

    if info.partitioning:
        # Build partitioning with proper types from schema
        partition_fields = []
        for col in info.partitioning:
            if col in schema.names:
                partition_fields.append(schema.field(col))
            else:
                # Default to string if not in schema
                partition_fields.append(pyarrow.field(col, pyarrow.string()))
        partitioning = ds.partitioning(pyarrow.schema(partition_fields), flavor="hive")

        # Add partition columns to schema if not present
        # This ensures partition columns are included when reading the dataset
        schema_fields = list(schema)
        for field in partition_fields:
            if field.name not in schema.names:
                schema_fields.append(field)
        if len(schema_fields) > len(schema):
            schema = pyarrow.schema(schema_fields, metadata=schema.metadata)

    return schema, partitioning


def get_dataset(
    name: str,
    schema: pyarrow.Schema | None = None,
    partitioning: ds.Partitioning | list[str] | None = None,
    use_template_schema: bool = True,
    layer: str | None = None,
    use_catalog_schema: bool = True,
) -> ds.Dataset:
    """Load a dataset by name.

    The name can be either a template ID or a dataset name. The function attempts
    to load schema and partitioning in the following order:
    1. Explicitly provided schema/partitioning parameters
    2. Dataset catalog (if use_catalog_schema is True and layer is provided)
    3. Template definition (if use_template_schema is True)
    4. Raw parquet metadata (fallback)

    Args:
        name: The template ID or dataset name.
        schema: Optional PyArrow schema to use. If None, attempts to load from
            catalog or template.
        partitioning: Optional partitioning specification. Can be a list of column names
            or a PyArrow Partitioning object.
        use_template_schema: If True (default), automatically loads the schema and
            partitioning from the template when not provided.
        layer: Optional data layer override. If None and use_template_schema is True,
            attempts to load from the template. If layer is provided, it takes precedence.
        use_catalog_schema: If True (default), attempts to load schema from the
            dataset catalog when layer is provided and schema is not explicitly set.

    Returns:
        PyArrow Dataset.
    """
    man = CacheManager()
    dataset_name = name

    if use_template_schema:
        # Try to get layer from template if not provided
        if layer is None:
            layer = get_template_layer(name)
        # Get the actual dataset name from template (may differ from template ID)
        template_dataset = get_template_dataset(name)
        if template_dataset:
            dataset_name = template_dataset

    # Try catalog first if layer is available and schema not provided
    if use_catalog_schema and layer and schema is None:
        catalog_schema, catalog_partitioning = get_catalog_schema(layer, dataset_name)
        if catalog_schema is not None:
            schema = catalog_schema
        if catalog_partitioning is not None and partitioning is None:
            partitioning = catalog_partitioning

    # Fall back to template schema if still not found
    if use_template_schema and schema is None:
        schema = get_template_schema(name)
    if use_template_schema and partitioning is None:
        partitioning = get_template_partitioning(name, schema)

    # Build path with layer if available
    dataset_path = f"{layer}/{dataset_name}" if layer else dataset_name

    return ds.dataset(
        man.db_path(dataset_path),
        schema=schema,
        format="parquet",
        partitioning=partitioning,
    )


def list_datasets(layer: str | None = None) -> list[DatasetInfo]:
    """List all registered datasets in the catalog.

    Args:
        layer: Optional filter by data layer (input, staging, curated).
            If None, returns datasets from all layers.

    Returns:
        List of DatasetInfo objects containing metadata for each dataset.
    """
    catalog = DatasetCatalog()
    return catalog.list_datasets(layer)


def describe_dataset(
    layer: str, dataset_name: str, compare_template: bool = False
) -> dict:
    """Get detailed information about a dataset from the catalog.

    Args:
        layer: The data layer (input, staging, curated).
        dataset_name: The name of the dataset.
        compare_template: If True, includes comparison with template schema.

    Returns:
        Dictionary containing dataset metadata including:
        - id: Unique identifier
        - layer: Data layer
        - dataset_name: Name of the dataset
        - schema: List of field definitions (name, type, nullable)
        - partitioning: List of partition column names
        - source_template: Source template ID if applicable
        - created_at: Creation timestamp
        - updated_at: Last update timestamp
        - schema_differences: (only if compare_template=True) List of differences

    Raises:
        ValueError: If the dataset is not found in the catalog.
    """
    catalog = DatasetCatalog()
    info = catalog.get_dataset_info(layer, dataset_name)

    if info is None:
        raise ValueError(f"Dataset '{layer}/{dataset_name}' not found in catalog")

    result = {
        "id": info.id,
        "layer": info.layer,
        "dataset_name": info.dataset_name,
        "schema": [
            {
                "name": field.name,
                "type": str(field.type),
                "nullable": field.nullable,
            }
            for field in info.schema
        ],
        "partitioning": info.partitioning,
        "source_template": info.source_template,
        "created_at": info.created_at.isoformat(),
        "updated_at": info.updated_at.isoformat(),
    }

    if compare_template and info.source_template:
        template_schema = get_template_schema(info.source_template)
        if template_schema:
            differences = _compare_schemas(info.schema, template_schema)
            result["schema_differences"] = differences

    return result


def _compare_schemas(
    catalog_schema: pyarrow.Schema, template_schema: pyarrow.Schema
) -> list[dict]:
    """Compare catalog schema with template schema.

    Args:
        catalog_schema: Schema from the catalog.
        template_schema: Schema from the template.

    Returns:
        List of differences, each containing field name and description.
    """
    differences = []
    catalog_fields = {f.name: f for f in catalog_schema}
    template_fields = {f.name: f for f in template_schema}

    # Check for fields in catalog but not in template
    for name, field in catalog_fields.items():
        if name not in template_fields:
            differences.append(
                {
                    "field": name,
                    "issue": "in_catalog_only",
                    "catalog_type": str(field.type),
                }
            )
        elif str(field.type) != str(template_fields[name].type):
            differences.append(
                {
                    "field": name,
                    "issue": "type_mismatch",
                    "catalog_type": str(field.type),
                    "template_type": str(template_fields[name].type),
                }
            )

    # Check for fields in template but not in catalog
    for name, field in template_fields.items():
        if name not in catalog_fields:
            differences.append(
                {
                    "field": name,
                    "issue": "in_template_only",
                    "template_type": str(field.type),
                }
            )

    return differences


def describe(name: str) -> None:
    ds = get_dataset(name)
    sc = ds.schema
    cols = json.loads(sc.metadata[b"pandas"].decode("utf-8"))["columns"]
    for k in cols:
        print(k["field_name"] + ":", k["pandas_type"])


def show(name: str, n: int = 10):
    ds = get_dataset(name)
    return ds.head(n).to_pandas().style.format(thousands=",", precision=2)


def write_dataset(
    df: pd.DataFrame,
    name: str,
    format: str = "parquet",
    schema: pyarrow.Schema = None,
    layer: str | None = None,
) -> None:
    """Write a DataFrame as a dataset.

    The name can be either a template ID or a dataset name. When a template
    exists, the function uses its configuration for layer and dataset name.
    Also registers the dataset in the catalog.

    Args:
        df: DataFrame to write.
        name: The template ID or dataset name.
        format: Output format (default: 'parquet').
        schema: Optional PyArrow schema.
        layer: Optional data layer. If None, attempts to get from template.
    """
    man = CacheManager()
    dataset_name = name

    # Get layer and dataset name from template if not provided
    if layer is None:
        layer = get_template_layer(name)

    # Get the actual dataset name from template (may differ from template ID)
    template_dataset = get_template_dataset(name)
    if template_dataset:
        dataset_name = template_dataset

    # Build path with layer if available
    dataset_path = f"{layer}/{dataset_name}" if layer else dataset_name

    if schema:
        tb = pyarrow.Table.from_pandas(df, schema=schema)
    else:
        tb = pyarrow.Table.from_pandas(df)
    ds.write_dataset(
        tb,
        man.db_path(dataset_path),
        format=format,
        existing_data_behavior="overwrite_or_ignore",
    )

    # Register dataset in catalog if layer is available
    if layer:
        catalog = DatasetCatalog()
        catalog.register_dataset(
            layer=layer,
            dataset_name=dataset_name,
            schema=tb.schema,
            partitioning=[],  # write_dataset doesn't use partitioning
            source_template=name if template_dataset else None,
        )


def _get_equity_symbols(sector=None) -> list[str]:
    df = (
        get_dataset("b3-equity-symbols-properties")
        .scanner(columns=["symbol", "sector"])
        .to_table()
        .to_pandas()
    )
    if sector is not None:
        df = df[df.sector == sector]
    return list(df.symbol.unique())


def get_industry_sectors() -> pd.DataFrame:
    return (
        get_dataset("b3-equity-symbols-properties")
        .scanner(columns=["sector", "subsector", "segment"])
        .to_table()
        .to_pandas()
        .drop_duplicates()
        .sort_values(["sector", "subsector", "segment"])
        .reset_index(drop=True)
    )


def _get_companies_industry_sectors(column) -> list[str]:
    df = get_industry_sectors()
    return list(df[column].unique())


def _get_companies_trading_names() -> list[str]:
    df = (
        get_dataset("b3-companies-details")
        .scanner(columns=["refdate", "trading_name"])
        .to_table()
        .to_pandas()
    )
    df = df.groupby(["trading_name"], sort=True).last().reset_index()
    return list(df.trading_name.unique())


def _get_companies_names() -> list[str]:
    tb = get_dataset("b3-equities-register").scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    df = (
        get_dataset("b3-equities-register")
        .filter(pc.field("instrument_market") == 10)
        .filter(pc.field("security_category") == 11)
        .filter(pc.field("refdate") == max_date)
        .scanner(columns=["instrument_asset"])
        .to_table()
        .to_pandas()
    )
    return list(df.instrument_asset.unique())


def _get_companies_cvm_codes() -> list[int]:
    df = (
        get_dataset("cvm-companies-registration", layer="input")
        .filter(pc.field("code_cvm") != 0)
        .scanner(columns=["code_cvm", "refdate"])
        .to_table()
        .to_pandas()
    )
    df = df.groupby(["code_cvm"], sort=True).last().reset_index()
    return [int(i) for i in df.code_cvm.unique()]


def _get_indexes_names() -> list[str]:
    tb = (
        get_dataset("b3-indexes-composition", layer="staging")
        .scanner(columns=["refdate"])
        .to_table()
    )
    max_date = pyarrow.compute.max(tb.column("refdate"))
    df = (
        get_dataset("b3-indexes-composition", layer="staging")
        .filter(pc.field("refdate") == max_date)
        .scanner(columns=["indexes"])
        .to_table()
        .to_pandas()
    )
    return list(df.indexes.unique())


def _get_symbols_by_index(index, end_month=None) -> list[str]:
    ds = get_dataset("b3-indexes-composition")
    if end_month is not None:
        ds = ds.filter(pc.field("end_month") == end_month)
    tb = ds.scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    df = (
        get_dataset("b3-indexes-composition")
        .filter(pc.field("refdate") == max_date)
        .filter(pc.field("indexes") == index)
        .scanner(columns=["code"])
        .to_table()
        .to_pandas()
    )
    return list(df.code.unique())


def _get_funds_symbols(type: str) -> list[str]:
    tb = get_dataset("b3-listed-funds").scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    symbols = (
        get_dataset("b3-listed-funds")
        .filter(pc.field("refdate") == max_date)
        .filter(pc.field("fund_type") == type)
        .scanner(columns=["symbol"])
        .to_table()
        .to_pandas()
        .iloc[:, 0]
    )
    return list(symbols)


def get_symbols(type: str, **kwargs) -> list:
    type = type.lower()

    type_handlers = {
        "etf": lambda: _get_funds_symbols("ETF"),
        "fii": lambda: _get_funds_symbols("FII"),
        "fixed-income-etf": lambda: _get_funds_symbols("Fixed Income ETF"),
        "fietf": lambda: _get_funds_symbols("Fixed Income ETF"),
        "index": _get_indexes_names,
        "company": _get_companies_names,
        "company-cvm-code": _get_companies_cvm_codes,
        "company-trading-name": _get_companies_trading_names,
        "industry-sector": lambda: _get_companies_industry_sectors("sector"),
        "industry-subsector": lambda: _get_companies_industry_sectors("subsector"),
        "industry-segment": lambda: _get_companies_industry_sectors("segment"),
    }

    if type in type_handlers:
        return type_handlers[type]()

    if type == "equity":
        if "sector" in kwargs:
            return _get_equity_symbols(kwargs["sector"])
        if "index" in kwargs:
            end_month = kwargs.get("end_month")
            return (
                _get_symbols_by_index(kwargs["index"], end_month)
                if end_month
                else _get_symbols_by_index(kwargs["index"])
            )
        return _get_equity_symbols()

    return []

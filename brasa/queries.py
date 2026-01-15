import json
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow
import pyarrow.compute as pc
import pyarrow.dataset as ds
from bizdays import Calendar, get_option, set_option

from .engine import CacheManager, retrieve_template
from .fieldset_schema import PyArrowAdapter

__all__ = [
    "BrasaDB",
    "describe",
    "get_dataset",
    "get_industry_sectors",
    "get_prices",
    "get_returns",
    "get_symbols",
    "get_template_dataset",
    "get_template_layer",
    "get_template_partitioning",
    "get_template_schema",
    "show",
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
        man = CacheManager()
        con = cls.get_connection()
        con.read_parquet(man.db_path(f"{template}/*.parquet")).create_view(template)

    @classmethod
    def create_views(cls) -> None:
        man = CacheManager()
        for p in Path(man.db_path("")).iterdir():
            try:
                p.name.index(".")
            except ValueError:
                cls.create_view(p.name)
                cls.create_view(p)


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


def get_dataset(
    name: str,
    schema: pyarrow.Schema | None = None,
    partitioning: ds.Partitioning | list[str] | None = None,
    use_template_schema: bool = True,
    layer: str | None = None,
) -> ds.Dataset:
    """Load a dataset by name.

    The name can be either a template ID or a dataset name. When use_template_schema
    is True, the function attempts to load the template to get schema, partitioning,
    layer, and the actual dataset name (which may differ from template ID).

    Args:
        name: The template ID or dataset name.
        schema: Optional PyArrow schema to use. If None and use_template_schema is True,
            attempts to load schema from the template definition.
        partitioning: Optional partitioning specification. Can be a list of column names
            or a PyArrow Partitioning object. If None and use_template_schema is True,
            attempts to load from the template.
        use_template_schema: If True (default), automatically loads the schema and
            partitioning from the template when not provided.
        layer: Optional data layer override. If None and use_template_schema is True,
            attempts to load from the template. If layer is provided, it takes precedence.

    Returns:
        PyArrow Dataset.
    """
    man = CacheManager()
    dataset_name = name

    if use_template_schema:
        if schema is None:
            schema = get_template_schema(name)
        if partitioning is None:
            partitioning = get_template_partitioning(name, schema)
        if layer is None:
            layer = get_template_layer(name)
        # Get the actual dataset name from template (may differ from template ID)
        template_dataset = get_template_dataset(name)
        if template_dataset:
            dataset_name = template_dataset

    # Build path with layer if available
    dataset_path = f"{layer}/{dataset_name}" if layer else dataset_name

    return ds.dataset(
        man.db_path(dataset_path),
        schema=schema,
        format="parquet",
        partitioning=partitioning,
    )


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
        get_dataset("b3-companies-info")
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


from datetime import datetime
import pandas as pd
import pyarrow
import pyarrow.dataset as ds
import pyarrow.compute as pc
from bizdays import Calendar, set_option, get_option

from .engine import CacheManager

__all__ = [
    "get_returns", "get_dataset", "write_dataset", "get_symbols"
]


# class BrasaDB:
#     connection: duckdb.DuckDBPyConnection | None = None

#     @classmethod
#     def path(cls) -> str:
#         man = CacheManager()
#         return man.cache_path(man.db_filename)

#     @classmethod
#     def get_connection(cls) -> duckdb.DuckDBPyConnection:
#         if cls.connection is None:
#             cls.connection = duckdb.connect(database=cls.path(), read_only=True)
#         else:
#             try:
#                 cls.connection.sql("SELECT 42")
#             except duckdb.ConnectionException:
#                 cls.connection.close()
#                 cls.connection = duckdb.connect(database=cls.path(), read_only=True)
#         return cls.connection


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


def get_returns(symbols: str|list[str], start=None, end=None, calendar="B3") -> pd.DataFrame:
    if isinstance(symbols, str):
        symbols = [symbols]
    if start is None:
        start = datetime(2000, 1, 1)
    if end is None:
        end = datetime.today()
    df = get_dataset("brasa-returns")\
            .filter(pc.field("symbol").isin(symbols))\
            .filter(pc.field("refdate") >= start)\
            .filter(pc.field("refdate") <= end)\
            .to_table()\
            .sort_by("refdate")\
            .to_pandas()
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


def get_dataset(name: str) -> ds.Dataset:
    man = CacheManager()
    return ds.dataset(man.db_path(name), format="parquet")


def write_dataset(df: pd.DataFrame, name: str, format: str = "parquet") -> None:
    man = CacheManager()
    tb = pyarrow.Table.from_pandas(df)
    ds.write_dataset(tb, man.db_path(name), format=format, existing_data_behavior="overwrite_or_ignore")


def _get_equity_symbols(sector=None) -> list:
    df = get_dataset("b3-equity-symbols-properties")\
        .scanner(columns=["symbol", "sector"])\
        .to_table().to_pandas()
    if sector is not None:
        df = df[df.sector == sector]
    return list(df.symbol.unique())


def _get_companies_sectors() -> list:
    df = get_dataset("b3-companies-properties")\
        .scanner(columns=["sector"])\
        .to_table().to_pandas()
    return list(df.sector.unique())


def _get_companies_trading_names() -> list:
    tb = get_dataset("b3-company-details").scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    df = get_dataset("b3-company-details")\
        .filter(pc.field("refdate") == max_date)\
        .scanner(columns=["tradingName"])\
        .to_table().to_pandas()
    return list(df.tradingName.unique())


def _get_companies_names() -> list:
    tb = get_dataset("b3-equities-register").scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    df = get_dataset("b3-equities-register")\
        .filter(pc.field("instrument_market") == 10)\
        .filter(pc.field("security_category") == 11)\
        .filter(pc.field("refdate") == max_date)\
        .scanner(columns=["instrument_asset"])\
        .to_table().to_pandas()
    return list(df.instrument_asset.unique())


def _get_companies_cvm_codes() -> list:
    tb = get_dataset("b3-company-info-report").scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    df = get_dataset("b3-company-info-report")\
        .filter(pc.field("refdate") == max_date)\
        .filter(pc.field("codeCVM") != "0")\
        .scanner(columns=["codeCVM"])\
        .to_table().to_pandas()
    return list(df.codeCVM.unique())


def _get_indexes_names() -> list:
    tb = get_dataset("b3-indexes-composition").scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    df = get_dataset("b3-indexes-composition")\
        .filter(pc.field("refdate") == max_date)\
        .scanner(columns=["indexes"])\
        .to_table().to_pandas()
    return list(df.indexes.unique())


def _get_symbols_by_index(index) -> list:
    tb = get_dataset("b3-indexes-composition").scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    df = get_dataset("b3-indexes-composition")\
        .filter(pc.field("refdate") == max_date)\
        .filter(pc.field("indexes") == index)\
        .scanner(columns=["code"])\
        .to_table().to_pandas()
    return list(df.code.unique())


def _get_funds_symbols(type: str) -> list:
    tb = get_dataset("b3-listed-funds").scanner(columns=["refdate"]).to_table()
    max_date = pyarrow.compute.max(tb.column("refdate"))
    symbols = get_dataset("b3-listed-funds")\
        .filter(pc.field("refdate") == max_date)\
        .filter(pc.field("fund_type") == type)\
        .scanner(columns=["symbol"])\
        .to_table().to_pandas().iloc[:,0]
    return list(symbols)


def get_symbols(type: str, **kwargs) -> list:
    type = type.lower()
    if type == "etf":
        return _get_funds_symbols("ETF")
    elif type == "fii":
        return _get_funds_symbols("FII")
    elif type == "fixed-income-etf" or type == "fietf":
        return _get_funds_symbols("Fixed Income ETF")
    elif type == "index":
        return _get_indexes_names()
    elif type == "company":
        return _get_companies_names()
    elif type == "company-cvm-code":
        return _get_companies_cvm_codes()
    elif type == "company-trading-name":
        return _get_companies_trading_names()
    elif type == "sector":
        return _get_companies_sectors()
    elif type == "equity":
        if "sector" in kwargs:
            return _get_equity_symbols(kwargs["sector"])
        elif "index" in kwargs:
            return _get_symbols_by_index(kwargs["index"])
        else:
            return _get_equity_symbols()
    else:
        return []

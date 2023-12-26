
from datetime import datetime
import pandas as pd
import pyarrow
import pyarrow.dataset as ds
import pyarrow.compute as pc
from bizdays import Calendar, set_option, get_option

from .engine import CacheManager

__all__ = ["get_returns", "get_dataset", "write_dataset"]


set_option("mode", "pandas")

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
    cal = Calendar.load(calendar)
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
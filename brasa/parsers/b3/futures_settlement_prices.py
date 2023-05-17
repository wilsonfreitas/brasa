
from datetime import datetime
from typing import IO
import numpy as np

import pandas as pd
from lxml import etree
from bizdays import Calendar
from brasa.engine import CacheManager
from brasa.engine import CacheMetadata

from brasa.engine import MarketDataReader, retrieve_template


def maturity2date_newcode(x: str, cal: Calendar, expr: str) -> datetime:
    """Converts a maturity code to a date.
    
    The new code is a single letter, as in "F" for January.
    This code started to be used in 2007.
    """
    year = int(x[-1:]) + 2000
    month = code2month_newcode(x[0])
    return cal.getdate(expr, year, month)


def maturity2date_oldcode(x: str, cal: Calendar, expr: str) -> datetime:
    """Converts a maturity code to a date.
    
    The old code is a three-letter code, as in "JAN" for January.
    This code was used until 2007.
    """
    year = int(x[-1:]) + 2000
    month = code2month_oldcode(x[:3])
    return cal.getdate(expr, year, month)


def maturity2date(x: str, cal: Calendar, expr: str="first day") -> datetime:
    maturity_code = x[-3:]
    if len(maturity_code) == 3:
        return maturity2date_newcode(maturity_code, cal, expr)
    else:
        return maturity2date_oldcode(maturity_code, cal, expr)


def code2month(code: str) -> int:
    """Converts a month code to a month number.
    
    The code can be a single letter, as in "F" for January,
    or a three-letter code, as in "JAN" for January.
    """
    if len(code) == 1:
        return code2month_newcode(code)
    else:
        return code2month_oldcode(code)


def code2month_newcode(code: str) -> int:
    """Converts a month code to a month number.
    
    The new code is a single letter, as in "F" for January.
    This code started to be used in 2007.
    """
    month_codes = "FGHJKMNQUVXZ"
    return month_codes.index(code) + 1


def code2month_oldcode(code: str) -> int:
    """Converts a month code to a month number.

    The old code is a three-letter code, as in "JAN" for January.
    This code was used until 2007.
    """
    month_codes = ["JAN", "FEV", "MAR", "ABR", "MAI", "JUN", "JUL", "AGO","SET", "OUT", "NOV", "DEZ"]
    return month_codes.index(code) + 1


def future_settlement_prices_parser(fname: IO | str) -> pd.DataFrame:
    df = pd.read_html(fname,
                      attrs=dict(id="tblDadosAjustes"),
                      decimal=",",
                      thousands=".",)[0]
    df.columns = ["commodity", "maturity_code", "previous_settlement_price", "settlement_price", "price_variation", "settlement_value"]
    tree = etree.parse(fname, etree.HTMLParser())
    refdate_str = tree.xpath(f"//input[@id='dData1']")[0].attrib["value"]
    df["refdate"] = pd.to_datetime(refdate_str, format="%d/%m/%Y")
    for ix in range(df.shape[0]):
        if df.loc[ix, "commodity"] is not np.nan:
            last_name = df.loc[ix, "commodity"]
        df.loc[ix, "commodity"] = last_name
    df.loc[:, "commodity"] = df.loc[:, "commodity"].str.extract(r"^(\w+)")[0]
    df["symbol"] = df["commodity"] + df["maturity_code"]
    return df


def read_b3_futures_settlement_prices(reader: MarketDataReader, meta: CacheMetadata, **kwargs) -> pd.DataFrame:
    fname = meta.downloaded_file_paths[0]
    df = future_settlement_prices_parser(fname)
    return df

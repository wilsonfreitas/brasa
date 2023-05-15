
from datetime import datetime
import os
from typing import IO
import numpy as np

import pandas as pd
from lxml import etree

from brasa.templates import download_marketdata, read_marketdata, retrieve_template


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


def future_prices(fname: IO | str) -> pd.DataFrame:
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


def futures_get(refdate: datetime):
    tpl = retrieve_template("AjustesDiarios")
    args = dict(refdate=refdate)
    # how do I know that the refdate has already been downloaded?
    meta = download_marketdata(tpl, **args)
    fname = os.path.join(meta["folder"], meta["downloaded_files"][0])
    return read_marketdata(tpl, fname, True)
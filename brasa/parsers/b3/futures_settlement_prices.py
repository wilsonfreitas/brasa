
from datetime import datetime
import os
from typing import IO
import numpy as np

import pandas as pd
from lxml import etree
from bizdays import Calendar
import yaml

from brasa.templates import MarketDataTemplate, download_marketdata, read_marketdata, retrieve_template
from brasa.util import generate_hash


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


class BrasaCacheManager:
    def __init__(self, template: MarketDataTemplate, args: dict) -> None:
        self.template = template
        self.args = args
        self.cache_folder = os.path.join(os.getcwd(), ".brasa-cache")
        os.makedirs(self.cache_folder, exist_ok=True)
        self.meta_folder = os.path.join(self.cache_folder, "meta")
        os.makedirs(self.meta_folder, exist_ok=True)
        self.db_folder = os.path.join(self.cache_folder, "db", template.id)
        os.makedirs(self.db_folder, exist_ok=True)

        hash = generate_hash(template.id, args)
        self.meta_file_path = os.path.join(self.meta_folder, f"{hash}.yaml")

    def parquet_file_path(self, refdate: datetime) -> str:
        return os.path.join(self.db_folder, f"{refdate.isoformat()[:10]}.parquet")

    def exists(self, refdate: datetime) -> bool:
        return self.has_meta and os.path.isfile(self.parquet_file_path(refdate))

    @property
    def has_meta(self) -> bool:
        return os.path.isfile(self.meta_file_path)

    def save_meta(self, meta: dict) -> None:
        with open(self.meta_file_path, "w") as fp:
            yaml.dump(meta, fp, indent=4)

    def load_meta(self) -> dict:
        with open(self.meta_file_path, "r") as fp:
            meta = yaml.load(fp, Loader=yaml.Loader)
        return meta

    def save_parquet(self, df: pd.DataFrame, refdate: datetime) -> None:
        df.to_parquet(self.parquet_file_path(refdate))

    def load_parquet(self, refdate: datetime) -> pd.DataFrame:
        df = pd.read_parquet(self.parquet_file_path(refdate))
        return df
    
    def process_with_checks(self, refdate: datetime) -> pd.DataFrame:
        if self.exists(refdate):
            df = self.load_parquet(refdate)
        else:
            if self.has_meta:
                meta = self.load_meta()
            else:
                meta = download_marketdata(self.template, **self.args)
            df = read_marketdata(self.template, meta)
            self.save_parquet(df, refdate)
            if not self.has_meta:
                self.save_meta(meta)
        return df

    def process_without_checks(self, refdate: datetime) -> pd.DataFrame:
        meta = download_marketdata(self.template, **self.args)
        df = read_marketdata(self.template, meta)
        self.save_parquet(df, refdate)
        self.save_meta(meta)
        return df


def futures_settlement_prices_get(refdate: datetime):
    tpl = retrieve_template("b3-futures-settlement-prices")
    args = dict(refdate=refdate)
    cache = BrasaCacheManager(tpl, args)
    return cache.process_with_checks(refdate)
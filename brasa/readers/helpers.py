import json
from typing import IO
import numpy as np

import pandas as pd
from lxml import etree

from brasa.templates import MarketDataReader
from brasa.parsers.b3.futures_settlement_prices import future_settlement_prices_parser


def read_json(reader: MarketDataReader, fname: IO | str) -> pd.DataFrame:
    if isinstance(fname, str):
        with open(fname, "r", encoding=reader.encoding) as f:
            data = json.load(f)
    else:
        data = json.load(fname)
    return pd.DataFrame(data, index=[0], columns=reader.fields.names)


def read_csv(reader: MarketDataReader, fname: IO | str) -> pd.DataFrame:
    converters = {n:str for n in reader.fields.names}
    return pd.read_csv(fname,
                       encoding=reader.encoding,
                       header=None,
                       skiprows=reader.skip,
                       sep=reader.separator,
                       converters=converters,
                       names=reader.fields.names,)


def read_settlement_prices(reader: MarketDataReader, fname: IO | str) -> pd.DataFrame:
    df = future_settlement_prices_parser(fname)
    return df

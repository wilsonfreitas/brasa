import json
import os
from typing import IO
import numpy as np

import pandas as pd
from lxml import etree
from brasa.meta import CacheMetadata
from brasa.parsers.b3.bvbg028 import BVBG028Parser

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


def read_b3_futures_settlement_prices(reader: MarketDataReader, meta: CacheMetadata, **kwargs) -> pd.DataFrame:
    fname = meta.downloaded_file_paths[0]
    df = future_settlement_prices_parser(fname)
    return df


def read_b3_bvbg028(reader: MarketDataReader, meta: CacheMetadata, **kwargs) -> pd.DataFrame:
    paths = meta.downloaded_file_paths
    paths.sort()
    fname = paths[-1]
    parser = BVBG028Parser(fname)
    instrs = [x for x in parser.instruments if x["instrument_type"] == kwargs["instrument_type"]]
    return pd.DataFrame(instrs)
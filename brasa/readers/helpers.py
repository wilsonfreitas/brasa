import json
from typing import IO

import pandas as pd

from brasa.engine import CacheManager, CacheMetadata, MarketDataReader
from brasa.parsers.b3.bvbg028 import BVBG028Parser
from brasa.parsers.b3.bvbg086 import BVBG086Parser
from brasa.parsers.b3.cdi import CDIParser
from brasa.parsers.b3.cotahist import COTAHISTParser
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


def read_b3_cotahist(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    parser = COTAHISTParser(man.cache_path(fname))
    return parser._data._tables["data"]


def read_b3_bvbg028(meta: CacheMetadata) -> dict[str, pd.DataFrame]:
    paths = meta.downloaded_files
    paths.sort()
    fname = paths[-1]
    man = CacheManager()
    parser = BVBG028Parser(man.cache_path(fname))
    return parser.data


def read_b3_bvbg086(meta: CacheMetadata) -> pd.DataFrame:
    paths = meta.downloaded_files
    paths.sort()
    fname = paths[-1]
    man = CacheManager()
    parser = BVBG086Parser(man.cache_path(fname))
    return parser.data


def read_b3_cdi(meta: CacheMetadata) -> pd.DataFrame:
    man = CacheManager()
    parser = CDIParser(man.cache_path(meta.downloaded_files[0]))
    return parser.data


def read_b3_futures_settlement_prices(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    df = future_settlement_prices_parser(man.cache_path(fname))
    return df
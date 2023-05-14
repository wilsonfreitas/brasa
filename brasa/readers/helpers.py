import json
from typing import IO

import pandas as pd

from brasa.templates import MarketDataReader


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
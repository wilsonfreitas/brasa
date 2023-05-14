import json
from typing import IO

import pandas as pd


def read_json(fname: IO | str, encoding: str="utf-8") -> pd.DataFrame:
    if isinstance(fname, str):
        with open(fname, "r", encoding=encoding) as f:
            data = json.load(f)
    else:
        data = json.load(fname)
    return pd.DataFrame(data, index=[0])
import gzip
import json

import pandas as pd

from ..engine import CacheManager, CacheMetadata, retrieve_template


def read_b3_indexes_composition(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    with gzip.open(fname) as f:
        obj = json.load(f)

    df = pd.DataFrame(obj["results"])
    df_stock_indexes = (
        df.groupby(["company", "spotlight", "code"])
        .apply(lambda x: x.indexes.str.split(",").explode())
        .reset_index()
    )
    header = obj["header"]
    df_stock_indexes["start_month"] = (
        f"{header['year']}-{str.zfill(str(header['startMonth']), 2)}"
    )
    df_stock_indexes["end_month"] = (
        f"{header['year']}-{str.zfill(str(header['endMonth']), 2)}"
    )
    df_stock_indexes["index_update_date"] = pd.to_datetime(header["update"])
    df_stock_indexes["refdate"] = pd.to_datetime(meta.timestamp.date())

    return df_stock_indexes


def read_bcb_sgs_data(meta: CacheMetadata) -> pd.DataFrame:
    fname = meta.downloaded_files[0]
    man = CacheManager()
    fname = man.cache_path(fname)
    df = pd.read_json(fname, dtype_backend="pyarrow")
    df["code"] = meta.download_args["code"]
    template = retrieve_template(meta.template)
    df.columns = template.fields.names
    df["refdate"] = pd.to_datetime(df["refdate"], format="%d/%m/%Y", errors="coerce")
    return df

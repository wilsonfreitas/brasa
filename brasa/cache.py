import os
from datetime import datetime

import pandas as pd
import yaml

from brasa.api import download_marketdata, read_marketdata
from brasa.templates import MarketDataTemplate
from brasa.util import generate_checksum_for_template


class CacheManager:
    def __init__(self, template: MarketDataTemplate, args: dict) -> None:
        self.template = template
        self.args = args
        self.cache_folder = os.path.join(os.getcwd(), ".brasa-cache")
        os.makedirs(self.cache_folder, exist_ok=True)
        self.meta_folder = os.path.join(self.cache_folder, "meta")
        os.makedirs(self.meta_folder, exist_ok=True)
        self.db_folder = os.path.join(self.cache_folder, "db", template.id)
        os.makedirs(self.db_folder, exist_ok=True)

        hash = generate_checksum_for_template(template.id, args)
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
            df = read_marketdata(self.template, meta, **self.args)
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
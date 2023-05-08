import json
import pandas as pd
from ..util import Parser


class StockIndexInfoParser(Parser):
    def __init__(self, fname):
        self.fname = fname
        self._table = None
        self.parse()

    def parse(self):
        self._data = self._open(self.fname, json.load)

        df = pd.DataFrame(self._data["results"])

        def _(dfx):
            indexes = dfx["indexes"].str.split(",").explode()
            return pd.DataFrame(
                {
                    "company": dfx["company"],
                    "spotlight": dfx["spotlight"],
                    "code": dfx["code"],
                    "indexes": indexes,
                }
            )

        dfr = (
            df.groupby(["company", "spotlight", "code"]
                       ).apply(_).reset_index(drop=True)
        )

        dfr["refdate"] = self._data["header"]["update"]
        dfr["duration_start_month"] = self._data["header"]["startMonth"]
        dfr["duration_end_month"] = self._data["header"]["endMonth"]
        dfr["duration_year"] = self._data["header"]["year"]

        dfr = dfr.rename(
            columns={
                "company": "corporation_name",
                "spotlight": "specification_code",
                "code": "symbol",
            }
        )

        self._table = dfr

    @property
    def data(self):
        return self._table

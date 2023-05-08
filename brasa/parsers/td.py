from datetime import datetime
import pandas as pd
from collections import namedtuple
from .util import Parser


class TesouroDiretoHistoricalDataParser(Parser):
    mode = "rb"
    encoding = None

    def __init__(self, fname, date_format="%d/%m/%Y", sheet_date_format="%d%m%y"):
        excel_file = self._open(fname, pd.ExcelFile)
        self.date_format = date_format
        self.sheet_date_format = sheet_date_format
        self._parse(excel_file)

    def _parse(self, excel_file):
        sheets = excel_file.sheet_names
        contracts = [self._parse_sheet_name(name) for name in sheets]
        data = []
        for contract in contracts:
            df = excel_file.parse(skiprows=1, sheet_name=contract.sheet_name)
            df.columns = [
                "refdate",
                "bid_rate",
                "ask_rate",
                "bid_price",
                "ask_price",
                "base_price",
            ]
            df["refdate"] = pd.to_datetime(df["refdate"], format="%d/%m/%Y")
            df["symbol"] = contract.name
            df["maturity"] = contract.maturity_date
            data.append(df)

        self.__data = pd.concat(data)

    def _parse_sheet_name(self, sheet_name):
        sheet_name_split = sheet_name.split()
        contract = namedtuple("Contract", "sheet_name name maturity_date")
        return contract(
            sheet_name,
            sheet_name_split[0],
            datetime.strptime(sheet_name_split[1], self.sheet_date_format),
        )

    @property
    def data(self):
        return self.__data

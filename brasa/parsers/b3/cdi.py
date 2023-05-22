import json
import pandas as pd

from brasa.engine import CacheManager, CacheMetadata, MarketDataReader
from ..util import PortugueseRulesParser2, Parser


class CDIParser(Parser):
    def __init__(self, fname):
        self.fname = fname
        self.parse()

    def parse(self):
        text_parser = PortugueseRulesParser2()
        _data = self._open(self.fname, json.load)
        cdi_data = {
            "refdate": text_parser.parse(_data["dataTaxa"]),
            "value": text_parser.parse(_data["taxa"]),
            "symbol": "CDI",
        }
        idi_data = {
            "refdate": text_parser.parse(_data["dataIndice"]),
            "value": text_parser.parse(_data["indice"]),
            "symbol": "IDI",
        }
        self._info = [cdi_data, idi_data]
        self._data = {
            "cdi": pd.DataFrame(cdi_data, index=[0]),
            "idi": pd.DataFrame(idi_data, index=[0]),
        }

    @property
    def data(self):
        return self._data
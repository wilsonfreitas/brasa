import io
from itertools import dropwhile
from lxml import etree
from ..util import PortugueseRulesParser2, Parser

from kyd.readers.csv import CSVFile, Field, DateField, NumericField


class TPFFile(CSVFile):
    encoding = "latin1"
    separator = "@"
    symbol = Field()
    refdate = DateField("%Y%m%d")
    cod_selic = Field()
    issue_date = DateField("%Y%m%d")
    maturity_date = DateField("%Y%m%d")
    bid_yield = NumericField()
    ask_yield = Field()
    ref_yield = Field()
    price = Field()


class TPFParser(Parser):
    encoding = "latin1"

    def __init__(self, fname):
        self.fname = fname
        self.instruments = []
        self.pp = PortugueseRulesParser2()
        self.parse()

    def parse(self):
        self._open(self.fname, self._parse)

    def _parse(self, fp):
        _drop_first_n = dropwhile(lambda x: x[0] < 3, enumerate(fp))
        _drop_empy = filter(lambda x: x[1].strip() != "", _drop_first_n)
        for _, line in _drop_empy:
            row = line.split("@")
            tit = dict(
                symbol=row[0],
                refdate=self.pp.parse(row[1]),
                cod_selic=row[2],
                issue_date=self.pp.parse(row[3]),
                maturity_date=self.pp.parse(row[4]),
                bid_yield=self.pp.parse(row[5]),
                ask_yield=self.pp.parse(row[6]),
                ref_yield=self.pp.parse(row[7]),
                price=self.pp.parse(row[8]),
            )
            self.instruments.append(tit)

    @property
    def data(self):
        return self.instruments


def get_all_node_text(node):
    return "".join(x.strip() for x in node.itertext())


class VnaTPFParser(Parser):
    encoding = "latin1"

    def __init__(self, fname):
        self.fname = fname
        self.pp = PortugueseRulesParser2()
        self._data = []
        self.parse()

    def parse(self):
        parser = etree.HTMLParser()
        tree = self._open(self.fname, lambda x: etree.parse(x, parser))
        self._data.append(self._parse_vna_node(tree, "listaNTN-B"))
        self._data.append(self._parse_vna_node(tree, "listaNTN-C"))
        self._data.append(self._parse_vna_node(tree, "listaLFT"))

    def _parse_vna_node(self, tree, id):
        trs = tree.xpath(f"//div[@id='{id}']/*/table/*/tr")
        if len(trs) == 0:
            return {}
        instrument_ref = get_all_node_text(trs[0])
        date = get_all_node_text(trs[1][1])
        index_ref = get_all_node_text(trs[2][1])
        value = get_all_node_text(trs[3][1])
        rate_value = get_all_node_text(trs[3][2])
        try:
            projection = get_all_node_text(trs[3][3]) == "P"
            rate_date = get_all_node_text(trs[3][4])
        except IndexError:
            projection = False
            rate_date = ""
        return {
            "refdate": self.pp.parse(date),
            "value": self.pp.parse(value),
            "rate": self.pp.parse(rate_value),
            "rate_start_date": self.pp.parse(rate_date),
            "proj": projection,
            "index_ref": index_ref,
            "instrument_ref": instrument_ref,
        }

    @property
    def data(self):
        return [x for x in self._data if x]

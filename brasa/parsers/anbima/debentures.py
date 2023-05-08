import io
from itertools import dropwhile
import pandas as pd
from ..util import PortugueseRulesParser2, Parser


class DebenturesParser(Parser):
    encoding = "latin1"

    def __init__(self, fname):
        self.fname = fname
        self.instruments = []
        self.pp = PortugueseRulesParser2()
        self.parse()

    def parse(self):
        self._open(self.fname, self._parse)

    def _parse(self, fp):
        # 0 Código@
        # 1 Nome@
        # 2 Repac./  Venc.@
        # 3 Índice/ Correção@
        # 4 Taxa de Compra@
        # 5 Taxa de Venda@
        # 6 Taxa Indicativa@
        # 7 Desvio Padrão@
        # 8 Intervalo Indicativo Minimo@
        # 9 Intervalo Indicativo Máximo@
        # 10 PU@
        # 11 % PU Par@
        # 12 Duration@
        # 13 % Reune@
        # 14 Referência NTN-B
        _drop_first_n = dropwhile(lambda x: x[0] < 3, enumerate(fp))
        _drop_empy = filter(lambda x: x[1].strip() != "", _drop_first_n)
        for _, line in _drop_empy:
            row = line.strip().split("@")
            tit = dict(
                symbol=row[0],
                name=row[1],
                maturity_date=self.pp.parse(row[2]),
                underlying=row[3],
                bid_yield=self.pp.parse(row[4]),
                ask_yield=self.pp.parse(row[5]),
                ref_yield=self.pp.parse(row[6]),
                price=self.pp.parse(row[10]),
                perc_price_par=self.pp.parse(row[11]),
                duration=self.pp.parse(row[12]),
                perc_reune=self.pp.parse(row[13]),
                ref_ntnb=self.pp.parse(row[14]),
            )
            self.instruments.append(tit)
        self._data = pd.DataFrame(self.instruments)

    @property
    def data(self):
        return self.instruments

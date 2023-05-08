
from . import GenericParser, float_or_none
from .util import Parser, PortugueseRulesParser2

class InformesDiariosParser(Parser):
    encoding = "latin1"

    def __init__(self, fname):
        self.fname = fname
        self.funds = []
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


def handle_row(row, names, parser=lambda x: x):
    fields = [parser(val.strip()) for val in row.split(';')]
    fields = [None if val == '' else val for val in fields]
    row = dict(zip(names, fields))
    cnpj_fundo_str_num = row['cnpj_fundo'].replace('.', '')\
        .replace('/', '').replace('-', '')
    return (cnpj_fundo_str_num, row)


def handle_informes_diarios(row):
    _names = 'CNPJ_FUNDO;DT_COMPTC;VL_TOTAL;VL_QUOTA;VL_PATRIM_LIQ;CAPTC_DIA;RESG_DIA;NR_COTST'
    names = _names.lower().split(';')
    parser = GenericParser()
    return handle_row(row, names, parser.parse)


def handle_info_cadastral(row):
    _names = 'CNPJ_FUNDO;DENOM_SOCIAL;DT_REG;DT_CONST;DT_CANCEL;SIT;DT_INI_SIT;DT_INI_ATIV;DT_INI_EXERC;DT_FIM_EXERC;CLASSE;DT_INI_CLASSE;RENTAB_FUNDO;CONDOM;FUNDO_COTAS;FUNDO_EXCLUSIVO;TRIB_LPRAZO;INVEST_QUALIF;TAXA_PERFM;INF_TAXA_PERFM;TAXA_ADM;INF_TAXA_ADM;VL_PATRIM_LIQ;DT_PATRIM_LIQ;DIRETOR;CNPJ_ADMIN;ADMIN;PF_PJ_GESTOR;CPF_CNPJ_GESTOR;GESTOR;CNPJ_AUDITOR;AUDITOR;CNPJ_CUSTODIANTE;CUSTODIANTE;CNPJ_CONTROLADOR;CONTROLADOR'
    names = _names.lower().split(';')
    vals = handle_row(row, names)
    vals[1]['taxa_perfm'] = float_or_none(vals[1]['taxa_perfm'])
    vals[1]['taxa_adm'] = float_or_none(vals[1]['taxa_adm'])
    vals[1]['vl_patrim_liq'] = float_or_none(vals[1]['vl_patrim_liq'])
    return vals

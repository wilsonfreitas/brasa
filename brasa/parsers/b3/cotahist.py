from ...readers.fwf import FWFFile, FWFRow, Field, DateField, NumericField


class COTAHIST_header(FWFRow):
    _pattern = r"^00"
    tipo_registro = Field(2)
    nome_arquivo = Field(13)
    cod_origem = Field(8)
    data_geracao_arquivo = DateField(8, "%Y%m%d")
    reserva = Field(214)


class COTAHIST_trailer(FWFRow):
    _pattern = r"^99"
    tipo_mercado = Field(2)
    nome_arquivo = Field(13)
    cod_origem = Field(8)
    data_geracao_arquivo = DateField(8, "%Y%m%d")
    num_registros = Field(11)
    reserva = Field(203)


class COTAHIST_histdata(FWFRow):
    _pattern = "^01"
    tipo_registro = Field(2)
    data_referencia = DateField(8, "%Y%m%d")
    cod_bdi = Field(2)
    cod_negociacao = Field(12)
    tipo_mercado = Field(3)
    nome_empresa = Field(12)
    especificacao = Field(10)
    num_dias_mercado_termo = Field(3)
    cod_moeda = Field(4)
    preco_abertura = NumericField(13, dec=2)
    preco_max = NumericField(13, dec=2)
    preco_min = NumericField(13, dec=2)
    preco_med = NumericField(13, dec=2)
    preco_ult = NumericField(13, dec=2)
    preco_melhor_oferta_compra = NumericField(13, dec=2)
    preco_melhor_oferta_venda = NumericField(13, dec=2)
    qtd_negocios = NumericField(5)
    qtd_titulos_negociados = NumericField(18)
    volume_titulos_negociados = NumericField(18, dec=2)
    preco_exercicio = NumericField(13, dec=2)
    indicador_correcao_preco_exercicio = Field(1)
    data_vencimento = DateField(8, "%Y%m%d")
    fator_cot = NumericField(7, dec=2)
    preco_exercicio_pontos = NumericField(13, dec=6)
    cod_isin = Field(12)
    num_dist = Field(3)


class COTAHIST_file(FWFFile):
    header = COTAHIST_header()
    trailer = COTAHIST_trailer()
    data = COTAHIST_histdata()


class COTAHISTParser:
    encoding = "latin1"

    def __init__(self, fname):
        self.fname = fname
        self._data = None
        self.parse()

    def parse(self):
        self._data = COTAHIST_file(self.fname, encoding=self.encoding)

    @property
    def data(self):
        return self._data.data

    @property
    def header(self):
        return self._data.header

    @property
    def trailer(self):
        return self._data.trailer

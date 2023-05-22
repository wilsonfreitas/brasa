from ..fwf import FWFFile, FWFRow, Field, DateField, NumericField


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
    regtype = Field(2)
    refdate = DateField(8, "%Y%m%d")
    bdi_code = NumericField(2)
    symbol = Field(12)
    instrument_market = NumericField(3)
    corporation_name = Field(12)
    specification_code = Field(10)
    days_to_settlement = NumericField(3)
    trading_currency = Field(4)
    open = NumericField(13, dec=2)
    high = NumericField(13, dec=2)
    low = NumericField(13, dec=2)
    average = NumericField(13, dec=2)
    close = NumericField(13, dec=2)
    best_bid = NumericField(13, dec=2)
    best_ask = NumericField(13, dec=2)
    trade_quantity = NumericField(5)
    traded_contracts = NumericField(18)
    volume = NumericField(18, dec=2)
    strike_price = NumericField(13, dec=2)
    strike_price_adjustment_indicator = Field(1)
    maturity_date = DateField(8, "%Y%m%d")
    allocation_lot_size = NumericField(7, dec=2)
    strike_price_in_points = NumericField(13, dec=6)
    isin = Field(12)
    distribution_id = NumericField(3)


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

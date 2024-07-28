from ..fwf import FWFFile, FWFRow, Field, DateField, NumericField


class Indic_data(FWFRow):
    id_transacao = NumericField(6)
    compl_transacao = NumericField(3)
    tipo_registro = NumericField(2)
    data_geracao_arquivo = DateField(8, "%Y%m%d")
    grupo_indicador = Field(2)
    cod_indicador = Field(25)
    valor_indicador = NumericField(25)
    num_casas_decimais = NumericField(2)
    reserva = Field(36)


class IndicParser(FWFFile):
    data = Indic_data()

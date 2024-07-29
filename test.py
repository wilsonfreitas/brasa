# %%

from brasa.parsers.b3 import IndicParser


# %%

indic = IndicParser("Indic.txt")

# %%

indic._tables["data"].valor_indicador / (10 ** indic._tables["data"].num_casas_decimais)

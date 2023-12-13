# %%
# import os
# os.environ["BRASA_DATA_PATH"] = "D:\\brasa"

import sys
sys.path.append('.')

from datetime import datetime

from bcb import sgs
from bcb import PTAX
import pandas as pd

import brasa

# %%
dd = sgs.get({"CDI": 4389}, start=datetime(2000, 1, 1))
dd_cdi = dd.reset_index()
dd_cdi["symbol"] = "CDI"
dd_cdi.columns = ["refdate", "value", "symbol"]

dd = sgs.get({"SELIC": 1178}, start=datetime(2000, 1, 1))
dd_selic = dd.reset_index()
dd_selic["symbol"] = "SELIC"
dd_selic.columns = ["refdate", "value", "symbol"]

dd = sgs.get({"SETA": 432}, start=datetime(2000, 1, 1))
dd_seta = dd.reset_index()
dd_seta["symbol"] = "SETA"
dd_seta.columns = ["refdate", "value", "symbol"]

dd = sgs.get({"IPCA": 433}, start=datetime(1980, 1, 1))
dd_ipca = dd.reset_index()
dd_ipca["symbol"] = "IPCA"
dd_ipca.columns = ["refdate", "value", "symbol"]

dd = sgs.get({"IGPM": 189}, start=datetime(1980, 1, 1))
dd_igpm = dd.reset_index()
dd_igpm["symbol"] = "IGPM"
dd_igpm.columns = ["refdate", "value", "symbol"]

ptax = PTAX()
ep = ptax.get_endpoint('CotacaoMoedaPeriodo')
dd = (ep.query()
   .parameters(moeda='USD', dataInicial='1/1/2000', dataFinalCotacao=datetime.today().strftime("%m/%d/%Y"))
   .filter(ep.tipoBoletim == "Fechamento")
   .select(ep.dataHoraCotacao, ep.cotacaoVenda)
   .collect())
dd_dol = dd
dd_dol["symbol"] = "BRLUSD"
dd_dol.columns = ["value", "refdate", "symbol"]
dd_dol["refdate"] = pd.to_datetime(dd_dol["refdate"])
dd_dol = dd_dol.loc[:, ["refdate", "value", "symbol"]]

# %%
df_bcb = pd.concat([dd_cdi, dd_selic, dd_seta, dd_ipca, dd_igpm, dd_dol], axis=0)
brasa.write_dataset(df_bcb, "bcb-data")

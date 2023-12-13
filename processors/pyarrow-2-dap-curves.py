# %%
# import os
# os.environ["BRASA_DATA_PATH"] = "D:\\brasa"

import sys
sys.path.append('.')

from datetime import datetime

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.compute as pc
import pyarrow
from bizdays import Calendar, set_option

import brasa

man = brasa.engine.CacheManager()

set_option("mode.datetype", "datetime")
set_option("mode", "python")

cal = Calendar.load("ANBIMA")

# %%
tb_dap = brasa.get_dataset("b3-futures-dap").filter(pc.field("business_days") > 0).to_table()

# %%

tb_dap_curve = (tb_dap
                .select(["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"])
                .sort_by([("refdate", "ascending"), ("business_days", "ascending")]))

brasa.write_dataset(tb_dap_curve.to_pandas(), "b3-curves-dap")

# %%
import numpy as np

def interp_ff(term, rates, terms):
    log_pu = np.log((1 + rates)**(terms/252))
    pu = np.exp(np.interp(term, terms, log_pu))
    return pu ** (252 / term) - 1

business_days_standard = np.array([252/2, 252], dtype=np.int32)
symbols_standard = pyarrow.array([f"DAPT{d}" for d in business_days_standard])
tables = []
for date in tb_dap_curve.column("refdate").unique():
    rates = tb_dap_curve.filter(pc.field("refdate") == date).column("adjusted_tax").to_numpy()
    terms = tb_dap_curve.filter(pc.field("refdate") == date).column("business_days").to_numpy()
    interp_rates = pyarrow.array(interp_ff(business_days_standard, rates, terms))
    mat_dates = pyarrow.array(cal.offset(date.as_py(), business_days_standard))
    ta = pyarrow.table([
        pyarrow.array([date.as_py()] * len(interp_rates)),
        symbols_standard,
        mat_dates,
        pyarrow.array(business_days_standard),
        interp_rates
    ], names=["refdate", "symbol", "maturity_date", "business_days", "adjusted_tax"])
    tables.append(ta)

# %%
tb_dap_curve_standard = pyarrow.concat_tables(tables).sort_by([("refdate", "ascending"), ("business_days", "ascending")])
brasa.write_dataset(tb_dap_curve_standard.to_pandas(), "b3-curves-dap-standard")

# %%
tables = []
for symbol in tb_dap_curve_standard.column("symbol").unique():
    rates = tb_dap_curve_standard.filter(pc.field("symbol") == symbol).column("adjusted_tax").to_numpy()
    dates = tb_dap_curve_standard.filter(pc.field("symbol") == symbol).column("refdate")
    symbols = tb_dap_curve_standard.filter(pc.field("symbol") == symbol).column("symbol")
    returns = np.concatenate([np.array([np.nan]), np.diff(rates)])
    ta = pyarrow.table([
        dates,
        symbols,
        pyarrow.array(returns)
    ], names=["refdate", "symbol", "returns"])
    tables.append(ta)

# %%
tb_dap_curve_standard_returns = pyarrow.concat_tables(tables).sort_by([("refdate", "ascending"), ("symbol", "ascending")])
brasa.write_dataset(tb_dap_curve_standard_returns.to_pandas(), "b3-curves-dap-standard-returns")

# %%

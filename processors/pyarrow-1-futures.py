# %%
# import os
# os.environ["BRASA_DATA_PATH"] = "D:\\brasa"

import sys
sys.path.append('.')

import pyarrow.compute as pc
from bizdays import Calendar

from brasa.parsers.b3.futures_settlement_prices import maturity2date
import brasa

man = brasa.CacheManager()

# %%
df = brasa.get_dataset("b3-futures-settlement-prices")

# %%
cal = Calendar.load("ANBIMA")
df_contracts = df.to_table().filter(pc.field("commodity") == 'DI1').to_pandas()
df_contracts['maturity_date'] = df_contracts['maturity_code'].apply(lambda x: maturity2date(x, cal))
df_contracts["business_days"] = cal.bizdays(df_contracts['refdate'], cal.following(df_contracts["maturity_date"]))
df_contracts["adjusted_tax"] = (100000 / df_contracts["settlement_price"]) ** (252 / df_contracts["business_days"]) - 1
df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
df_contracts = df_contracts[["refdate", "symbol", "maturity_date", "settlement_price", "adjusted_tax", "business_days"]]#.query("business_days > 0")

brasa.write_dataset(df_contracts, "b3-futures-di1")

# %%
cal = Calendar.load("ANBIMA")
df_contracts = df.filter(pc.field("commodity") == 'DOL').to_table().to_pandas()
df_contracts['maturity_date'] = df_contracts['maturity_code'].apply(lambda x: maturity2date(x, cal))
df_contracts["business_days"] = cal.bizdays(df_contracts['refdate'], cal.following(df_contracts["maturity_date"]))
df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
df_contracts = df_contracts[["refdate", "symbol", "maturity_date", "settlement_price", "business_days"]]
brasa.write_dataset(df_contracts, "b3-futures-dol")

# %%
first = df_contracts.groupby("refdate").nth(0)
second = df_contracts.groupby("refdate").nth(1)
merged = first.merge(second, on="refdate", how="left")
first_contracts = first.copy().reset_index(drop=True)
second_contracts = second.copy().reset_index(drop=True)
first_contracts.loc[merged["business_days_x"] <= 1, :] = second_contracts.loc[merged["business_days_x"] <= 1, :]
first_contracts["ref"] = first_contracts["symbol"]
first_contracts["symbol"] = "DOLT01"
brasa.write_dataset(first_contracts, "b3-futures-dol-first-generic")

# %%
cal = Calendar.load("ANBIMA")
df_contracts = df.filter(pc.field("commodity") == 'DDI').to_table().to_pandas()
df_contracts['maturity_date'] = df_contracts['maturity_code'].apply(lambda x: maturity2date(x, cal))
df_contracts["business_days"] = cal.bizdays(df_contracts['refdate'], cal.following(df_contracts["maturity_date"]))
df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
df_contracts = df_contracts[["refdate", "symbol", "maturity_date", "settlement_price", "business_days"]]
brasa.write_dataset(df_contracts, "b3-futures-ddi")

# %%
cal = Calendar.load("ANBIMA")
df_contracts = df.filter(pc.field("commodity") == 'FRC').to_table().to_pandas()
df_contracts['maturity_date'] = df_contracts['maturity_code'].apply(lambda x: maturity2date(x, cal))
df_contracts["business_days"] = cal.bizdays(df_contracts['refdate'], cal.following(df_contracts["maturity_date"]))
df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
df_contracts = df_contracts[["refdate", "symbol", "maturity_date", "settlement_price", "business_days"]]
brasa.write_dataset(df_contracts, "b3-futures-frc")

# %%
cal = Calendar.load("ANBIMA")
df_contracts = df.filter(pc.field("commodity") == 'DAP').to_table().to_pandas()
df_contracts['maturity_date'] = df_contracts['maturity_code'].apply(lambda x: maturity2date(x, cal, "15th day"))
df_contracts["business_days"] = cal.bizdays(df_contracts['refdate'], cal.following(df_contracts["maturity_date"]))
df_contracts["adjusted_tax"] = (100000 / df_contracts["settlement_price"]) ** (252 / df_contracts["business_days"]) - 1
df_contracts.sort_values(["refdate", "maturity_date"], inplace=True)
df_contracts = df_contracts[["refdate", "symbol", "maturity_date", "settlement_price", "adjusted_tax", "business_days"]]
brasa.write_dataset(df_contracts, "b3-futures-dap")

# %%
# min_bd = df_contracts.groupby("refdate")["business_days"].transform("min")
# df_dap_first_contracts = df_contracts.loc[df_contracts["business_days"] == min_bd,:].copy()
# df_dap_first_contracts["symbol"] = "DAPT01"

first = df_contracts.groupby("refdate").nth(0)
second = df_contracts.groupby("refdate").nth(1)
merged = first.merge(second, on="refdate", how="left").set_index("refdate")
first_contracts = first.copy().reset_index(drop=True).set_index("refdate")
second_contracts = second.copy().reset_index(drop=True).set_index("refdate")
idx = merged.index[merged["business_days_x"] == 0]
first_contracts.loc[idx, :] = second_contracts.loc[idx, :]
first_contracts["ref"] = first_contracts["symbol"]
first_contracts["symbol"] = "DAPT01"
first_contracts.reset_index(inplace=True)
brasa.write_dataset(first_contracts, "b3-futures-dap-first-generic")
# %%

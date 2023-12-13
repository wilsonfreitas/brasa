# %%
# import os
# os.environ["BRASA_DATA_PATH"] = "D:\\brasa"

import sys
# sys.path.append('c:/Users/wilso/OneDrive/Documentos/Dev/python/brasa')
sys.path.append('.')

import pyarrow.dataset as ds
import pyarrow

import brasa

man = brasa.CacheManager()

# %%
tb_cotahist_yearly = brasa.get_dataset("b3-cotahist-yearly").to_table()
tb_cotahist_daily = brasa.get_dataset("b3-cotahist-daily").to_table()
tb_cotahist = pyarrow.concat_tables([tb_cotahist_yearly, tb_cotahist_daily])

tb_cotahist.sort_by([("refdate", "ascending")])
ds.write_dataset(tb_cotahist, man.db_path("b3-cotahist"), format="parquet", existing_data_behavior="overwrite_or_ignore")

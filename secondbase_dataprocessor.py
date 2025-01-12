# %%
import pandas as pd
import pyarrow as pa
# %%
parquet_file_path = r"C:\Users\EDY\Desktop\PROJECT\PI\cloudsail\database\aggregated_database\aggregated_data_60s(test_51turbines_ori).parquet"
df = pd.read_parquet(parquet_file_path, engine='pyarrow')

# %%

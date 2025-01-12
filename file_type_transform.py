# %%
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



class DATA_PREPROCESSING:
    def __init__(self, ):
        # vars
        self.CSV_FILE_PATH = r"C:\Users\EDY\Desktop\PROJECT\PI\cloudsail\database\aggregated_database\aggregated_data_60s(test_51turbines_ori).csv"
        self.PARQUET_FILR_PATH = r"C:\Users\EDY\Desktop\PROJECT\PI\cloudsail\database\aggregated_database\aggregated_data_60s(test_51turbines_ori).parquet"
        self.TIME_COLUMN = 'rectime'

    def csv2parque(self, ):
        # read .csv
        df = pd.read_csv(self.CSV_FILE_PATH)

        print(df.dtypes)

        # convert rectime to pandas datetime format
        df[self.TIME_COLUMN] = pd.to_datetime(df[self.TIME_COLUMN])

        # convert DataFrame to PyArrow
        table = pa.Table.from_pandas(df)
        print(table.schema)

        # save .parquet
        pq.write_table(table, self.PARQUET_FILR_PATH)
        print(f"Data has been converted and saved to {self.PARQUET_FILR_PATH}")

    def parquet2parquet(self,):
        # make sure that time column covert to PyArrow type

        # read .parquet
        table = pq.read_table(self.PARQUET_FILR_PATH)
        print(table.schema)

        # convert PyArrow to DataFrame
        df_trans = table.to_pandas(types_mapper=pd.ArrowDtype)
        print(df_trans.dtypes)

        # save .parquet
        pq.write_table(pa.Table.from_pandas(df_trans), self.PARQUET_FILR_PATH)
        print(f"Data has been converted and saved to {self.PARQUET_FILR_PATH}")

# %%

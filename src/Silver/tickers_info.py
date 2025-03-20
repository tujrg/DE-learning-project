import pandas as pd
import polars as pl
import numpy as np
import json
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
import os
#Set up Credential for GCS
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/opt/airflow/code/src/sql-server-replicate-0ec74ad95b13.json'
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'../sql-server-replicate-0ec74ad95b13.json'

credentials = service_account.Credentials.from_service_account_file(
    r'/opt/airflow/code/src/sql-server-replicate-0ec74ad95b13.json'
    # r'../sql-server-replicate-0ec74ad95b13.json'
)
bqclient = bigquery.Client(credentials=credentials, project=credentials.project_id,)

class TickersInfo():
    def __init__(self):
        self.project_name = "sql-server-replicate"
        self.bucket_name = "coding-pyspark-workspace-global"
        self.bronze_path = "tu_mai/BronzeLayer/TickersInfoData"
        self.silver_path = "tu_mai/SilverLayer"
        pass

    def get_bronze_data(self, gcs_path):
        """ Get TickersInfo raw data into a Dataframe
        Return:
            Dataframe about TickersInfo data
        """

        # Read the Delta table directly into a Polars DataFrame
        df = pl.read_delta(gcs_path)

        print(df)
        return df
    
    def validate_data(self, df):
        """ Validate polar Dataframe
        Args:
            df: Bronze polar dataframe
        Return:
            Validated polar dataframe
        """

        # Replace '.' with '_' in all column names
        df = df.rename({col: col.replace(".", "_") for col in df.columns})

        # Rename columns
        df = df.rename({"name": "company_name"})
        df = df.rename({"website": "company_website"})
        # Unnest struct columns
        df = df.unnest(["key_executives", "stock_exchanges"])

        # Convert salary column: remove "M", cast to float, multiply by 1e6, then cast to int using with_columns
        try:
            df = df.with_columns([
                (pl.col("salary")
                .str.replace("M", "")
                .cast(pl.Float64) * 1_000_000)
                .cast(pl.Int64)
                .alias("salary")
            ])
        except:
            df = df.with_columns(
                pl.col("salary").replace("",0)
            )

            df = df.with_columns([
                (pl.col("salary")
                .str.replace("M", "")
                .cast(pl.Float64) * 1_000_000)
                .cast(pl.Int64)
                .alias("salary")
            ])
        return df
    
    def write_gcs(self, df):
        """ Function to write polar Dataframe to Delta table
        Args:
            df: Polar Dataframe
        Return:
            Write polar dataframe into GCS
        """

        # Define the GCS path (Ensure you have authentication set up)
        gcs_path = f"gs://{self.bucket_name}/{self.silver_path}/TickersInfo"
        # Write polar df
        df.write_delta(gcs_path, mode="append")
        print('WRITE TICKERS INFO DATA SUCESSFULLY!')
    
    def main(self):
        gcs_path = f"gs://{self.bucket_name}/{self.bronze_path}/"
        df = self.get_bronze_data(gcs_path)
        df = self.validate_data(df)
        self.write_gcs(df)
        
# if __name__ == "__main__":
#     tmp = TickersInfo()
#     tmp.main()
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

class EndOfDay():
    def __init__(self):
        self.project_name = "sql-server-replicate"
        self.bucket_name = "coding-pyspark-workspace-global"
        self.bronze_path = "tu_mai/BronzeLayer/EndOfDayData"
        self.silver_path = "tu_mai/SilverLayer"
        pass

    def get_bronze_data(self, gcs_path):
        """ Get EndOfDay raw data into a Dataframe
        Return:
            Dataframe about EndOfDay data
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

        # Cast datetime datatype
        df = df.with_columns(
            df["date"].str.to_datetime().dt.date()
        )
        # Upcase price_currency
        df = df.with_columns(df["price_currency"].str.to_uppercase())

        return df
    
    def write_gcs(self, df):
        """ Function to write polar Dataframe to Delta table
        Args:
            df: Polar Dataframe
        Return:
            Write polar dataframe into GCS
        """

        # Define the GCS path (Ensure you have authentication set up)
        gcs_path = f"gs://{self.bucket_name}/{self.silver_path}/EndOfDay"
        # Write polar df
        df.write_delta(gcs_path, mode="append")
        print('WRITE END OF DAY DATA SUCESSFULLY!')
    
    def main(self):
        gcs_path = f"gs://{self.bucket_name}/{self.bronze_path}/"
        df = self.get_bronze_data(gcs_path)
        df = self.validate_data(df)
        self.write_gcs(df)
        
# if __name__ == "__main__":
#     tmp = EndOfDay()
#     tmp.main()
import pandas as pd
from pandas import json_normalize
import polars as pl
import numpy as np
import json
from google.cloud import storage
from google.oauth2 import service_account
from google.cloud import bigquery
import os
#Set up Credential for GCS
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'/opt/airflow/code/src/sql-server-replicate-0ec74ad95b13.json'

credentials = service_account.Credentials.from_service_account_file(
    r'/opt/airflow/code/src/sql-server-replicate-0ec74ad95b13.json'
)
bqclient = bigquery.Client(credentials=credentials, project=credentials.project_id,)

class DeltaTable():
    def __init__(self, data_folder):
        self.project_name = "sql-server-replicate"
        self.bucket_name = "coding-pyspark-workspace-global"
        self.data_path = "tu_mai/LandingLayer"
        self.bronze_path = "tu_mai/BronzeLayer"
        self.data_folder = data_folder
        pass

    def get_raw_data(self):
        """ Get raw data into a Polar Dataframe
        Return:
            Polar dataframe about raw data
        """

        # Initialize the GCS client
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        
        raw_data = []
        # List all blobs (files) in the specified folder
        blobs = bucket.list_blobs(prefix=f"{self.data_path}/{self.data_folder}/")
        # Filter for JSONL files and sort by the latest updated timestamp
        jsonl_files = [blob for blob in blobs if blob.name.endswith('.jsonl')]
        if not jsonl_files:
            print("No JSONL files found.")
            return []
        latest_blob = max(jsonl_files, key=lambda b: b.updated)
        print(f"Downloading latest file: {latest_blob.name}")
        # Download the file content as a string
        content = latest_blob.download_as_string()
        # Load the JSON content. 
        json_data = json.loads(content)

        if isinstance(json_data, list):
            raw_data.extend(json_data)
        else:
            raw_data.append(json_data)
        
        # # Convert the combined list of records into a Polar DataFrame
        # try:
        #     df = pl.DataFrame(raw_data)
        # except:
        #     tmp = pd.DataFrame(raw_data)
        #     df = pl.DataFrame(tmp)

        # result = pl.DataFrame()
        # for row in df['_airbyte_data']:
        #     row_df = pl.DataFrame(row['data'])
        #     result = pl.concat([result, row_df])

        
        # Flatten JSON before converting
        df_flattened = json_normalize(raw_data[0]['_airbyte_data']['data'])
        # Convert to Polars
        result = pl.from_pandas(df_flattened)

        # Iterate over each column in the DataFrame
        for col in result.columns:
            # Convert the column to a Python list and check if any element is a list
            if any(isinstance(x, list) for x in result[col].to_list()):
                result = result.explode(col)

        # Delete column with all valuses is null
        result = result[[s.name for s in result if not (s.null_count() == result.height)]]
        print(f"RAW DATA FOR {self.data_folder}:\n", result)
        return result
    
    def write_gcs(self, df, gcs_path):
        """ Function to write polar Dataframe to Delta table
        Args:
            df: Polar Dataframe
            gcs_path: path of directory to store Delta table
        Return:
            Write polar dataframe into GCS
        """

        # # Define the GCS path (Ensure you have authentication set up)
        # gcs_path = f"gs://{self.bucket_name}/{self.data_path}/BronzeLayer/EndOfDay"
        # Write polar df
        df.write_delta(gcs_path, mode="append")
        print('WRITE END OF DAY DATA SUCESSFULLY!')
    
    def main(self):
        df = self.get_raw_data()
        gcs_path = f"gs://{self.bucket_name}/{self.bronze_path}/{self.data_folder}"
        self.write_gcs(df, gcs_path)
        
# if __name__ == "__main__":
#     tmp = EndOfDay()
#     tmp.main()
from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
# Define the timezone for Vietnam
import pytz
from datetime import datetime, timedelta

vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
START_DATE = datetime.now(vietnam_tz) - timedelta(days=1)

# Define the default arguments for the DAG
default_args = {
    "owner": "tu.mai@jrgvn.com",
    "depends_on_past": False,
    "email_on_failure": False,
    "email": "tu.mai@jrgvn.com",
    "email_on_retry": False,
}

# Airbyte Connection ID (replace with your actual connection ID)
eod_conn_id = "97abb886-7427-4550-ac19-e10b2f7da51a"
splits_conn_id = "b73d1dd5-5aad-4bfa-9ff4-dbb39fc863ee"
dividends_conn_id = "ea625de8-07df-4e16-abbb-d9b6d6f865eb"
currencies_conn_id = "a9a684a4-047c-41ce-ac7f-a4991022f614"
tickers_info_conn_id = "dc6531de-1351-4eb3-9eb7-6471d7d52e2f"
tickers_list_conn_id = "03d05e77-3198-41b2-97e0-e7cf6227b0ac"

# Define DAG
with DAG(
    dag_id="replicate_raw_data",
    default_args=default_args,
    schedule_interval="0 17 * * *", # Adjust as needed
    start_date=START_DATE,
    catchup=False,
    tags=["landing"],
) as dag:
    
    # Local task
    def local_task():
        print("This task runs on the local executor.")

    local_task_operator = PythonOperator(
        task_id="local_task",
        python_callable=local_task,
    )
    
    eod_sync = AirbyteTriggerSyncOperator(
        task_id="EOD_sync",
        airbyte_conn_id="airbyte_conn",
        connection_id=eod_conn_id,
        asynchronous=False,  # Set to True if you don't want to wait for the sync to finish
        timeout=3600,  # Timeout in seconds
    )

    splits_sync = AirbyteTriggerSyncOperator(
        task_id="Splits_sync",
        airbyte_conn_id="airbyte_conn",
        connection_id=splits_conn_id,
        asynchronous=False,  # Set to True if you don't want to wait for the sync to finish
        timeout=3600,  # Timeout in seconds
    )

    dividends_sync = AirbyteTriggerSyncOperator(
        task_id="Dividends_sync",
        airbyte_conn_id="airbyte_conn",
        connection_id=dividends_conn_id,
        asynchronous=False,  # Set to True if you don't want to wait for the sync to finish
        timeout=3600,  # Timeout in seconds
    )

    currencies_sync = AirbyteTriggerSyncOperator(
        task_id="Currencies_sync",
        airbyte_conn_id="airbyte_conn",
        connection_id=currencies_conn_id,
        asynchronous=False,  # Set to True if you don't want to wait for the sync to finish
        timeout=3600,  # Timeout in seconds
    )

    tickers_info_sync = AirbyteTriggerSyncOperator(
        task_id="TickersInfo_sync",
        airbyte_conn_id="airbyte_conn",
        connection_id=tickers_info_conn_id,
        asynchronous=False,  # Set to True if you don't want to wait for the sync to finish
        timeout=3600,  # Timeout in seconds
    )

    tickers_list_sync = AirbyteTriggerSyncOperator(
        task_id="TickersList_sync",
        airbyte_conn_id="airbyte_conn",
        connection_id=tickers_list_conn_id,
        asynchronous=False,  # Set to True if you don't want to wait for the sync to finish
        timeout=3600,  # Timeout in seconds
    )

    trigger_bronze_layer = TriggerDagRunOperator(
        task_id="trigger_bronze_layer",
        trigger_dag_id="upload_raw_data",
        dag=dag,
    )

    (
        local_task_operator
        >> [
            eod_sync,
            splits_sync,
            dividends_sync,
            currencies_sync,
            tickers_info_sync,
            tickers_list_sync
        ] >> trigger_bronze_layer
    )

if __name__ == "__main__":
    dag.cli()

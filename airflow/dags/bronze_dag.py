from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
# import sys
# sys.path.append(r'/mnt/d/Tu/Personal_project/DE-learning-project')
import src.Bronze.replicate_delta_table as delta
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

# Define DAG
with DAG(
    dag_id="upload_raw_data",
    default_args=default_args,
    schedule_interval=None, # Adjust as needed
    start_date=START_DATE,
    catchup=False,
    tags=["bronze layer"],
) as dag:
    
    # Local task
    def local_task():
        print("This task runs on the local executor.")

    local_task_operator = PythonOperator(
        task_id="local_task",
        python_callable=local_task,
    )

    @task(task_id="upload_eod")
    def upload_eod():
        delta.DeltaTable(data_folder="EndOfDayData").main()
    eod_data = upload_eod()

    @task(task_id="upload_splits")
    def upload_splits():
        delta.DeltaTable(data_folder="SplitsData").main()
    splits_data = upload_splits()

    @task(task_id="upload_dividends")
    def upload_dividends():
        delta.DeltaTable(data_folder="DividendsData").main()
    dividends_data = upload_dividends()

    @task(task_id="upload_currencies")
    def upload_currencies():
        delta.DeltaTable(data_folder="CurrenciesData").main()
    currencies_data = upload_currencies()

    @task(task_id="upload_tickersinfo")
    def upload_tickersinfo():
        delta.DeltaTable(data_folder="TickersInfoData").main()
    tickers_info_data = upload_tickersinfo()

    @task(task_id="upload_tickerslist")
    def upload_tickerslist():
        delta.DeltaTable(data_folder="TickersListData").main()
    tickers_list_data = upload_tickerslist()

    trigger_silver_layer = TriggerDagRunOperator(
        task_id="trigger_silver_layer",
        trigger_dag_id="validate_data",
        dag=dag,
    )

    (
        local_task_operator
        >> [
            eod_data,
            splits_data,
            dividends_data,
            currencies_data,
            tickers_info_data,
            tickers_list_data
        ] >> trigger_silver_layer
    )

if __name__ == "__main__":
    dag.cli()

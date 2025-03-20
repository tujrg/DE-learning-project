from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.decorators import task
from airflow.operators.python import PythonOperator
# import sys
# sys.path.append(r'/mnt/d/Tu/Personal_project/DE-learning-project')
import src.Silver.end_of_day as eod
import src.Silver.currencies as cur
import src.Silver.dividends as div
import src.Silver.splits as spl
import src.Silver.tickers_info as t_info
import src.Silver.tickers_list as t_list
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
    dag_id="validate_data",
    default_args=default_args,
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
    tags=["silver layer"],
) as dag:
    
    # Local task
    def local_task():
        print("This task runs on the local executor.")

    local_task_operator = PythonOperator(
        task_id="local_task",
        python_callable=local_task,
    )

    @task(task_id="upload_silver_eod")
    def upload_eod():
        eod.EndOfDay().main()
    eod_data = upload_eod()

    @task(task_id="upload_silver_splits")
    def upload_splits():
        spl.Splits().main()
    splits_data = upload_splits()

    @task(task_id="upload_silver_dividends")
    def upload_dividends():
        div.Dividends().main()
    dividends_data = upload_dividends()

    @task(task_id="upload_silver_currencies")
    def upload_currencies():
        cur.Currencies().main()
    currencies_data = upload_currencies()

    @task(task_id="upload_silver_tickersinfo")
    def upload_tickersinfo():
        t_info.TickersInfo().main()
    tickers_info_data = upload_tickersinfo()

    @task(task_id="upload_silver_tickerslist")
    def upload_tickerslist():
        t_list.TickersList().main()
    tickers_list_data = upload_tickerslist()

    (
        local_task_operator
        >> [
            eod_data,
            splits_data,
            dividends_data,
            currencies_data,
            tickers_info_data,
            tickers_list_data
        ]
    )

if __name__ == "__main__":
    dag.cli()

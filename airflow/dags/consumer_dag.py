from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from src.Postgres_db.Postgres import LmsTraining
import json
import requests
import pytz

vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
START_DATE = datetime.now(vietnam_tz) - timedelta(days=1)

KAFKA_BROKER = 'broker:29092'
topic_name = 'eod_data'


def consume_kafka():
    consumer = KafkaConsumer(topic_name, bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda v: json.loads(v.decode('utf-8')))
    print('Consuming messages from Kafka')
    # Connect to postgres
    obj = LmsTraining()
    obj.connect_to_DB()
    
    # Get messages and save in df
    for mess in consumer:
        df = pd.DataFrame([mess.value])
        print('OUTPUT: ', df)
        obj.insert_to_pg(df, 'test_table')
        

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
    dag_id="consume_from_kafka",
    default_args=default_args,
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
    tags=["stream layer"],
) as dag:

    consume_task = PythonOperator(
        task_id='consume_kafka',
        python_callable=consume_kafka,
        dag=dag,
    )

    (
        consume_task
    )

if __name__ == "__main__":
    dag.cli()

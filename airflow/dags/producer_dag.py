from airflow import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from kafka import KafkaProducer
import json
import requests
import pytz

vietnam_tz = pytz.timezone("Asia/Ho_Chi_Minh")
START_DATE = datetime.now(vietnam_tz) - timedelta(days=1)

KAFKA_BROKER = 'broker:29092'
topic_name = 'eod_data'

def get_stream_data():
    api_key = '4cdd1d77a221e74d0538b9be7f8ea184'
    url = 'https://api.marketstack.com/v2/eod'

    # Parameters for the API request
    params = {
        "access_key": api_key,
        "symbols": "AAPL",  # Example: Apple stock
        "limit": 1  # Number of results to fetch
    }
    # Make the GET request
    response = requests.get(url, params=params)

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()  # Parse JSON response
        print(data)
    else:
        print(f"Error: {response.status_code}, {response.text}")
    return data

def produce_to_kafka():
    # data = get_stream_data()
    data = {"timestamp": str(datetime.utcnow()), "value": "sample_data"}
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    producer.send(topic_name, data)
    producer.flush()
    print('SEND DATA TO KAFKA!!!')
        

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
    dag_id="produce_streaming_data",
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    start_date=START_DATE,
    catchup=False,
    tags=["stream layer"],
) as dag:
    produce_task = PythonOperator(
        task_id='produce_to_kafka',
        python_callable=produce_to_kafka,
        dag=dag,
    )

    # trigger_dag_consumer = TriggerDagRunOperator(
    #     task_id="trigger_dag_consumer",
    #     trigger_dag_id="consume_from_kafka",
    #     dag=dag,
    # )

    (
        produce_task 
        # >> trigger_dag_consumer
    )

if __name__ == "__main__":
    dag.cli()

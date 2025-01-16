import os
import sys
import logging
import requests
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


#from weatheretl.scripts.extract import extract_weather
#sys.path.append("/home/amychude/Weather_ETL")

from weather_etl.config.config import log_file
from weather_etl.config.config import server
from weather_etl.config.config import database
from weather_etl.config.config import driver
from weather_etl.config.config import batch_size
from weather_etl.config.config import cities

from weather_etl.scripts.etl_functions import convert_time

from weather_etl.scripts.extract_weather import extract_weather
from weather_etl.scripts.extract_traffic import extract_traffic_data

from weather_etl.scripts.transform_traffic import transform_traffic
from weather_etl.scripts.transform_weather import transform_weather
from weather_etl.scripts.database_connection import connect_db
from weather_etl.scripts.load import load_weather



logging.basicConfig(
    level=logging.DEBUG,  # Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(filename)s - %(message)s",  #Include filename in the logs
    handlers=[
        logging.FileHandler(log_file),  # Write logs to the log file
        logging.StreamHandler()  # Write logs to the console
    ]
)


default_args = {
    "owner": "amy",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

# Define the DAG
with DAG(
    dag_id="weather_etl",         
    default_args=default_args,
    description="ETL pipeline for weather data",
    schedule_interval="*/5 * * * *",  # Run every 5 minutes
    start_date=datetime(2025, 1, 8),   #DAG start date
    catchup=False,                     #Skip running for past dates
    tags=["weather", "traffic", "ETL"],
) as dag:
    
    def generate_timestamp(**context):
        # get current time extraction starts
        current_time_utc = datetime.utcnow().timestamp()

        #convert the time from UTC to normal time format. This is done so this time and time from APIs are consistent formats
        current_time = convert_time(current_time_utc)

        context['ti'].xcom_push(key='shared_timestamp', value=current_time)

    def extract_weather_task(**kwargs):
        cities = kwargs["cities"]
        batch_size = kwargs["batch_size"]
        current_time = kwargs['ti'].xcom_pull(task_ids='generate_timestamp', key='shared_timestamp')

        return extract_weather(cities, batch_size, current_time)
    
    def extract_traffic_task(**kwargs):
        cities = kwargs['cities']
        batch_size = kwargs['batch_size']
        current_time = kwargs['ti'].xcom_pull(task_ids='generate_timestamp', key='shared_timestamp')
        return extract_traffic_data(cities, batch_size, current_time)

    def transform_weather_task(**kwargs):
        raw_weather_data = kwargs['ti'].xcom_pull(task_ids='extract_weather')
        return transform_weather(raw_weather_data)

    def transform_traffic_task(**kwargs):
        raw_traffic_data = kwargs['ti'].xcom_pull(task_ids='extract_traffic')
        return transform_traffic(raw_traffic_data)
    
    def merge_and_load_task(**kwargs):

        tf_weather_data = kwargs['ti'].xcom_pull(task_ids='transform_weather')
        tf_traffic_data = kwargs['ti'].xcom_pull(task_ids='transform_traffic')
        
        if tf_weather_data is not None and tf_traffic_data is not None:

            merged_data = pd.merge(tf_weather_data, tf_traffic_data, on=["city", "extraction_time"], how="outer")
            connection = connect_db(server, database, driver)
            load_weather(merged_data, connection)
            logging.info("ETL process completed successfully.")

        else:
            logging.warning("One or both transformed datasets are None. Skipping load.")

    generate_timestamp_task = PythonOperator(
        task_id="generate_timestamp",
        python_callable=generate_timestamp,
        provide_context=True,
    )

    extract_weather_task = PythonOperator(
        task_id='extract_weather',
        python_callable=extract_weather_task,
        op_kwargs={'cities': cities, 'batch_size': batch_size},
        provide_context=True,
        dag=dag,
    )

    extract_traffic_task = PythonOperator(
        task_id='extract_traffic',
        python_callable=extract_traffic_task,
        op_kwargs={'cities': cities, 'batch_size': batch_size},
        provide_context=True,
        dag=dag,
    )

    transform_weather_task = PythonOperator(
        task_id='transform_weather',
        python_callable=transform_weather_task,
        provide_context=True,
        dag=dag,
    )

    transform_traffic_task = PythonOperator(
        task_id='transform_traffic',
        python_callable=transform_traffic_task,
        provide_context=True,
        dag=dag,
    )

    merge_and_load_task = PythonOperator(
        task_id='merge_and_load',
        python_callable=merge_and_load_task,
        provide_context=True,
        dag=dag,
    )

    generate_timestamp_task >> [extract_weather_task, extract_traffic_task]

    # Set dependencies between extraction and transformation tasks
    extract_weather_task >> transform_weather_task
    extract_traffic_task >> transform_traffic_task

    [transform_weather_task, transform_traffic_task] >> merge_and_load_task
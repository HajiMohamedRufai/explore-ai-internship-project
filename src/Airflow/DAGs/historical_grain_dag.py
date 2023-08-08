import time
import datetime
import pandas as pd
import glob
import boto3
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from includes.etl.grain.historical_grain_etl import grain_data_ingesting
from includes.etl.grain.historical_grain_etl import grain_pre_processing
from includes.etl.grain.historical_grain_etl import grain_processing
from includes.etl.grain.historical_grain_etl import database_upload


default_args = {
    "owner": "ExploreAI-Team8",
    "depends_on_past": False,
    "email": ["onidajo99@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5)
}


dag = DAG(
    dag_id='Historical_Grain_Airflow_Pipeline',
    default_args=default_args,
    description="A pipeline to pull data from the grain source website, process it and write to an S3 bucket while loading into a relational database.",
    schedule_interval=datetime.timedelta(days=7),
    start_date=airflow.utils.dates.days_ago(0),
    catchup=False,
    tags=['historical_grain']
)

def grain_data_ingest():
    market_name, market_code, market_type = grain_data_ingesting()
    grain_pre_processing(market_name, market_code, market_type)
    
def grain_data_process():
    df = grain_processing()
    database_upload(df)

start = DummyOperator(task_id='begin')

grain_selenium_pandas_extract = PythonOperator(
    task_id='grain_ingestion',
    python_callable=grain_data_ingest,
    dag=dag
    )

grain_pyspark_process = PythonOperator(
        task_id = 'grain_processing',
        python_callable=grain_data_process,
        dag=dag
    )

#run_this = BashOperator(
#            task_id="check_firefox",
#                bash_command= "java -version" ,
#                dag = dag
#    )


ready = DummyOperator(task_id='ready')

start >> grain_selenium_pandas_extract >> grain_pyspark_process>>  ready

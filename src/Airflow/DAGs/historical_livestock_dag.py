import time
import datetime
import pandas as pd
import glob
import boto3
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from includes.etl.livestock.historical_livestock_etl import livestock_data_ingesting
from includes.etl.livestock.historical_livestock_etl import livestock_pre_processing
from includes.etl.livestock.historical_livestock_etl import livestock_processing
from includes.etl.livestock.historical_livestock_etl import database_upload

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
    dag_id='Historical_Livestock_Airflow_Pipeline',
    default_args=default_args,
    description="A pipeline to pull data from the livestock source website and write to an S3 bucket for processing.",
    schedule_interval=datetime.timedelta(days=7),
    start_date=airflow.utils.dates.days_ago(0),
    catchup=False,
    tags=['historical_livestock']
)

def data_ingest():
   livestock_data_ingesting()

def data_pre_process():
    livestock_pre_processing()

def data_process():
    df, df1, df2 = livestock_processing()
    database_upload(df,df1,df2)

start = DummyOperator(task_id='start')

extract = PythonOperator(
    task_id='ingestion',
    python_callable=data_ingest,
    dag=dag
    )
pre_process = PythonOperator(
    task_id='pre_processing',
    python_callable=data_pre_process,
    dag=dag
    )
clean_load = PythonOperator(
    task_id='processing',
    python_callable=data_process,
    dag=dag
    )




ready = DummyOperator(task_id='ready')

start >> extract >> pre_process >> clean_load >> ready

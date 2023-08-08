import glob
import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from includes.etl.livestock.historical_horticulture_etl import horticulture_data_ingesting
from includes.etl.livestock.historical_horticulture_etl import preprocessing
from includes.etl.livestock.historical_horticulture_etl import processing
from includes.etl.livestock.historical_horticulture_etl import removal_of_folder_containing_excel_files_and_pre_processed_csv
from includes.etl.livestock.historical_horticulture_etl import database_upload

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
    dag_id='Historical_Horticulture_Airflow_Pipeline',
    default_args=default_args,
    description="A pipeline to pull data from the livestock source website and write to an S3 bucket for processing.",
    schedule_interval=datetime.timedelta(days=7),
    start_date=airflow.utils.dates.days_ago(0),
    catchup=False,
    tags=['historical_horticulture']
)

def data_ingest():
   horticulture_data_ingesting()

def data_pre_process():
    preprocessing()
    removal_of_folder_containing_excel_files_and_pre_processed_csv()

def data_process():
    df= processing()
    database_upload(df)

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

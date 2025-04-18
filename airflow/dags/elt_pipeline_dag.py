import os
import sys
from datetime import datetime
from airflow import DAG

from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from scripts.extract_load import extract_load

###############################################
# Parameters & Arguments
###############################################
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
MINIO_ACCESS_KEY = os.environ['MINIO_ACCESS_KEY']
MINIO_SECRET_KEY = os.environ['MINIO_SECRET_KEY']
###############################################

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


def download_upload_dag(
        dag,
        url_template,
        local_path_template,
        minio_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_path_template}"
        )

        extract_load_task = PythonOperator(
            task_id="extract_load",
            python_callable=extract_load,
            op_kwargs={'endpoint_url': MINIO_ENDPOINT, 'access_key': MINIO_ACCESS_KEY, 'secret_key': MINIO_SECRET_KEY,
                       'source': local_path_template, 'dest': minio_path_template}
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_path_template}"
        )

        download_dataset_task >> extract_load_task >> rm_task


# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2019-01.parquet
URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

YELLOW_TAXI_FILE_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
YELLOW_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + ('/data/output_yellow_tripdata{{ execution_date.strftime(\'%Y-%m\') '
                                              '}}.parquet')
YELLOW_TAXI_MINIO_PATH_TEMPLATE = ("yellow_tripdata/{{ execution_date.strftime(\'%Y\') }}/yellow_tripdata_{{ "
                                   "execution_date.strftime(\'%Y-%m\') }}.parquet")

yellow_taxi_data_dag = DAG(
    dag_id="yellow_taxi_data_v1",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2024, 12, 31),
    default_args=default_args,
    catchup=True,
    max_active_runs=3
)

download_upload_dag(
    dag=yellow_taxi_data_dag,
    url_template=YELLOW_TAXI_FILE_TEMPLATE,
    local_path_template=YELLOW_OUTPUT_FILE_TEMPLATE,
    minio_path_template=YELLOW_TAXI_MINIO_PATH_TEMPLATE
)

GREEN_TAXI_FILE_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
GREEN_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + ('/data/output_green_tripdata{{ execution_date.strftime(\'%Y-%m\') '
                                             '}}.parquet')
GREEN_TAXI_MINIO_PATH_TEMPLATE = ("green_tripdata/{{ execution_date.strftime(\'%Y\') }}/green_tripdata_{{ "
                                  "execution_date.strftime(\'%Y-%m\') }}.parquet")

green_taxi_data_dag = DAG(
    dag_id="green_taxi_data_v1",
    schedule_interval="0 7 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2024, 12, 31),
    default_args=default_args,
    catchup=True,
    max_active_runs=3
)

download_upload_dag(
    dag=green_taxi_data_dag,
    url_template=GREEN_TAXI_FILE_TEMPLATE,
    local_path_template=GREEN_OUTPUT_FILE_TEMPLATE,
    minio_path_template=GREEN_TAXI_MINIO_PATH_TEMPLATE,
)

FHV_TAXI_FILE_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/data/output_fhv_tripdata{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
FHV_TAXI_MINIO_PATH_TEMPLATE = ("fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/fhv_tripdata_{{ "
                                "execution_date.strftime(\'%Y-%m\') }}.parquet")

fhv_taxi_data_dag = DAG(
    dag_id="fhv_taxi_data_v1",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2024, 12, 31),
    default_args=default_args,
    catchup=True,
    max_active_runs=3
)

download_upload_dag(
    dag=fhv_taxi_data_dag,
    url_template=FHV_TAXI_FILE_TEMPLATE,
    local_path_template=FHV_OUTPUT_FILE_TEMPLATE,
    minio_path_template=FHV_TAXI_MINIO_PATH_TEMPLATE,
)
# https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
ZONES_URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'
ZONES_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/data/taxi_zone_lookup.csv'
ZONES_MINIO_PATH_TEMPLATE = "taxi_zone/taxi_zone_lookup.csv"

zones_data_dag = DAG(
    dag_id="zones_data_v1",
    schedule_interval="@once",
    start_date=days_ago(1),
    default_args=default_args,
    catchup=True,
    max_active_runs=3
)

download_upload_dag(
    dag=zones_data_dag,
    url_template=ZONES_URL_TEMPLATE,
    local_path_template=ZONES_OUTPUT_FILE_TEMPLATE,
    minio_path_template=ZONES_MINIO_PATH_TEMPLATE,
)

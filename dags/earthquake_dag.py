import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.extract import extract_and_upload_to_bronze
from src.load import transform_and_load_silver, execute_gold_query

BUCKET_NAME = "earthquake-data-bronze"
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

default_args = {
    'owner': 'karen_morel',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='earthquake_medallion_pipeline_v1',
    default_args=default_args,
    description='ETL Medallion: API USGS -> S3 (Bronze) -> MotherDuck (Silver/Gold)',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['earthquake', 's3', 'motherduck', 'etl', 'gold', 'medallion']
)
def earthquake_pipeline():

    @task()
    def bronze_task() -> str:
        return extract_and_upload_to_bronze(bucket_name=BUCKET_NAME)

    @task()
    def silver_task(s3_key: str):
        md_token = Variable.get("motherduck_token")
        transform_and_load_silver(
            s3_key=s3_key,
            bucket_name=BUCKET_NAME,
            md_token=md_token
        )

    @task()
    def gold_daily_summary_task():
        md_token = Variable.get("motherduck_token")
        sql_path = os.path.join(BASE_DIR, 'src', 'sql', 'gold_daily_summary.sql')
        execute_gold_query(sql_file_path=sql_path, md_token=md_token)

    @task()
    def gold_high_intensity_task():
        md_token = Variable.get("motherduck_token")
        sql_path = os.path.join(BASE_DIR, 'src', 'sql', 'gold_high_intensity.sql')
        execute_gold_query(sql_file_path=sql_path, md_token=md_token)

    # Definición de dependencias
    raw_s3_key = bronze_task()
    silver_step = silver_task(raw_s3_key)
    
    gold_summary = gold_daily_summary_task()
    gold_high = gold_high_intensity_task()

    silver_step >> [gold_summary, gold_high]

earthquake_pipeline()
import json
import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

default_args = {
    'owner': 'karen_morel',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='earthquake_api_to_s3_v1',
    default_args=default_args,
    description='Pipeline ETL: Ingesta de sismos desde USGS API hacia AWS S3 (Bronze Layer)',
    schedule='@daily',  
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['earthquake', 's3', 'api', 'bronze']
)
def earthquake_pipeline():

    @task()
    def extract_earthquake_data():
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
        print(f"Pidiendo datos a la API: {url}")
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        total_earthquakes = len(data.get('features', []))
        print(f"Extracción exitosa. Total de sismos obtenidos: {total_earthquakes}")
        return data

    @task()
    def upload_to_s3(data: dict):
        BUCKET_NAME = "earthquake-data-bronze" 
        
        now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"raw/earthquakes_{now_str}.json"
        
        print(f"Subiendo datos a AWS S3 en el bucket: {BUCKET_NAME}...")
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        
        s3_hook.load_string(
            string_data=json.dumps(data, indent=2),
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f"✅ ¡Éxito! Archivo subido en: s3://{BUCKET_NAME}/{s3_key}")

    raw_json = extract_earthquake_data()
    upload_to_s3(raw_json)

earthquake_pipeline()
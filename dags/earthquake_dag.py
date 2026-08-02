import json
import requests
import pandas as pd
import duckdb
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

default_args = {
    'owner': 'karen_morel',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='earthquake_api_to_s3_v1',
    default_args=default_args,
    description='Pipeline ETL: API USGS -> AWS S3 -> MotherDuck',
    schedule='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['earthquake', 's3', 'motherduck', 'etl']
)

def earthquake_pipeline():

    @task()
    def extract_earthquake_data():
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"
        print(f"Pidiendo datos a la API: {url}")
        
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task()
    def upload_to_s3(data: dict):
        BUCKET_NAME = "earthquake-data-bronze" 
        
        now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        s3_key = f"raw/earthquakes_{now_str}.json"
        
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            string_data=json.dumps(data, indent=2),
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f"✅ ¡Éxito! Archivo subido en: s3://{BUCKET_NAME}/{s3_key}")

    @task()
    def load_to_motherduck(data: dict):

        print("Aplanando datos JSON con Pandas...")
        features = data.get('features', [])
        
        flat_data = []
        for f in features:
            props = f.get('properties', {})
            geom = f.get('geometry', {}).get('coordinates', [None, None, None])
            
            flat_data.append({
                'id': f.get('id'),
                'magnitude': props.get('mag'),
                'place': props.get('place'),
                'time_epoch': props.get('time'),
                'longitude': geom[0] if len(geom) > 0 else None,
                'latitude': geom[1] if len(geom) > 1 else None,
                'depth': geom[2] if len(geom) > 2 else None
            })
        
        df = pd.DataFrame(flat_data)

        df['timestamp'] = pd.to_datetime(df['time_epoch'], unit='ms')
        
        print("Conectando a MotherDuck...")
        md_token = Variable.get("motherduck_token")
        con = duckdb.connect(f'md:earthquakes_dw?motherduck_token={md_token}')
        
        print("Creando tabla silver_earthquakes...")
        con.execute("CREATE OR REPLACE TABLE silver_earthquakes AS SELECT * FROM df")
        
        print(f"✅ ¡Éxito! {len(df)} registros cargados en MotherDuck.")
        con.close()

    raw_json = extract_earthquake_data()
    
    upload_to_s3(raw_json)
    load_to_motherduck(raw_json)

earthquake_pipeline()
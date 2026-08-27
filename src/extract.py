import json
import requests
from datetime import datetime
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

USGS_API_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

def extract_and_upload_to_bronze(bucket_name: str, aws_conn_id: str = 'aws_default') -> str:
    print(f"📡 Solicitando datos a la API: {USGS_API_URL}")
    response = requests.get(USGS_API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()

    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    s3_key = f"raw/earthquakes_{now_str}.json"

    print(f"☁️ Subiendo archivo crudo a S3: s3://{bucket_name}/{s3_key}")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_string(
        string_data=json.dumps(data, indent=2),
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    print("✅ Archivo subido con éxito a Bronze.")
    return s3_key
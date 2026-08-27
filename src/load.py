import json
import pandas as pd
import duckdb
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def transform_and_load_silver(s3_key: str, bucket_name: str, md_token: str, aws_conn_id: str = 'aws_default'):
    print(f"📥 Descargando {s3_key} desde S3...")
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    raw_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
    data = json.loads(raw_content)

    print("🔄 Transformando y aplanando GeoJSON con Pandas...")
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
    if not df.empty and 'time_epoch' in df.columns:
        df['timestamp'] = pd.to_datetime(df['time_epoch'], unit='ms')

    print(f"🦆 Conectando a MotherDuck para cargar {len(df)} registros en Silver...")
    con = duckdb.connect(f'md:earthquakes_dw?motherduck_token={md_token}')
    con.execute("CREATE OR REPLACE TABLE silver_earthquakes AS SELECT * FROM df")
    con.close()
    print("✅ Capa Silver actualizada exitosamente.")

def execute_gold_query(sql_file_path: str, md_token: str):
    print(f"📊 Ejecutando agregación Gold desde: {sql_file_path}")
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        query = f.read()

    con = duckdb.connect(f'md:earthquakes_dw?motherduck_token={md_token}')
    con.execute(query)
    con.close()
    print(f"✅ Query {sql_file_path} ejecutada con éxito.")
import pytest
from unittest.mock import patch, MagicMock
from src.extract import extract_and_upload_to_bronze
from src.load import transform_and_load_silver

def test_extract_and_upload_to_bronze():
    mock_response = {
        "features": [{"id": "test1", "properties": {"mag": 4.5}}]
    }
    
    with patch("requests.get") as mock_get, patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.load_string") as mock_s3_load:
        mock_get.return_value.status_code = 200
        mock_get.return_value.json.return_value = mock_response

        s3_key = extract_and_upload_to_bronze(bucket_name="test-bucket")

        assert s3_key.startswith("raw/earthquakes_")
        assert s3_key.endswith(".json")
        assert mock_s3_load.called

def test_transform_and_load_silver():
    dummy_json_str = '{"features": [{"id": "us1", "properties": {"mag": 5.0, "place": "Salta", "time": 1700000000000}, "geometry": {"coordinates": [-65.0, -24.0, 10.0]}}]}'
    
    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.read_key", return_value=dummy_json_str), \
         patch("duckdb.connect") as mock_duckdb:
        
        mock_conn = MagicMock()
        mock_duckdb.return_value = mock_conn

        transform_and_load_silver(
            s3_key="raw/test.json",
            bucket_name="test-bucket",
            md_token="fake_token"
        )

        assert mock_conn.execute.called
        assert mock_conn.close.called
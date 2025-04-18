import sys
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from extract import extract_csv
from transform import transform_data
from load import load_to_postgres
import boto3
from botocore.exceptions import NoCredentialsError

# MinIO
MINIO_ENDPOINT = 'minio:9000'  # MinIO serving address
MINIO_ACCESS_KEY = 'minio'
MINIO_SECRET_KEY = 'minio123'
MINIO_BUCKET = 'imdb-movies'


def upload_to_minio(file_path: str, bucket_name: str, object_name: str):
    s3_client = boto3.client('s3', endpoint_url=f'http://{MINIO_ENDPOINT}',
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY)

    # Bucket
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except:
        print(f"Bucket '{bucket_name}' not found. Creating it...")
        s3_client.create_bucket(Bucket=bucket_name)


    try:
        s3_client.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded to MinIO bucket '{bucket_name}' as '{object_name}'")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

def download_from_minio(bucket_name: str, object_name: str, file_path: str):
    s3_client = boto3.client('s3', endpoint_url=f'http://{MINIO_ENDPOINT}',
                             aws_access_key_id=MINIO_ACCESS_KEY,
                             aws_secret_access_key=MINIO_SECRET_KEY)
    try:
        s3_client.download_file(bucket_name, object_name, file_path)
        print(f"File downloaded from MinIO bucket '{bucket_name}'")
    except Exception as e:
        print(f"Error: {e}")

def run_etl():
    file_path = "/opt/airflow/data/imdb_top_5000.csv"
    conn_string = "postgresql+psycopg2://airflow:airflow@postgres:5432/airflow"
    table_name = "imdb_movies"

    # upload
    upload_to_minio(file_path, MINIO_BUCKET, 'imdb_top_5000.csv')
    
    # download
    download_from_minio(MINIO_BUCKET, 'imdb_top_5000.csv', file_path)

    df = extract_csv(file_path)

    df = transform_data(df)

    load_to_postgres(df, table_name, conn_string)

with DAG(
    dag_id="etl_imdb_pipeline_with_minio",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id="run_etl_pipeline",
        python_callable=run_etl
    )

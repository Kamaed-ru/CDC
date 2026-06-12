from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def upload_test_file():
    bucket_name = Variable.get("S3_BUCKET")

    s3_hook = S3Hook(aws_conn_id="minio_s3")

    s3_hook.load_string(
        string_data="Hello from Airflow!",
        key="test/hello.txt",
        bucket_name=bucket_name,
        replace=True,
    )

    print("File uploaded successfully")


with DAG(
    dag_id="test_minio_upload",
    start_date=datetime(2025, 12, 24),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    upload_file = PythonOperator(
        task_id="upload_file",
        python_callable=upload_test_file,
    )
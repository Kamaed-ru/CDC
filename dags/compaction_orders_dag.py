from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
import pendulum


OWNER = "omash"
DAG_ID = "compaction_orders"


conn = BaseHook.get_connection("minio_s3")

env = {
    "AWS_ACCESS_KEY_ID": conn.login,
    "AWS_SECRET_ACCESS_KEY": conn.password,
    "S3_ENDPOINT_URL": conn.extra_dejson.get("endpoint_url"),
    "AWS_DEFAULT_REGION": conn.extra_dejson.get("region_name", "ru-central1"),
    "S3_BUCKET": Variable.get("S3_BUCKET"),
}


args = {
    "owner": OWNER,
    "start_date": pendulum.now("Europe/Moscow"),
    "catchup": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}


with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule="0 * * * *",
    max_active_runs=1,
    tags=["spark", "silver", "compaction"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )

    compact_orders = DockerOperator(
        task_id="compact_orders",
        image="spark-ephemeral:3.5.7",
        command=[
            "/opt/spark/bin/spark-submit",
            "/opt/spark_jobs/compact_orders.py",
            "{{ ds }}",
        ],
        environment=env,
        auto_remove="success",
        network_mode="cdc_default",
        mount_tmp_dir=False,
        do_xcom_push=False,
        cpus=4.0,
        mem_limit="6g",
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> compact_orders >> end

from airflow import DAG
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
import pendulum


OWNER = "omash"
DAG_ID = "load_orders_to_clickhouse"


s3_conn = BaseHook.get_connection("minio_s3")
ch_conn = BaseHook.get_connection("clickhouse_default")

env = {
    "AWS_ACCESS_KEY_ID": s3_conn.login,
    "AWS_SECRET_ACCESS_KEY": s3_conn.password,
    "S3_ENDPOINT_URL": s3_conn.extra_dejson.get("endpoint_url"),
    "AWS_DEFAULT_REGION": s3_conn.extra_dejson.get(
        "region_name",
        "ru-central1"
    ),
    "S3_BUCKET": Variable.get("S3_BUCKET"),

    "CLICKHOUSE_HOST": ch_conn.host,
    "CLICKHOUSE_DB": ch_conn.schema,
    "CLICKHOUSE_USER": ch_conn.login,
    "CLICKHOUSE_PASSWORD": ch_conn.password,
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
    tags=["spark", "clickhouse", "silver"],
) as dag:

    start = EmptyOperator(
        task_id="start"
    )
    
    wait_compaction = ExternalTaskSensor(
        task_id="wait_compaction",

        external_dag_id="compaction_orders",
        external_task_id="end",

        allowed_states=[DagRunState.SUCCESS],

        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )


    load_orders = DockerOperator(
        task_id="load_orders",
        image="spark-ephemeral:3.5.7",
        command=[
            "/opt/spark/bin/spark-submit",
            "/opt/spark_jobs/from_s3_to_click.py",
            "{{ ds }}",
        ],
        environment=env,
        auto_remove="success",
        network_mode="cdc_default",
        mount_tmp_dir=False,
        do_xcom_push=False,
        cpus=2.0,
        mem_limit="4g",
    )

    end = EmptyOperator(
        task_id="end"
    )

    start >> load_orders >> end
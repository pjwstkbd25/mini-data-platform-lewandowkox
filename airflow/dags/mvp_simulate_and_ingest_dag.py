from __future__ import annotations

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DEFAULT_ARGS = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

HOST_PROJECT_DIR = os.environ.get(
    "PROJECT_DIR",
    "/Users/olga/Desktop/WPBD_repo/mini-data-platform-lewandowkox"
)

with DAG(
    dag_id="mvp_simulate_and_ingest",
    description="MVP: generate CSV + ingest to Postgres (via DockerOperator)",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@hourly",  
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["mvp", "ingest"],
) as dag:

    generate_csv = DockerOperator(
        task_id="generate_csv",
        image="python:3.11-slim",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        command=(
            "bash -lc "
            "'pip install -q pandas faker && python /app/generate_csv.py'"
        ),
        mounts=[Mount(source=HOST_PROJECT_DIR, target="/app", type="bind")],
    )


    ingest_to_postgres = DockerOperator(
    task_id="ingest_to_postgres",
    image="python:3.11-slim",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    auto_remove="success",
    mount_tmp_dir=False,
    command="bash -lc 'pip install -q pandas sqlalchemy psycopg2-binary && python /app/csv_ingestion.py'",
    mounts=[Mount(source=HOST_PROJECT_DIR, target="/app", type="bind")],
)



    generate_csv >> ingest_to_postgres

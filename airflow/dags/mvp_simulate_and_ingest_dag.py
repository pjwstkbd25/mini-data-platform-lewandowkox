from __future__ import annotations

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

DEFAULT_ARGS = {"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=2)}

# HOST_PROJECT_DIR dostajesz z docker-compose (PROJECT_DIR: ${PWD})
HOST_PROJECT_DIR = os.environ.get("PROJECT_DIR", "/opt/airflow/jobs")
APP_DIR = "/app"  # ścieżka wewnątrz kontenera python

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
        auto_remove=True,
        command=f"bash -lc \"cd {APP_DIR} && python generate_csv.py\"",
        mounts=[Mount(source=HOST_PROJECT_DIR, target=APP_DIR, type="bind")],
    )

    ingest_to_postgres = DockerOperator(
        task_id="ingest_to_postgres",
        image="python:3.11-slim",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        auto_remove=True,
        # MVP: doinstaluj deps w locie (żeby działało bez budowania image)
        command=f"bash -lc \"cd {APP_DIR} && pip install -q psycopg2-binary pandas && python csv_ingestion.py\"",
        mounts=[Mount(source=HOST_PROJECT_DIR, target=APP_DIR, type="bind")],
    )

    generate_csv >> ingest_to_postgres

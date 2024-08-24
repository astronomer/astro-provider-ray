from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

from ray_provider.operators.ray import SubmitRayJob

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=0),
}

CONN_ID = "ray_conn"
RAY_SPEC = Path(__file__).parent / "scripts/ray.yaml"
FOLDER_PATH = Path(__file__).parent / "ray_scripts"
RAY_RUNTIME_ENV = {"working_dir": str(FOLDER_PATH)}

dag = DAG(
    "Single_Operator",
    default_args=default_args,
    description="Setup Ray cluster and submit a job",
    schedule=None,
)

submit_ray_job = SubmitRayJob(
    task_id="SubmitRayJob",
    conn_id=CONN_ID,
    entrypoint="python script.py",
    runtime_env=RAY_RUNTIME_ENV,
    num_cpus=1,
    num_gpus=0,
    memory=0,
    resources={},
    xcom_task_key="SubmitRayJob.dashboard",
    ray_cluster_yaml=RAY_SPEC,
    fetch_logs=True,
    wait_for_completion=True,
    job_timeout_seconds=600,
    poll_interval=5,
    dag=dag,
)


# Create ray cluster and submit ray job
submit_ray_job

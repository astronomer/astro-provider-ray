from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

from ray_provider.operators.ray import DeleteRayCluster, SetupRayCluster, SubmitRayJob

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=0),
}


RAY_SPEC = Path(__file__).parent / "scripts/ray.yaml"
FOLDER_PATH = Path(__file__).parent / "ray_scripts"

dag = DAG(
    "Setup_Teardown",
    default_args=default_args,
    description="Setup Ray cluster and submit a job",
    schedule=None,
)

setup_cluster = SetupRayCluster(
    task_id="SetupRayCluster",
    conn_id="ray_conn",
    ray_cluster_yaml=str(RAY_SPEC),
    use_gpu=False,
    update_if_exists=False,
    dag=dag,
)

submit_ray_job = SubmitRayJob(
    task_id="SubmitRayJob",
    conn_id="ray_conn",
    entrypoint="python script.py",
    runtime_env={"working_dir": str(FOLDER_PATH)},
    num_cpus=1,
    num_gpus=0,
    memory=0,
    resources={},
    fetch_logs=True,
    wait_for_completion=True,
    job_timeout_seconds=600,
    xcom_task_key="SetupRayCluster.dashboard",
    poll_interval=5,
    dag=dag,
)

delete_cluster = DeleteRayCluster(
    task_id="DeleteRayCluster",
    conn_id="ray_conn",
    ray_cluster_yaml=str(RAY_SPEC),
    use_gpu=False,
    dag=dag,
)

# Create ray cluster and submit ray job
setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
setup_cluster >> delete_cluster

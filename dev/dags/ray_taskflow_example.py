import os
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from ray_provider.decorators import ray

CONN_ID = "ray_conn"
RAY_SPEC_FILENAME = os.getenv("RAY_SPEC_FILENAME", "ray.yaml")
RAY_SPEC = Path(__file__).parent / "scripts" / RAY_SPEC_FILENAME

FOLDER_PATH = Path(__file__).parent / "ray_scripts"
RAY_TASK_CONFIG = {
    "conn_id": CONN_ID,
    "runtime_env": {"working_dir": str(FOLDER_PATH), "pip": ["numpy"]},
    "num_cpus": 1,
    "num_gpus": 0,
    "memory": 0,
    "poll_interval": 5,
    "ray_cluster_yaml": str(RAY_SPEC),
    "xcom_task_key": "dashboard",
}


@dag(
    dag_id="Ray_Taskflow_Example",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ray", "example"],
)
def ray_taskflow_dag():

    @task
    def generate_data():
        return [1, 2, 3]

    @ray.task(config=RAY_TASK_CONFIG)
    def process_data_with_ray(data):
        import numpy as np
        import ray

        @ray.remote
        def square(x):
            return x**2

        ray.init()
        data = np.array(data)
        futures = [square.remote(x) for x in data]
        results = ray.get(futures)
        mean = np.mean(results)
        print(f"Mean of this population is {mean}")
        return mean

    data = generate_data()
    process_data_with_ray(data)


ray_example_dag = ray_taskflow_dag()

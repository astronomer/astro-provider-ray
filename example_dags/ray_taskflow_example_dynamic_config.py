from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from ray_provider.decorators.ray import ray


def generate_config(custom_memory: int, **context):

    CONN_ID = "ray_conn"
    RAY_SPEC = Path(__file__).parent / "scripts/ray.yaml"
    FOLDER_PATH = Path(__file__).parent / "ray_scripts"

    return {
        "conn_id": CONN_ID,
        "runtime_env": {"working_dir": str(FOLDER_PATH), "pip": ["numpy"]},
        "num_cpus": 1,
        "num_gpus": 0,
        "memory": custom_memory,
        "poll_interval": 5,
        "ray_cluster_yaml": str(RAY_SPEC),
        "xcom_task_key": "dashboard",
        "execution_date": str(context.get("execution_date")),
    }


@dag(
    dag_id="Ray_Taskflow_Example_Dynamic_Config",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ray", "example"],
)
def ray_taskflow_dag():
    @task
    def generate_data():
        return [1, 2, 3]

    @ray.task(config=generate_config, custom_memory=1024)
    def process_data_with_ray(data):
        import numpy as np
        import ray

        @ray.remote
        def square(x):
            return x**2

        data = np.array(data)
        futures = [square.remote(x) for x in data]
        results = ray.get(futures)
        mean = np.mean(results)
        print(f"Mean of this population is {mean}")
        return mean

    data = generate_data()
    process_data_with_ray(data)


ray_example_dag = ray_taskflow_dag()

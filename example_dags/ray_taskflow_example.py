from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task

from ray_provider.decorators.ray import ray

RAY_TASK_CONFIG = {
    "conn_id": "ray_conn",
    "runtime_env": {
        "working_dir": Path(__file__).parent / "ray_scripts",
        "pip": ["numpy"],
    },
    "num_cpus": 1,
    "num_gpus": 0,
    "memory": 0,
    "poll_interval": 5,
}


@dag(
    dag_id="ray_taskflow_example",
    start_date=datetime(2023, 1, 1),
    schedule=timedelta(days=1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ray", "example"],
)
def ray_taskflow_dag():

    @task
    def generate_data():
        import numpy as np

        return np.random.rand(100).tolist()

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
        print(f"Mean of squared values: {mean}")
        return mean

    data = generate_data()
    process_data_with_ray(data)


ray_example_dag = ray_taskflow_dag()

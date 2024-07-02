import os
from datetime import datetime, timedelta

from airflow.decorators import dag

from ray_provider.decorators.ray import ray_task

CLUSTERNAME = "RayCluster"
REGION = "us-east-2"
K8SPEC = "/usr/local/airflow/dags/scripts/k8-gpu.yaml"
RAY_SPEC = "/usr/local/airflow/dags/scripts/ray-gpu.yaml"
RAY_SVC = "/usr/local/airflow/dags/scripts/ray-service.yaml"
RAY_RUNTIME_ENV = {"working_dir": "/usr/local/airflow/dags/ray_scripts"}
kubeconfig_directory = f"/tmp/airflow_kubeconfigs/{REGION}/{CLUSTERNAME}/"
os.makedirs(kubeconfig_directory, exist_ok=True)  # Ensure the directory exists
KUBECONFIG_PATH = os.path.join(kubeconfig_directory, "kubeconfig.yaml")

RAY_TASK_CONFIG = {
    "entrypoint": "python script.py",
    "runtime_env": {"working_dir": "/usr/local/airflow/dags/ray_scripts"},
    "num_cpus": 1,
    "num_gpus": 0,
    "memory": 0,
}


@dag(
    "ray_decorator",
    start_date=datetime(2024, 3, 26),
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval=None,
    description="Setup EKS cluster with eksctl and run a gpu task on the ray cluster",
)
def taskflow_gpu_task():

    @ray_task(config=RAY_TASK_CONFIG, node_group=None)
    def ray_decorator_task(number):

        import ray

        @ray.remote
        def hello_world():
            return f"{number} -- hello world"

        ray.init()
        print(ray.get(hello_world.remote()))

    ray_decorator_task(123)


gpu_dag = taskflow_gpu_task()

from datetime import datetime, timedelta

from airflow import DAG

from ray_provider.operators.ray import DeleteRayCluster, SetupRayCluster, SubmitRayJob

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

CLUSTERNAME = "RayCluster"
REGION = "us-east-2"
K8SPEC = "/usr/local/airflow/dags/scripts/k8.yaml"
RAY_SPEC = "/usr/local/airflow/dags/scripts/ray.yaml"
RAY_SVC = "/usr/local/airflow/dags/scripts/ray-service.yaml"
RAY_RUNTIME_ENV = {"working_dir": "/usr/local/airflow/dags/ray_scripts"}

dag = DAG(
    "start_ray_cluster",
    default_args=default_args,
    description="Setup EKS cluster with eksctl and deploy KubeRay operator",
    schedule_interval="@daily",
)

setup_cluster = SetupRayCluster(
    task_id="SetupRayCluster",
    cluster_name=CLUSTERNAME,
    ray_namespace="ray",
    ray_cluster_yaml=RAY_SPEC,
    ray_svc_yaml=RAY_SVC,
    ray_gpu=False,
    env={},
    dag=dag,
)

submit_ray_job = SubmitRayJob(
    task_id="SubmitRayJob",
    host="{{ task_instance.xcom_pull(task_ids='RayClusterOperator', key='dashboard') }}",
    entrypoint="python script.py",
    runtime_env=RAY_RUNTIME_ENV,
    num_cpus=1,
    num_gpus=0,
    memory=0,
    resources={},
    dag=dag,
)

delete_cluster = DeleteRayCluster(
    task_id="DeleteRayCluster",
    cluster_name=CLUSTERNAME,
    dag=dag,
)

# Create ray cluster and submit ray job
setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
setup_cluster >> delete_cluster

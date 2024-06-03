
import os
from airflow import DAG
from datetime import datetime, timedelta
from ray_provider.operators.kuberay import RayClusterOperator,SubmitRayJob

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

CLUSTERNAME = 'RayCluster'
REGION = 'us-east-2'
K8SPEC = '/usr/local/airflow/dags/scripts/k8.yaml'
RAY_SPEC = '/usr/local/airflow/dags/scripts/ray.yaml'
RAY_SVC = '/usr/local/airflow/dags/scripts/ray-service.yaml'
RAY_RUNTIME_ENV = {"working_dir": '/usr/local/airflow/dags/ray_scripts'}
kubeconfig_directory = f"/tmp/airflow_kubeconfigs/{REGION}/{CLUSTERNAME}/"
os.makedirs(kubeconfig_directory, exist_ok=True)  # Ensure the directory exists
KUBECONFIG_PATH = os.path.join(kubeconfig_directory, "kubeconfig.yaml")

dag = DAG(
    'start_ray_cluster',
    default_args=default_args,
    description='Setup EKS cluster with eksctl and deploy KubeRay operator',
    schedule_interval='@daily',
)

ray_cluster = RayClusterOperator(task_id="RayClusterOperator",
                                 cluster_name=CLUSTERNAME,
                                 region=REGION,
                                 ray_namespace="ray",
                                 ray_cluster_yaml=RAY_SPEC,
                                 ray_svc_yaml= RAY_SVC,
                                 kubeconfig=KUBECONFIG_PATH,
                                 ray_gpu=False,
                                 env = {},
                                 dag = dag,)
submit_ray_job = SubmitRayJob(task_id="SubmitRayJob",
                              host = "{{ task_instance.xcom_pull(task_ids='RayClusterOperator', key='dashboard') }}",
                              entrypoint='python script.py',
                              runtime_env= RAY_RUNTIME_ENV,
                              num_cpus=1,
                              num_gpus=0,
                              memory=0,
                              resources={},
                              dag = dag,)

# Create ray cluster and submit ray job 
ray_cluster >> submit_ray_job

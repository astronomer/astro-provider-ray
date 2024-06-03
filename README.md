# airflow-provider-kuberay

This repository provides a set of tools for integrating Ray with Apache Airflow, enabling the orchestration of Ray jobs within Airflow workflows. It includes a decorator, two operators, and one trigger specifically designed for managing and monitoring Ray jobs and services.

### Components

#### Decorators
- **_RayDecoratedOperator**: This decorator allows you to submit a job to a Ray cluster. It simplifies the integration process by decorating your task functions to work seamlessly with Ray.

#### Operators
- **RayClusterOperator**: This operator sets up a Ray cluster. It requires access to the kubeconfig file, the Ray cluster specification, and the services specification. For an example, refer to the [example_dags](https://github.com/astronomer/airflow-provider-kuberay/tree/main/ray_provider/example_dags) folder.
- **SubmitRayJob**: This operator is used to submit a job to a Ray cluster using a specified host name. It facilitates scheduling Ray jobs to execute at defined intervals.

#### Triggers
- **RayJobTrigger**: This trigger monitors the status of asynchronous jobs submitted via the `SubmitRayJob` operator. It ensures that the Airflow task waits until the job is completed before proceeding with the next step in the DAG.

### Example Usage

The provided `start_ray_cluster.py` script demonstrates how to configure and use the `RayClusterOperator` and `SubmitRayJob` operators within an Airflow DAG:

```python
import os
from airflow import DAG
from datetime import datetime, timedelta
from ray_provider.operators.kuberay import RayClusterOperator, SubmitRayJob

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

ray_cluster = RayClusterOperator(
    task_id="RayClusterOperator",
    cluster_name=CLUSTERNAME,
    region=REGION,
    ray_namespace="ray",
    ray_cluster_yaml=RAY_SPEC,
    ray_svc_yaml=RAY_SVC,
    kubeconfig=KUBECONFIG_PATH,
    ray_gpu=False,
    env={},
    dag=dag,
)

submit_ray_job = SubmitRayJob(
    task_id="SubmitRayJob",
    host="{{ task_instance.xcom_pull(task_ids='RayClusterOperator', key='dashboard') }}",
    entrypoint='python script.py',
    runtime_env=RAY_RUNTIME_ENV,
    num_cpus=1,
    num_gpus=0,
    memory=0,
    resources={},
    dag=dag,
)

# Create Ray cluster and submit Ray job
ray_cluster >> submit_ray_job
```

### Changelog
_________

We follow [Semantic Versioning](https://semver.org/) for releases.
Check [CHANGELOG.rst](https://github.com/astronomer/airflow-provider-kuberay/blob/main/CHANGELOG.rst)
for the latest changes.


### Contributing Guide
__________________

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the [Contributing Guide](https://github.com/astronomer/airflow-provider-kuberay/blob/main/CONTRIBUTING.rst)
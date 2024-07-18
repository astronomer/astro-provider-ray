# astro-provider-ray

This repository provides a set of tools for integrating Ray with Apache Airflow, enabling the orchestration of Ray jobs within Airflow workflows. It includes a decorator, two operators, and one trigger specifically designed for managing and monitoring Ray jobs and services.

### Components

#### Decorators
- **_RayDecoratedOperator**: This decorator allows you to submit a job to a Ray cluster. It simplifies the integration process by decorating your task functions to work seamlessly with Ray.

#### Operators

- **SubmitRayJob**: This operator is used to submit a job to a Ray cluster using a specified host name. It facilitates scheduling Ray jobs to execute at defined intervals.

#### Triggers
- **RayJobTrigger**: This trigger monitors the status of asynchronous jobs submitted via the `SubmitRayJob` operator. It ensures that the Airflow task waits until the job is completed before proceeding with the next step in the DAG.

### Compatibility

These operators have been tested with the below versions. They will most likely be compatible with future versions but have not yet been tested.

| Python Version | Airflow Version | Ray Version |
|----------------|-----------------|-------------|
| 3.11           | 2.9.0           | 2.23.0      |


### Compatibility

These operators have been tested with the below versions. They will most likely be compatible with future versions but have not yet been tested.

| Python Version | Airflow Version | Ray Version |
|----------------|-----------------|-------------|
| 3.11           | 2.9.0           | 2.23.0      |


### Example Usage

The provided `setup_teardown.py` script demonstrates how to configure and use the `SetupRayCluster`, `DeleteRayCluster` and the `SubmitRayJob` operators within an Airflow DAG:

```python
import os
from airflow import DAG
from datetime import datetime, timedelta
from ray_provider.operators.ray import SetupRayCluster, DeleteRayCluster, SubmitRayJob

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=0),
}

CLUSTERNAME = "RayCluster"
REGION = "us-east-2"
K8SPEC = "/usr/local/airflow/dags/scripts/k8.yaml"
RAY_SPEC = "/usr/local/airflow/dags/scripts/ray.yaml"
RAY_SVC = "/usr/local/airflow/dags/scripts/ray-service.yaml"
RAY_RUNTIME_ENV = {"working_dir": "/usr/local/airflow/dags/ray_scripts"}

dag = DAG(
    "Setup_Teardown",
    default_args=default_args,
    description="Setup Ray cluster and submit a job",
    schedule_interval=None,
)

setup_cluster = SetupRayCluster(
    task_id="SetupRayCluster",
    conn_id="ray_conn",
    ray_cluster_yaml=RAY_SPEC,
    ray_svc_yaml=RAY_SVC,
    use_gpu=False,
    dag=dag,
)

submit_ray_job = SubmitRayJob(
    task_id="SubmitRayJob",
    conn_id="ray_conn",
    entrypoint="python script.py",
    runtime_env=RAY_RUNTIME_ENV,
    num_cpus=1,
    num_gpus=0,
    memory=0,
    resources={},
    xcom_task_key="SetupRayCluster.dashboard",
    dag=dag,
)

delete_cluster = DeleteRayCluster(
    task_id="DeleteRayCluster",
    conn_id="ray_conn",
    ray_cluster_yaml=RAY_SPEC,
    ray_svc_yaml=RAY_SVC,
    use_gpu=False,
    dag=dag,
)

# Create ray cluster and submit ray job
setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
setup_cluster >> delete_cluster
```

### Changelog
_________

We follow [Semantic Versioning](https://semver.org/) for releases.
Check [CHANGELOG.rst](https://github.com/astronomer/astro-provider-ray/blob/main/CHANGELOG.rst)
for the latest changes.


### Contributing Guide
__________________

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the [Contributing Guide](https://github.com/astronomer/astro-provider-ray/blob/main/CONTRIBUTING.rst)

# astro-provider-ray

This repository provides tools for integrating [Apache Airflow®](https://airflow.apache.org/) with Ray, enabling the orchestration of Ray jobs within Airflow workflows. It includes a decorator, two operators, and one trigger designed to efficiently manage and monitor Ray jobs and services.



## Table of Contents

- [Components](#components)
- [Compatibility](#compatibility)
- [Example Usage](#example-usage)
- [Contact the Devs](#contact-the-devs)
- [Changelog](#changelog)
- [Contributing Guide](#contributing-guide)

## Components

### Hooks

- **RayHook**: Sets up methods needed to run operators and decorators, working with the 'Ray' connection type to manage Ray clusters and submit jobs.

### Decorators

- **_RayDecoratedOperator**: Simplifies integration by decorating task functions to work seamlessly with Ray.

### Operators

- **SetupRayCluster**: (Placeholder for cluster setup details)
- **DeleteRayCluster**: (Placeholder for cluster deletion details)
- **SubmitRayJob**: Submits jobs to a Ray cluster using a specified host name.

### Triggers

- **RayJobTrigger**: Monitors asynchronous job execution submitted via `SubmitRayJob` or using the `@task.ray()` decorator.

## Compatibility

These operators have been tested with the below versions. They will most likely be compatible with future versions but have not yet been tested.

| Python Version | Airflow Version | Ray Version |
|----------------|-----------------|-------------|
| 3.11           | 2.9.0           | 2.23.0      |


## Example Usage

### 1. Setting up the connection

#### For SubmitRayJob operator (using an existing Ray cluster)

- Connection Type: "Ray"
- Connection ID: e.g., "ray_conn"
- Ray dashboard URL: URL of the Ray dashboard
- Other optional fields: Cookies, Metadata, Headers, Verify SSL

#### For SetupRayCluster and DeleteRayCluster operators

- Connection Type: "Ray"
- Connection ID: e.g., "ray_k8s_conn"
- Kube config path OR Kube config content (JSON format)
- Namespace: The k8 namespace where your cluster must be created. If not provided, "default" is used
- Optional fields: Cluster context, Disable SSL, Disable TCP keepalive

### 2. Setting up the Ray cluster spec

Create a YAML file defining your Ray cluster configuration. Example:

```yaml
# ray.yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-complete
spec:
  rayVersion: "2.10.0"
  enableInTreeAutoscaling: true
  headGroupSpec:
    serviceType: LoadBalancer
    rayStartParams:
      dashboard-host: "0.0.0.0"
      block: "true"
    template:
      metadata:
        labels:
          ray-node-type: head
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray-ml:latest
          resources:
            limits:
              cpu: 4
              memory: 8Gi
            requests:
              cpu: 4
              memory: 8Gi
          lifecycle:
            preStop:
              exec:
                command: ["/bin/sh","-c","ray stop"]
          ports:
          - containerPort: 6379
            name: gcs
          - containerPort: 8265
            name: dashboard
          - containerPort: 10001
            name: client
          - containerPort: 8000
            name: serve
          - containerPort: 8080
            name: metrics
  workerGroupSpecs:
  - groupName: small-group
    replicas: 2
    minReplicas: 2
    maxReplicas: 5
    rayStartParams:
      block: "true"
    template:
      metadata:
      spec:
        containers:
        - name: machine-learning
          image: rayproject/ray-ml:latest
          resources:
            limits:
              cpu: 2
              memory: 4Gi
            requests:
              cpu: 2
              memory: 4Gi
```
Save this file in a location accessible to your Airflow installation, and reference it in your DAG code.

**Note:** `spec.headGroupSpec.serviceType` must be a 'LoadBalancer' to spin a service that exposes your dashboard

### 3. Code Samples

There are two main scenarios for using this provider:

#### Scenario 1: Setting up a Ray cluster on an existing Kubernetes cluster

If you have an existing Kubernetes cluster and want to install a Ray cluster on it, and then run a Ray job, you can use the `SetupRayCluster`, `SubmitRayJob`, and `DeleteRayCluster` operators. Here's an example DAG (`setup_teardown.py`):


```python
from airflow import DAG
from datetime import datetime, timedelta
from ray_provider.operators.ray import SetupRayCluster, DeleteRayCluster, SubmitRayJob

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=0),
}

CONN_ID = "ray_k8s_conn"
RAY_SPEC = "/usr/local/airflow/dags/scripts/ray.yaml"
RAY_RUNTIME_ENV = {"working_dir": "/usr/local/airflow/example_dags/ray_scripts"}

dag = DAG(
    "Setup_Teardown",
    default_args=default_args,
    description="Setup Ray cluster and submit a job",
    schedule=None,
)

setup_cluster = SetupRayCluster(
    task_id="SetupRayCluster",
    conn_id=CONN_ID,
    ray_cluster_yaml=RAY_SPEC,
    use_gpu=False,
    update_if_exists=False,
    dag=dag,
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
    fetch_logs=True,
    wait_for_completion=True,
    job_timeout_seconds=600,
    xcom_task_key="SetupRayCluster.dashboard",
    poll_interval=5,
    dag=dag,
)

delete_cluster = DeleteRayCluster(
    task_id="DeleteRayCluster",
    conn_id=CONN_ID,
    ray_cluster_yaml=RAY_SPEC,
    use_gpu=False,
    dag=dag,
)

# Create ray cluster and submit ray job
setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
setup_cluster >> delete_cluster
```

#### Scenario 2: Using an existing Ray cluster

If you already have a Ray cluster set up, you can use the `SubmitRayJob` operator or `task.ray()` decorator to submit jobs directly.

In the below example(`ray_taskflow_example.py`), the `@task.ray` decorator is used to define a task that will be executed on the Ray cluster.

```python
from airflow.decorators import dag, task as airflow_task
from datetime import datetime, timedelta
from ray_provider.decorators.ray import task

RAY_TASK_CONFIG = {
    "conn_id": "ray_conn",
    "runtime_env": {
        "working_dir": "/usr/local/airflow/dags/ray_scripts",
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
    schedule_interval=timedelta(days=1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ray", "example"],
)
def ray_taskflow_dag():

    @airflow_task
    def generate_data():
        import numpy as np

        return np.random.rand(100).tolist()

    @task.ray(config=RAY_TASK_CONFIG)
    def process_data_with_ray(data):
        import ray
        import numpy as np

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
```


Remember to adjust file paths, connection IDs, and other specifics according to your setup.

## Contact the devs


If you have any questions, issues, or feedback regarding the astro-provider-ray package, please don't hesitate to reach out to the development team. You can contact us through the following channels:

- **GitHub Issues**: For bug reports, feature requests, or general questions, please open an issue on our [GitHub repository](https://github.com/astronomer/astro-provider-ray/issues).
- **Slack Channel**: Join our community Slack channel [#airflow-ray](https://astronomer-community.slack.com/archives/C01234567) for real-time discussions and support.

We appreciate your input and are committed to improving this package to better serve the community.



## Changelog
_________

We follow [Semantic Versioning](https://semver.org/) for releases.
Check [CHANGELOG.rst](https://github.com/astronomer/astro-provider-ray/blob/main/CHANGELOG.rst)
for the latest changes.


## Contributing Guide
__________________

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the [Contributing Guide](https://github.com/astronomer/astro-provider-ray/blob/main/CONTRIBUTING.rst)

# astro-provider-ray

This repository provides a set of tools for integrating Ray with Apache Airflow, enabling the orchestration of Ray jobs within Airflow workflows. It includes a decorator, two operators, and one trigger specifically designed for managing and monitoring Ray jobs and services.

## Table of Contents
- [Components](#components)
  - [Hooks](#hooks)
  - [Decorators](#decorators)
  - [Operators](#operators)
  - [Triggers](#triggers)
- [Compatibility](#compatibility)
- [Example Usage](#example-usage)
  - [1. Setting up the connection](#1-setting-up-the-connection)
  - [2. Setting up the Ray cluster spec](#2-setting-up-the-ray-cluster-spec)
  - [3. Usage Scenarios](#3-usage-scenarios)
    - [Scenario 1: Setting up a Ray cluster on an existing Kubernetes cluster](#scenario-1-setting-up-a-ray-cluster-on-an-existing-kubernetes-cluster)
    - [Scenario 2: Using an existing Ray cluster](#scenario-2-using-an-existing-ray-cluster)
- [Contact the devs](#contact-the-devs)
- [Changelog](#changelog)
- [Contributing Guide](#contributing-guide)


### Components

#### Hooks
- **RayHook**: This hook is used to setup the methods needed to run the operators. It works in the background along with the connection of type 'Ray' to setup & delete ray clusters and also submit Ray jobs to existing clusters

#### Decorators
- **_RayDecoratedOperator**: This decorator allows you to submit a job to a Ray cluster. It simplifies the integration process by decorating your task functions to work seamlessly with Ray.

#### Operators

- **SetupRayCluster**: Placeholder -- write details on cluster setup
- **DeleteRayCluster**: Placeholder -- write details on cluster deletion
- **SubmitRayJob**: This operator is used to submit a job to a Ray cluster using a specified host name. It facilitates scheduling Ray jobs to execute at defined intervals.

#### Triggers
- **RayJobTrigger**: This trigger monitors the status of asynchronous jobs submitted via the `SubmitRayJob` operator. It ensures that the Airflow task waits until the job is completed before proceeding with the next step in the DAG.

### Compatibility

These operators have been tested with the below versions. They will most likely be compatible with future versions but have not yet been tested.

| Python Version | Airflow Version | Ray Version |
|----------------|-----------------|-------------|
| 3.11           | 2.9.0           | 2.23.0      |


### Example Usage

Before using the astro-provider-ray, you need to set up a few things:

#### 1. Setting up the connection

To use this provider, you need to set up a connection in Airflow:

1. Go to the Airflow UI and navigate to Admin -> Connections.
2. Click on "Create" to add a new connection.
3. Set the Connection Type to "Ray".
4. Fill in the following fields:
   - Connection ID: A unique identifier for this connection (e.g., "ray_conn")
   - Host: The hostname or IP address of your Ray cluster's head node
   - Port: The port number for the Ray cluster (default is usually 10001)
   - Extra: You can add any additional configuration here in JSON format

#### 2. Setting up the Ray cluster spec

For the `SetupRayCluster` and `DeleteRayCluster` operators, you need to provide a Ray cluster specification. This is typically a YAML file that defines the configuration of your Ray cluster. Here's a basic example:

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

#### 3. Usage Scenarios

There are two main scenarios for using this provider:

##### Scenario 1: Setting up a Ray cluster on an existing Kubernetes cluster

If you have an existing Kubernetes cluster and want to install a Ray cluster on it, then run a Ray job, you can use the `SetupRayCluster`, `SubmitRayJob`, and `DeleteRayCluster` operators. Here's an example DAG (`setup_teardown.py`):

The provided `setup_teardown.py` script demonstrates how to configure and use the `SetupRayCluster`, `DeleteRayCluster` and the `SubmitRayJob` operators within an Airflow DAG:

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

RAY_SPEC = "/usr/local/airflow/dags/scripts/ray.yaml"
RAY_RUNTIME_ENV = {"working_dir": "/usr/local/airflow/example_dags/ray_scripts"}

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
    use_gpu=False,
    update_if_exists=False,
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
    ray_cluster_yaml=RAY_SPEC,
    use_gpu=False,
    dag=dag,
)

# Create ray cluster and submit ray job
setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
setup_cluster >> delete_cluster
```

##### Scenario 2: Using an existing Ray cluster

If you already have a Ray cluster set up, you can use the Ray Taskflow API to submit jobs directly. Here's an example DAG (`ray_taskflow_example.py`):

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
In this example, the `@task.ray` decorator is used to define a task that will be executed on the Ray cluster.
Remember to adjust file paths, connection IDs, and other specifics according to your setup.

### Contact the devs
_________

If you have any questions, issues, or feedback regarding the astro-provider-ray package, please don't hesitate to reach out to the development team. You can contact us through the following channels:

- **GitHub Issues**: For bug reports, feature requests, or general questions, please open an issue on our [GitHub repository](https://github.com/astronomer/astro-provider-ray/issues).
- **Slack Channel**: Join our community Slack channel [#airflow-ray](https://astronomer-community.slack.com/archives/C01234567) for real-time discussions and support.

We appreciate your input and are committed to improving this package to better serve the community.



### Changelog
_________

We follow [Semantic Versioning](https://semver.org/) for releases.
Check [CHANGELOG.rst](https://github.com/astronomer/astro-provider-ray/blob/main/CHANGELOG.rst)
for the latest changes.


### Contributing Guide
__________________

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the [Contributing Guide](https://github.com/astronomer/astro-provider-ray/blob/main/CONTRIBUTING.rst)

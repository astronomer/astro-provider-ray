<h1 align="center">
  Ray provider
</h1>

<div align="center">

:books: [Docs](https://astronomer.github.io/astro-provider-ray/) &nbsp; | &nbsp; :rocket: [Getting Started](https://astronomer.github.io/astro-provider-ray/getting_started/setup.html) &nbsp; | &nbsp; :speech_balloon: [Slack](https://join.slack.com/t/apache-airflow/shared_invite/zt-2nsw28cw1-Lw4qCS0fgme4UI_vWRrwEQ) (``#airflow-ray``)&nbsp; | &nbsp; :fire: [Contribute](https://astronomer.github.io/astro-provider-ray/CONTRIBUTING.html) &nbsp;

</div>

Orchestrate your Ray jobs using [Apache Airflow®](https://airflow.apache.org/) combining Airflow's workflow management with Ray's distributed computing capabilities.

Benefits of using this provider include:
- **Integration**: Incorporate Ray jobs into Airflow DAGs for unified workflow management.
- **Distributed computing**: Use Ray's distributed capabilities within Airflow pipelines for scalable ETL, LLM fine-tuning etc.
- **Monitoring**: Track Ray job progress through Airflow's user interface.
- **Dependency management**: Define and manage dependencies between Ray jobs and other tasks in DAGs.
- **Resource allocation**: Run Ray jobs alongside other task types within a single pipeline.


## Table of Contents
- [Quickstart](#quickstart)
- [Sample DAGs](#sample-dags)
- [Getting Involved](#getting-involved)
- [Changelog](#changelog)
- [Contributing Guide](#contributing-guide)

## Quickstart
Check out the Getting Started guide in our [docs](https://astronomer.github.io/astro-provider-ray/getting_started/setup.html). Sample DAGs are available at [example_dags/](https://github.com/astronomer/astro-provider-ray/tree/main/example_dags).

## Sample DAGs

### Example 1: Using @ray.task for job life cycle
The below example showcases how to use the ``@ray.task`` decorator to manage the full lifecycle of a Ray cluster: setup, job execution, and teardown.

This approach is ideal for jobs that require a dedicated, short-lived cluster, optimizing resource usage by cleaning up after task completion

```python
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

from ray_provider.decorators.ray import ray

CONN_ID = "ray_conn"
RAY_SPEC = Path(__file__).parent / "scripts/ray.yaml"
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
```
### Example 2: Using SetupRayCluster, SubmitRayJob & DeleteRayCluster
This example shows how to use separate operators for cluster setup, job submission, and teardown, providing more granular control over the process.

This approach allows for more complex workflows involving Ray clusters.

Key Points:

- Uses SetupRayCluster, SubmitRayJob, and DeleteRayCluster operators separately.
- Allows for multiple jobs to be submitted to the same cluster before deletion.
- Demonstrates how to pass cluster information between tasks using XCom.

This method is ideal for scenarios where you need fine-grained control over the cluster lifecycle, such as running multiple jobs on the same cluster or keeping the cluster alive for a certain period.

```python
from datetime import datetime
from pathlib import Path

from airflow import DAG

from ray_provider.operators.ray import DeleteRayCluster, SetupRayCluster, SubmitRayJob

CONN_ID = "ray_conn"
RAY_SPEC = Path(__file__).parent / "scripts/ray.yaml"
FOLDER_PATH = Path(__file__).parent / "ray_scripts"

with DAG(
    "Setup_Teardown",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ray", "example"],
):

    setup_cluster = SetupRayCluster(
        task_id="SetupRayCluster",
        conn_id=CONN_ID,
        ray_cluster_yaml=str(RAY_SPEC),
        update_if_exists=False,
    )

    submit_ray_job = SubmitRayJob(
        task_id="SubmitRayJob",
        conn_id=CONN_ID,
        entrypoint="python script.py",
        runtime_env={"working_dir": str(FOLDER_PATH)},
        num_cpus=1,
        num_gpus=0,
        memory=0,
        resources={},
        fetch_logs=True,
        wait_for_completion=True,
        job_timeout_seconds=600,
        xcom_task_key="SetupRayCluster.dashboard",
        poll_interval=5,
    )

    delete_cluster = DeleteRayCluster(
        task_id="DeleteRayCluster", conn_id=CONN_ID, ray_cluster_yaml=str(RAY_SPEC)
    )

    # Create ray cluster and submit ray job
    setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
    setup_cluster >> delete_cluster
```

## Getting Involved

| Platform | Purpose | Est. Response time |
|:---:|:---:|:---:|
| [Discussion Forum](https://github.com/astronomer/astro-provider-ray/discussions) | General inquiries and discussions | < 3 days |
| [GitHub Issues](https://github.com/astronomer/astro-provider-ray/issues) | Bug reports and feature requests | < 1-2 days |
| [Slack](https://join.slack.com/t/apache-airflow/shared_invite/zt-2nsw28cw1-Lw4qCS0fgme4UI_vWRrwEQ) | Quick questions and real-time chat | 12 hrs |

## Changelog
We follow [Semantic Versioning](https://semver.org/) for releases. Check [CHANGELOG.rst](https://github.com/astronomer/astro-provider-ray/blob/main/CHANGELOG.rst) for the latest changes.

## Contributing Guide
All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview on how to contribute can be found in the [Contributing Guide](https://github.com/astronomer/astro-provider-ray/blob/main/docs/CONTRIBUTING.rst).

## License
[Apache 2.0 License](https://github.com/astronomer/astro-provider-ray/blob/main/LICENSE)

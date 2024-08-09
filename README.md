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
- [Changelog](#changelog)
- [Contributing Guide](#contributing-guide)

## Quickstart
Check out the Getting Started guide in our [docs](https://astronomer.github.io/astro-provider-ray/getting_started/setup.html). Sample DAGs are available at [example_dags/](https://github.com/astronomer/astro-provider-ray/tree/main/example_dags).

## Sample DAGs

```python
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG

from ray_provider.operators.ray import SetupRayCluster, SubmitRayJob, DeleteRayCluster

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 26),
    "retries": 1,
    "retry_delay": timedelta(minutes=0),
}


RAY_SPEC = Path(__file__).parent / "scripts/ray.yaml"
FOLDER_PATH = Path(__file__).parent / "ray_scripts"

dag = DAG(
    "Setup_Teardown",
    default_args=default_args,
    description="Setup Ray cluster and submit a job",
    schedule=None,
)

setup_cluster = SetupRayCluster(
    task_id="SetupRayCluster",
    conn_id="ray_conn",
    ray_cluster_yaml=str(RAY_SPEC),
    use_gpu=False,
    update_if_exists=False,
    dag=dag,
)

submit_ray_job = SubmitRayJob(
    task_id="SubmitRayJob",
    conn_id="ray_conn",
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
    dag=dag,
)

delete_cluster = DeleteRayCluster(
    task_id="DeleteRayCluster",
    conn_id="ray_conn",
    ray_cluster_yaml=str(RAY_SPEC),
    use_gpu=False,
    dag=dag,
)

# Create ray cluster and submit ray job
setup_cluster.as_setup() >> submit_ray_job >> delete_cluster.as_teardown()
setup_cluster >> delete_cluster
```

```python
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
    dag_id="Ray_Taskflow_Example",
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
```

## Changelog
We follow [Semantic Versioning](https://semver.org/) for releases. Check [CHANGELOG.rst](https://github.com/astronomer/astro-provider-ray/blob/main/CHANGELOG.rst) for the latest changes.

## Contributing Guide
All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview on how to contribute can be found in the [Contributing Guide](https://github.com/astronomer/astro-provider-ray/blob/main/docs/CONTRIBUTING.rst).

## License
[Apache 2.0 License](https://github.com/astronomer/astro-provider-ray/blob/main/LICENSE)

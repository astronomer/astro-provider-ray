<h1 align="center">
  Ray provider
</h1>

![CI](https://github.com/astronomer/astro-provider-ray/actions/workflows/python_ci.yaml/badge.svg)
![Release Tests](https://github.com/astronomer/astro-provider-ray/actions/workflows/release_tests.yaml/badge.svg)

Orchestrate your Ray jobs using [Apache Airflow®](https://airflow.apache.org/) combining Airflow's workflow management with Ray's distributed computing capabilities.

Benefits of using this provider include:
- **Integration**: Incorporate Ray jobs into Airflow DAGs for unified workflow management.
- **Distributed computing**: Use Ray's distributed capabilities within Airflow pipelines for scalable ETL, LLM fine-tuning etc.
- **Monitoring**: Track Ray job progress through Airflow's user interface.
- **Dependency management**: Define and manage dependencies between Ray jobs and other tasks in DAGs.
- **Resource allocation**: Run Ray jobs alongside other task types within a single pipeline.

</div>

## 📑 Resources


<div align="center">

:books: [Docs]() &nbsp; | &nbsp; :rocket: [Getting Started]() &nbsp; | &nbsp; :speech_balloon: [Slack](https://join.slack.com/t/apache-airflow/shared_invite/zt-2nsw28cw1-Lw4qCS0fgme4UI_vWRrwEQ) &nbsp; | &nbsp; :fire: [Contribute]() &nbsp;

</div>

## Table of Contents
- [Quickstart](#quickstart)
- [Contact the Devs](#contact-the-devs)
- [Changelog](#changelog)
- [Contributing Guide](#contributing-guide)

## Quickstart
Check out the Getting Started guide in our [docs](). Sample DAGs are available at `example_dags/`.

## Example Usage

```python
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
```

```python
@ray.task(config=RAY_TASK_CONFIG)
def process_data_with_ray(data):
    import ray

    @ray.remote
    def hello_world():
        return "hello world"

    print(hello_world())
```

## Contact the devs
If you have any questions, issues, or feedback regarding the astro-provider-ray package, please don't hesitate to reach out to the development team. You can contact us through the following channels:

- **GitHub Issues**: For bug reports, feature requests, or general questions, please open an issue on our [GitHub repository](https://github.com/astronomer/astro-provider-ray/issues).
- **Slack Channel**: Join Apache Airflow's [Slack](https://join.slack.com/t/apache-airflow/shared_invite/zt-2nsw28cw1-Lw4qCS0fgme4UI_vWRrwEQ). Visit `#airflow-ray` for discussions and help.

We appreciate your input and are committed to improving this package to better serve the community.

## Changelog
We follow [Semantic Versioning](https://semver.org/) for releases.

Check [CHANGELOG.rst](https://github.com/astronomer/astro-provider-ray/blob/main/CHANGELOG.rst) for the latest changes.

## Contributing Guide
All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview on how to contribute can be found in the [Contributing Guide](https://github.com/astronomer/astro-provider-ray/blob/main/CONTRIBUTING.rst).

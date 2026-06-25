## ⚠️ Discontinuation of project

> This project is no longer actively maintained by Astronomer. Development has been paused and we are
> not accepting new contributions, bug fixes or releases. The code is still here for you to explore,
> fork and adapt under the terms of its license. Please note that it may not work with the latest
> dependencies or platforms, and it could contain security vulnerabilities. Astronomer can't offer
> guarantees or warranties for its use.
>
> **Google Cloud alternative (partial):** If you run Ray on Google Cloud, the official Apache Airflow
> [Google provider](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/index.html)
> (`apache-airflow-providers-google`) ships Ray operators that cover a subset of what this provider does:
> - Ray cluster lifecycle on Vertex AI — [`CreateRayClusterOperator`, `ListRayClustersOperator`, `GetRayClusterOperator`, `UpdateRayClusterOperator`, `DeleteRayClusterOperator`](https://github.com/apache/airflow/blob/main/providers/google/src/airflow/providers/google/cloud/operators/vertex_ai/ray.py)
> - Ray job management — [`RaySubmitJobOperator`, `RayStopJobOperator`, `RayDeleteJobOperator`, `RayGetJobInfoOperator`, `RayListJobsOperator`](https://github.com/apache/airflow/blob/main/providers/google/src/airflow/providers/google/cloud/operators/ray.py)
>
> This provider is Kubernetes-generic, so the Google operators are a Google Cloud-specific alternative,
> not a drop-in replacement.
>
> If you're interested in adopting or stewarding this project, we'd be happy to chat — reach us at
> oss@astronomer.io. Thanks for being part of the open-source journey and helping keep great ideas alive!

---

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

https://github.com/astronomer/astro-provider-ray/blob/bd6d847818be08fae78bc1e4c9bf3334adb1d2ee/example_dags/ray_taskflow_example.py#L1-L57

### Example 2: Using SetupRayCluster, SubmitRayJob & DeleteRayCluster
This example shows how to use separate operators for cluster setup, job submission, and teardown, providing more granular control over the process.

This approach allows for more complex workflows involving Ray clusters.

Key Points:

- Uses SetupRayCluster, SubmitRayJob, and DeleteRayCluster operators separately.
- Allows for multiple jobs to be submitted to the same cluster before deletion.
- Demonstrates how to pass cluster information between tasks using XCom.

This method is ideal for scenarios where you need fine-grained control over the cluster lifecycle, such as running multiple jobs on the same cluster or keeping the cluster alive for a certain period.

https://github.com/astronomer/astro-provider-ray/blob/bd6d847818be08fae78bc1e4c9bf3334adb1d2ee/example_dags/setup-teardown.py#L1-L44

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

## Privacy Notice
This project follows `Astronomer's Privacy Policy <https://www.astronomer.io/privacy/>`_

Welcome to the Ray provider documentation!
======================================

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Contents:

   Home <self>
   Getting started <getting_started/setup>
   Code Samples <getting_started/code_samples>
   API Reference <api/ray_provider>
   Contributing <CONTRIBUTING>

This repository contains modules for integrating `Apache Airflow®`_ with Ray, enabling the orchestration of Ray jobs from Airflow DAGs. It includes a decorator, two operators, and one trigger designed to efficiently manage and monitor Ray jobs and services.

Benefits of using this provider include:

- **Integration**: Incorporate Ray jobs into Airflow DAGs for unified workflow management.
- **Distributed computing**: Use Ray's distributed capabilities within Airflow pipelines for scalable ETL, LLM fine-tuning etc.
- **Monitoring**: Track Ray job progress through Airflow's user interface.
- **Dependency management**: Define and manage dependencies between Ray jobs and other tasks in Airflow DAGs.
- **Resource allocation**: Run Ray jobs alongside other task types within a single pipeline.

.. _Apache Airflow®: https://airflow.apache.org/

Table of Contents
-----------------

- `Quickstart`_
- `What is the Ray provider?`_
- `Components`_
- `Getting Involved`_
- `Changelog`_
- `Contributing Guide`_

Quickstart
----------

See the :doc:`Getting Started <getting_started/setup>` page for detailed instructions on how to begin using the provider.

Why use Airflow with Ray?
-------------------------

Value creation from data in an enterprise environment involves two crucial components:

- Data Engineering (ETL/ELT/Infrastructure Management)
- Data Science (ML/AI)

While Airflow excels at orchestrating both, data engineering and data science related tasks through its extensive provider ecosystem, it often relies on external systems when dealing with large-scale (100s GB to PB scale) data and compute (GPU) requirements, such as fine-tuning & deploying LLMs etc.

Ray is a particularly powerful platform for handling large scale computations and this provider makes it very straightforward to orchestrate Ray jobs from Airflow.

.. image:: _static/architecture.png
   :alt: Alternative text for the image
   :align: center
   :width: 499
   :height: 561

The architecture diagram above shows that we can run both, Airflow and Ray side by side on Kubernetes to leverage the best of both worlds. Airflow can be used to orchestrate Ray jobs and services, while Ray can be used to run distributed computations.

Use Cases
^^^^^^^^^
- **Scalable ETL**: Orchestrate and monitor Ray jobs to perform distributed ETL for heavy data loads on on-demand compute clusters using the Ray Data library.
- **Model Training**: Schedule model training or fine-tuning jobs on flexible cadences (daily/weekly/monthly). Benefits include:

  * Optimize resource utilization by scheduling Ray jobs during cost-effective periods
  * Trigger model refresh based on changing business conditions or data trends

- **Model Inference**: Inference of trained or fine-tuned models can be handled in two ways:

  * **Batch Inference** jobs can be incorporated into Airflow DAGs for unified workflow management and monitoring
  * **Real time** models (or online models) that use Ray Serve can be deployed using the same operators with ``wait_for_completion=False``

- **Model Ops**: Leverage Airflow tasks following Ray job tasks for model management

  * Deploy models to a model registry
  * Perform champion-challenger analysis
  * Execute other model lifecycle management tasks


Components
----------

Decorators
^^^^^^^^^^
- **@ray.task()**: Simplifies integration by decorating task functions to work seamlessly with Ray.

Operators
^^^^^^^^^
- **SetupRayCluster**: Sets up or Updates a ray cluster on kubernetes using a kubeconfig input provided through the Ray connection
- **DeleteRayCluster**: Deletes an existing Ray cluster on kubernetes using the same Ray connection
- **SubmitRayJob**: Submits jobs to a Ray cluster using a specified host name and monitors its execution

Hooks
^^^^^
- **RayHook**: Sets up methods needed to run operators and decorators, working with the 'Ray' connection type to manage Ray clusters and submit jobs.

Triggers
^^^^^^^^
- **RayJobTrigger**: Monitors asynchronous job execution submitted via the ``SubmitRayJob`` operator or using the ``@ray.task()`` decorator and prints real-time logs to the the Airflow UI


Getting Involved
----------------
.. list-table::
   :widths: 25 50 25
   :header-rows: 1

   * - Platform
     - Purpose
     - Estimated Response Time
   * - `Discussion Forum`_
     - General inquiries and discussions
     - < 3 day
   * - `GitHub Issues`_
     - Bug reports and feature requests
     - < 1-2 days
   * - `Slack`_
     - Quick questions and real-time chat. Join (#airflow-ray)
     - < 12 hrs

.. _`Discussion Forum`: https://github.com/astronomer/astro-provider-ray/discussions
.. _`GitHub Issues`: https://github.com/astronomer/astro-provider-ray/issues
.. _`Slack`: https://join.slack.com/t/apache-airflow/shared_invite/zt-2nsw28cw1-Lw4qCS0fgme4UI_vWRrwEQ

Changelog
---------

We follow `Semantic Versioning`_ for releases.
Check `CHANGELOG.rst`_ for the latest changes.

.. _Semantic Versioning: https://semver.org/
.. _CHANGELOG.rst: https://github.com/astronomer/astro-provider-ray/blob/main/CHANGELOG.rst

Contributing Guide
------------------

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the `Contributing Guide`_

.. _Contributing Guide: https://github.com/astronomer/astro-provider-ray/blob/main/docs/CONTRIBUTING.rst

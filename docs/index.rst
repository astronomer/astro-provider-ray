Welcome to astro-provider-ray documentation!
===================================================

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   Home <self>
   API Reference <api/ray_provider>
   Contributing <CONTRIBUTING>
   Changelog <CHANGELOG>
   Code of Conduct <CODE_OF_CONDUCT>

This repository provides tools for integrating `Apache Airflow®`_ with Ray, enabling the orchestration of Ray jobs within Airflow workflows. It includes a decorator, two operators, and one trigger designed to efficiently manage and monitor Ray jobs and services.

.. _Apache Airflow®: https://airflow.apache.org/

Table of Contents
-----------------

- `Components`_
- `Example Usage`_
- `Contact the Devs`_
- `Changelog`_
- `Contributing Guide`_

Components
----------

Hooks
^^^^^
- **RayHook**: Sets up methods needed to run operators and decorators, working with the 'Ray' connection type to manage Ray clusters and submit jobs.

Decorators
^^^^^^^^^^
- **_RayDecoratedOperator**: Simplifies integration by decorating task functions to work seamlessly with Ray.

Operators
^^^^^^^^^
- **SetupRayCluster**: (Placeholder for cluster setup details)
- **DeleteRayCluster**: (Placeholder for cluster deletion details)
- **SubmitRayJob**: Submits jobs to a Ray cluster using a specified host name.

Triggers
^^^^^^^^
- **RayJobTrigger**: Monitors asynchronous job execution submitted via ``SubmitRayJob`` or using the ``@task.ray()`` decorator.


5. Code Samples
^^^^^^^^^^^^^^^

There are two main scenarios for using this provider:

Scenario 1: Setting up a Ray cluster on an existing Kubernetes cluster
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you have an existing Kubernetes cluster and want to install a Ray cluster on it, and then run a Ray job, you can use the ``SetupRayCluster``, ``SubmitRayJob``, and ``DeleteRayCluster`` operators. Here's an example DAG (``setup_teardown.py``):

.. literalinclude:: ../example_dags/setup-teardown.py

Scenario 2: Using an existing Ray cluster
"""""""""""""""""""""""""""""""""""""""""

If you already have a Ray cluster set up, you can use the ``SubmitRayJob`` operator or ``task.ray()`` decorator to submit jobs directly.

In the below example(``ray_taskflow_example.py``), the ``@task.ray`` decorator is used to define a task that will be executed on the Ray cluster.

.. code-block:: python

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

Remember to adjust file paths, connection IDs, and other specifics according to your setup.

Contact the devs
----------------

If you have any questions, issues, or feedback regarding the astro-provider-ray package, please don't hesitate to reach out to the development team. You can contact us through the following channels:

- **GitHub Issues**: For bug reports, feature requests, or general questions, please open an issue on our `GitHub repository`_.
- **Slack Channel**: Join Apache Airflow's `Slack <https://join.slack.com/t/apache-airflow/shared_invite/zt-2nsw28cw1-Lw4qCS0fgme4UI_vWRrwEQ>`_. Visit ``#airflow-ray`` for discussions and help.

We appreciate your input and are committed to improving this package to better serve the community.

.. _GitHub repository: https://github.com/astronomer/astro-provider-ray/issues
.. _#airflow-ray: https://astronomer-community.slack.com/archives/C01234567

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

.. _Contributing Guide: https://github.com/astronomer/astro-provider-ray/blob/main/CONTRIBUTING.rst

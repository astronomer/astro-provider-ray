astro-provider-ray
==================

This repository provides tools for integrating `Apache Airflow® <https://airflow.apache.org/>`_ with Ray, enabling the orchestration of Ray jobs within Airflow workflows. It includes a decorator, two operators, and one trigger designed to efficiently manage and monitor Ray jobs and services.

Table of Contents
-----------------

- `Components`_
- `Quickstart`_
- `Contact the Devs`_
- `Changelog`_
- `Contributing Guide`_

Components
----------

Hooks
~~~~~

- **RayHook**: Sets up methods needed to run operators and decorators, working with the 'Ray' connection type to manage Ray clusters and submit jobs.

Decorators
~~~~~~~~~~

- **_RayDecoratedOperator**: Simplifies integration by decorating task functions to work seamlessly with Ray.

Operators
~~~~~~~~~

- **SetupRayCluster**: (Placeholder for cluster setup details)
- **DeleteRayCluster**: (Placeholder for cluster deletion details)
- **SubmitRayJob**: Submits jobs to a Ray cluster using a specified host name.

Triggers
~~~~~~~~

- **RayJobTrigger**: Monitors asynchronous job execution submitted via ``SubmitRayJob`` or using the ``@ray.task()`` decorator.

Quickstart
----------

1. Pre-requisites
~~~~~~~~~~~~~~~~~

The ``SetupRayCluster`` and the ``DeleteRayCluster`` operator require helm to install the kuberay operator. See the `installing Helm <https://helm.sh/docs/intro/install/>`_ page for more details.

2. Installation
~~~~~~~~~~~~~~~

.. code-block:: sh

   pip install astro-provider-ray

3. Setting up the connection
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For SubmitRayJob operator (using an existing Ray cluster)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Connection Type: "Ray"
- Connection ID: e.g., "ray_conn"
- Ray dashboard URL: URL of the Ray dashboard
- Other optional fields: Cookies, Metadata, Headers, Verify SSL

For SetupRayCluster and DeleteRayCluster operators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Connection Type: "Ray"
- Connection ID: e.g., "ray_k8s_conn"
- Kube config path OR Kube config content (JSON format)
- Namespace: The k8 namespace where your cluster must be created. If not provided, "default" is used
- Optional fields: Cluster context, Disable SSL, Disable TCP keepalive

4. Setting up the Ray cluster spec
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a YAML file defining your Ray cluster configuration. Example:

.. code-block:: yaml

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

Save this file in a location accessible to your Airflow installation, and reference it in your DAG code.

**Note:** ``spec.headGroupSpec.serviceType`` must be a 'LoadBalancer' to spin a service that exposes your dashboard

5. Code Samples
~~~~~~~~~~~~~~~

There are two main scenarios for using this provider:

Scenario 1: Setting up a Ray cluster on an existing Kubernetes cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you have an existing Kubernetes cluster and want to install a Ray cluster on it, and then run a Ray job, you can use the ``SetupRayCluster``, ``SubmitRayJob``, and ``DeleteRayCluster`` operators. Here's an example DAG (``setup-teardown.py``):

.. code-block:: python

   from datetime import datetime, timedelta
   from pathlib import Path

   from airflow import DAG

   from ray_provider.operators.ray import DeleteRayCluster, SetupRayCluster, SubmitRayJob

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

Scenario 2: Using an existing Ray cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you already have a Ray cluster set up, you can use the ``SubmitRayJob`` operator or ``ray.task()`` decorator to submit jobs directly.

In the below example(``ray_taskflow_example.py``), the ``@ray.task`` decorator is used to define a task that will be executed on the Ray cluster.

.. code-block:: python

   from airflow.decorators import dag, task
   from datetime import datetime, timedelta
   from ray_provider.decorators.ray import ray

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

- **GitHub Issues**: For bug reports, feature requests, or general questions, please open an issue on our `GitHub repository <https://github.com/astronomer/astro-provider-ray/issues>`_.
- **Slack Channel**: Join Apache Airflow's `Slack <https://join.slack.com/t/apache-airflow/shared_invite/zt-2nsw28cw1-Lw4qCS0fgme4UI_vWRrwEQ>`_. Visit ``#airflow-ray`` for discussions and help.

We appreciate your input and are committed to improving this package to better serve the community.

Changelog
---------

We follow `Semantic Versioning <https://semver.org/>`_ for releases.
Check `CHANGELOG.rst <https://github.com/astronomer/astro-provider-ray/blob/main/CHANGELOG.rst>`_
for the latest changes.

Contributing Guide
------------------

All contributions, bug reports, bug fixes, documentation improvements, enhancements are welcome.

A detailed overview an how to contribute can be found in the `Contributing Guide <https://github.com/astronomer/astro-provider-ray/blob/main/CONTRIBUTING.rst>`_

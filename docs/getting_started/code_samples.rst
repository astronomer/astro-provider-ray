Code Samples
^^^^^^^^^^^^^^^

There are two main scenarios for using this provider:

Scenario 1: Setting up a Ray cluster on an existing Kubernetes cluster
""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

If you have an existing Kubernetes cluster and want to install a Ray cluster on it, and then run a Ray job, you can use the ``SetupRayCluster``, ``SubmitRayJob``, and ``DeleteRayCluster`` operators. Here's an example DAG (``setup_teardown.py``):

.. literalinclude:: ../../example_dags/setup-teardown.py

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

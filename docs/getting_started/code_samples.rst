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

.. literalinclude:: ../../example_dags/ray_taskflow_example.py

Remember to adjust file paths, connection IDs, and other specifics according to your setup.

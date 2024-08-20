Code Samples
============

There are two main scenarios for using this provider:

Scenario 1: Setting up a Ray cluster on an existing Kubernetes cluster
----------------------------------------------------------------------

If you have an existing Kubernetes cluster and want to install a Ray cluster on it, and then run a Ray job, you can use the ``SetupRayCluster``, ``SubmitRayJob``, and ``DeleteRayCluster`` operators.

This will involve 2 steps -

Create a YAML file defining your Ray cluster configuration.
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. note::
    ``spec.headGroupSpec.serviceType`` must be a 'LoadBalancer' to spin a service that exposes your dashboard externally

.. literalinclude:: ../../example_dags/scripts/ray.yaml

Save this file in a location accessible to your Airflow installation, and reference it in your DAG code.


Sample DAG (``setup_teardown.py``):
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. literalinclude:: ../../example_dags/setup-teardown.py
   :language: python
   :linenos:

Scenario 2: Using an existing Ray cluster
-----------------------------------------

If you already have a Ray cluster set up, you can use the ``SubmitRayJob`` operator or ``task.ray()`` decorator to submit jobs directly.

In the example below (``ray_taskflow_example.py``), the ``@task.ray`` decorator is used to define a task that will be executed on the Ray cluster:

.. literalinclude:: ../../example_dags/ray_taskflow_example.py
   :language: python
   :linenos:

.. note::
   Remember to adjust file paths, connection IDs, and other specifics according to your setup.

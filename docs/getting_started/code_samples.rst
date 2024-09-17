Code Samples
============

Index
-----
- `Example 1: Ray jobs on an existing cluster`_
- `Ray Cluster Sample Spec (YAML)`_
- `Example 2: Using @ray.task for job lifecycle`_
- `Example 3: Using SubmitRayJob operator for job lifecycle`_
- `Example 4: SetupRayCluster, SubmitRayJob & DeleteRayCluster`_

Example 1: Ray jobs on an existing cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
If you already have a Ray cluster set up, you can use the ``SubmitRayJob`` operator or ``ray.task()`` decorator to submit jobs directly.

In the example below (``ray_taskflow_example_existing_cluster.py``), the ``@ray.task`` decorator is used to define a task that will be executed on the Ray cluster:

.. important::
   **Set the Ray Dashboard URL connection parameter or RAY_ADDRESS on your airflow worker to connect to your cluster**

.. literalinclude:: ../../example_dags/ray_taskflow_example_existing_cluster.py
   :language: python
   :linenos:


Ray Cluster Sample Spec (YAML)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
.. important::
    ``spec.headGroupSpec.serviceType`` must be a 'LoadBalancer' to spin a service that exposes your dashboard externally

Save this file in a location accessible to your Airflow installation, and reference it in your DAG code.

.. literalinclude:: ../../example_dags/scripts/ray.yaml
   :language: yaml


Example 2: Using @ray.task for job lifecycle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The below example showcases how to use the ``@ray.task`` decorator to manage the full lifecycle of a Ray cluster: setup, job execution, and teardown.

This approach is ideal for jobs that require a dedicated, short-lived cluster, optimizing resource usage by cleaning up after task completion.

.. note::
   Configuration can be specified as a dictionary, either statically or dynamically at runtime as needed.

.. literalinclude:: ../../example_dags/ray_taskflow_example.py
   :language: python
   :linenos:


Example 3: Using SubmitRayJob operator for job lifecycle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example demonstrates how to use the ``SubmitRayJob`` operator to manage the full lifecycle of a Ray cluster and job execution.

This operator provides a more declarative way to define your Ray job within an Airflow DAG.

.. literalinclude:: ../../example_dags/ray_single_operator.py
   :language: python
   :linenos:


Example 4: SetupRayCluster, SubmitRayJob & DeleteRayCluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

This example shows how to use separate operators for cluster setup, job submission, and teardown, providing more granular control over the process.

This approach allows for more complex workflows involving Ray clusters.

Key Points:

- Uses SetupRayCluster, SubmitRayJob, and DeleteRayCluster operators separately.
- Allows for multiple jobs to be submitted to the same cluster before deletion.
- Demonstrates how to pass cluster information between tasks using XCom.

This method is ideal for scenarios where you need fine-grained control over the cluster lifecycle, such as running multiple jobs on the same cluster or keeping the cluster alive for a certain period.

.. important::
   **The SubmitRayJob operator uses the xcom_task_key parameter "SetupRayCluster.dashboard" to retrieve the Ray dashboard URL. This URL, stored as an XCom variable by the SetupRayCluster task, is necessary for job submission.**

.. literalinclude:: ../../example_dags/setup-teardown.py
   :language: python
   :linenos:

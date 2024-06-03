CHANGELOG
=========

1.0.0 (2024-06-03)
------------------

* Initial release, with the following decorators, operators, and triggers:

.. list-table::
   :header-rows: 1

   * - Decorator Class
     - Import Path
     - Example DAG

   * - ``_RayDecoratedOperator``
     - .. code-block:: python

        from ray_provider.decorators.kuberay import ray_task
     - N/A

.. list-table::
   :header-rows: 1

   * - Operator Class
     - Import Path
     - Example DAG

   * - ``RayClusterOperator``
     - .. code-block:: python

        from ray_provider.operators.kuberay import RayClusterOperator
     - Example DAG

   * - ``SubmitRayJob``
     - .. code-block:: python

        from ray_provider.operators.kuberay import SubmitRayJob
     - Example DAG

.. list-table::
   :header-rows: 1

   * - Trigger Class
     - Import Path
     - Example DAG

   * - ``RayJobTrigger``
     - .. code-block:: python

        from ray_provider.triggers.kuberay import RayJobTrigger
     - N/A
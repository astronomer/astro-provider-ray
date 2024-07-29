CHANGELOG
=========

1.0.0 (2024-07-28)
------------------

* Initial release, with the following decorators, hooks, operators, and triggers:

.. list-table::
   :header-rows: 1

   * - Hook Class
     - Import Path

   * - ``RayHook``
     - .. code-block:: python

            from ray_provider.hooks.ray import RayHook

.. list-table::
   :header-rows: 1

   * - Decorator Class
     - Import Path

   * - ``task.ray()``
     - .. code-block:: python

            from ray_provider.decorators.ray import task

.. list-table::
   :header-rows: 1

   * - Operator Class
     - Import Path

   * - ``SetupRayCluster``
     - .. code-block:: python

            from ray_provider.operators.ray import SetupRayCluster

   * - ``DeleteRayCluster``
     - .. code-block:: python

            from ray_provider.operators.ray import DeleteRayCluster

   * - ``SubmitRayJob``
     - .. code-block:: python

            from ray_provider.operators.ray import SubmitRayJob

.. list-table::
   :header-rows: 1

   * - Trigger Class
     - Import Path

   * - ``RayJobTrigger``
     - .. code-block:: python

            from ray_provider.triggers.ray import RayJobTrigger

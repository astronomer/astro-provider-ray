CHANGELOG
=========

0.2.0 (2024-08-29)
------------------

**Breaking changes**

- We removed the "use_gpu" input parameter from the SetupRayCluster and DeleteRayCluster operators. GPU drivers get installed if GPU nodes are available
- Spelling correction in the ``SubmitRayJob``operator. Changed "self.terminal _state" to "self.terminal_states"

**Enhancements to the SubmitRayJob operator**

Based on customer feedback, we learnt that it would be a much easier UX to spin up/down the cluster in the background of a task. The user would simply decorate their python function with @ray.task and the decorator would orchestrate the rest.

To enable this feature, we had to make changes to the code for SetupRayCluster and DeleteRayCluster operators. Making these changes helps us avoid duplication.

Following new input params added to enable this change -- ray_cluster_yaml, kuberay_version, update_if_exists, gpu_device_plugin_yaml

**Add more more example DAGs**

Earlier we had only 2 example dags. We now have 4. And we execute a different DAG for integration test.

**Making the Decorator more robust**

We made some changes to the decorator source code to make it more robust

**Unit tests updated**

Added unit tests where necessary and deleted where unnecessary. Updated where required.

**Documentation improvements**

- Significant changes to code samples section of the github page to make it easier to navigate
- Added two additional code samples along with explanation
- Added Getting Involved section to both Readme and Index.rst along with box formatting
- Some other minor changes


0.1.0 (2024-08-09)
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

   * - ``ray.task()``
     - .. code-block:: python

            from ray_provider.decorators.ray import ray

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

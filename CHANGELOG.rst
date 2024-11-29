CHANGELOG
=========

0.3.0 (2024-11-29)
---------------------

**Breaking changes**

* Simplify the project structure and debugging by @tatiana in #93

In order to improve the development and troubleshooting DAGs created with this provider, we introduced breaking changes
to the folder structure. It was flattened and the import paths to existing decorators, hooks, operators and trigger
changed, as documented in the table below:

+-----------+---------------------------------------------+-----------------------------------------+
| Type      | Previous import path                        | Current import path                     |
+===========+=============================================+=========================================+
| Decorator | ray_provider.decorators.ray.ray             | ray_provider.decorators.ray             |
| Hook      | ray_provider.hooks.ray.RayHook              | ray_provider.hooks.RayHook              |
| Operator  | ray_provider.operators.ray.DeleteRayCluster | ray_provider.operators.DeleteRayCluster |
| Operator  | ray_provider.operators.ray.SetupRayCluster  | ray_provider.operators.SetupRayCluster  |
| Operator  | ray_provider.operators.ray.SubmitRayJob     | ray_provider.operators.SubmitRayJob     |
| Trigger   | ray_provider.triggers.ray.RayJobTrigger     | ray_provider.triggers.RayJobTrigger     |
+-----------+---------------------------------------------+-----------------------------------------+

* Removal of ``SubmitRayJob.terminal_states``. The same values are now available at ``ray_provider.constants.TERMINAL_JOB_STATUSES``.


**Features**

* Support using callable ``config`` in ``@ray.task`` by @tatiana in #103
* Support running Ray jobs indefinitely without timing out by @venkatajagannath and @tatiana in #74

**Bug fixes**

* Fix integration test and bug in load balancer wait logic by @pankajastro in #85
* Bugfix: Better exception handling and cluster clean up by @venkatajagannath in #68
* Stop catching generic ``Exception`` in operators by @tatiana in #100
* Stop catching generic ``Exception`` in trigger by @tatiana in #99

**Docs**

* Add docs to deploy project on Astro Cloud by @pankajastro in #90
* Fix dead reference in docs index page by @pankajastro in #87
* Cloud Auth documentation update by @venkatajagannath in #58
* Improve main docs page by @TJaniF in #71

**Others**

Local development

* Fix the local development environment and update documentation by @tatiana in #92
* Enable secret detection precommit check by @pankajastro in #91
* Add astro cli project + kind Raycluster setup instruction by @pankajastro in #83
* Remove pytest durations from tests by @tatiana in #102
* Fix running make docker-run when there is a new version by @tatiana in #99 and #101
* Improve Astro CLI DAGs test so running hatch test-cov locally doesn't fail by @tatiana in #97

CI

* CI improvement by @venkatajagannath in #73
* CI fix related to broken coverage upload artifact by @pankajkoti in #60
* Allow tests to run for PRs from forked repos by @venkatajagannath in #72
* Update CODEOWNERS by @tatiana in #84
* Add Airflow 2.10 (released in August 2024) to tests by @tatiana in #96


0.2.1 (2024-09-04)
------------------

**Bug fixes**
Namespace variable initialized in init method is not used in setup_ray_cluster and delete_ray_cluster methods
CI/CD pipeline broken due to sudden github action breaking change



0.2.0 (2024-08-29)
------------------

by @venkatajagannath in #50

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

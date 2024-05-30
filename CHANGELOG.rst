CHANGELOG
=========

1.0.0 (2024-05-29)
------------------

* Initial release, with the following hooks, operators, and triggers:

.. list-table::
   :header-rows: 1

   * - Hook Class
     - Import Path
     - Example DAG

   * - ``AnyscaleHook``
     - .. code-block:: python

        from anyscale_provider.hooks.anyscale import AnyscaleHook
     - N/A

.. list-table::
   :header-rows: 1

   * - Operator Class
     - Import Path
     - Example DAG

   * - ``AnyscaleSubmitJob``
     - .. code-block:: python

        from anyscale_provider.operators.anyscale import AnyscaleSubmitJob
     - Example DAG

   * - ``AnyscaleDeployService``
     - .. code-block:: python

        from anyscale_provider.operators.anyscale import RolloutAnyscaleService
     - Example DAG

.. list-table::
   :header-rows: 1

   * - Trigger Class
     - Import Path
     - Example DAG

   * - ``AnyscaleJobTrigger``
     - .. code-block:: python

        from anyscale_provider.triggers.anyscale import AnyscaleJobTrigger
     - N/A

   * - ``AnyscaleServiceTrigger``
     - .. code-block:: python

        from anyscale_provider.triggers.anyscale import AnyscaleServiceTrigger
     - N/A
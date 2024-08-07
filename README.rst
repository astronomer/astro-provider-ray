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

.. literalinclude:: ../example_dags/scripts/ray.yaml
   :language: yaml
   :caption: ray.yaml
   :linenos:

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

.. literalinclude:: ../example_dags/setup-teardown.py

Scenario 2: Using an existing Ray cluster
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If you already have a Ray cluster set up, you can use the ``SubmitRayJob`` operator or ``ray.task()`` decorator to submit jobs directly.

In the below example(``ray_taskflow_example.py``), the ``@ray.task`` decorator is used to define a task that will be executed on the Ray cluster.

.. literalinclude:: ../example_dags/ray_taskflow_example.py

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

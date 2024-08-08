Getting started
~~~~~~~~~~~~~~~

1. Pre-requisites
^^^^^^^^^^^^^^^^^

The ``SetupRayCluster`` and the ``DeleteRayCluster`` operator require helm to install the kuberay operator. See the `installing Helm <https://helm.sh/docs/intro/install/>`_ page for more details.

2. Installation
^^^^^^^^^^^^^^^

.. code-block:: sh

   pip install astro-provider-ray

3. Setting up the connection
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For SubmitRayJob operator (using an existing Ray cluster)
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""

- **Connection Type**: "Ray"
- **Connection ID**: e.g., "ray_conn"
- **Ray dashboard URL**: URL of the Ray dashboard
- **Optional fields**: Cookies, Metadata, Headers, Verify SSL

For SetupRayCluster and DeleteRayCluster operators
""""""""""""""""""""""""""""""""""""""""""""""""""

- **Connection Type**: "Ray"
- **Connection ID**: e.g., "ray_k8s_conn"
- **Kube config path** OR **Kube config content (JSON format)** : Kubeconfig of the kubernetes cluster where Ray cluster must be setup
- **Namespace**: The k8 namespace where your cluster must be created. If not provided, "default" is used
- **Optional fields**: Cluster context, Disable SSL, Disable TCP keepalive

5. Setting up the Ray cluster spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Create a YAML file defining your Ray cluster configuration. Example:

**Note:** ``spec.headGroupSpec.serviceType`` must be a 'LoadBalancer' to spin a service that exposes your dashboard externally

.. literalinclude:: ../../example_dags/scripts/ray.yaml

Save this file in a location accessible to your Airflow installation, and reference it in your DAG code.

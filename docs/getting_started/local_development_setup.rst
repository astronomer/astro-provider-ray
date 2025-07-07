Local Development Setup Guide
#############################

This document describes the local setup for RayCluster and `Apache Airflow® <https://airflow.apache.org/>`_.

Table of Contents
=================

- `Setup RayCluster`_
- `Setup Apache Airflow®`_


Setup RayCluster
================

This section describes how to set up RayCluster on Kind. For detailed instructions, please refer to the official guide: `RayCluster Quick Start <https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html#raycluster-quickstart>`_.

Prerequisites
-------------

Install the following software:

- `Docker <https://docs.docker.com/desktop/>`_
- `Kubectl <https://kubernetes.io/docs/tasks/tools/>`_
- `Kind <https://kind.sigs.k8s.io/docs/user/quick-start/>`_
- `Helm <https://helm.sh/>`_

1. **Create a Kind Cluster**

a) If you plan to access the Kind Kubernetes cluster from Airflow using Astro CLI, use the following configuration file,
also available in ``dev/kind-config.yaml``, to create the Kind cluster:

.. code-block::

    kind: Cluster
    name: local
    apiVersion: kind.x-k8s.io/v1alpha4
    networking:
      apiServerAddress: "0.0.0.0"
      apiServerPort: 6443
    nodes:
      - role: control-plane
    kubeadmConfigPatchesJSON6902:
    - group: kubeadm.k8s.io
      version: v1beta3
      kind: ClusterConfiguration
      patch: |
        - op: add
          path: /apiServer/certSANs/-
          value: host.docker.internal

Use the following command to create the Kind cluster:

.. code-block:: bash

    kind create cluster --config kind-config.yaml

If you don't do this, Astro CLI will have issues reaching the Kind Kubernetes cluster, raising exceptions similar to:

.. code-block::

    [2024-11-19, 14:52:39 UTC] {ray.py:606} ERROR - Standard Error: Error: Kubernetes cluster unreachable: Get "https://host.docker.internal:57034/version": tls: failed to verify certificate: x509: certificate is valid for kind-control-plane, kubernetes, kubernetes.default, kubernetes.default.svc, kubernetes.default.svc.cluster.local, localhost, not host.docker.internal


b) Otherwise, if planning to access Kind from Airflow **outside of Astro CLi**, create a cluster using:

.. code-block:: bash

    kind create cluster --image=kindest/node:v1.26.0


2. **Deploy a KubeRay Operator**

.. code-block:: bash

    helm repo add kuberay https://ray-project.github.io/kuberay-helm/
    helm repo update

    # Install both CRDs and KubeRay operator v1.2.2.
    helm install kuberay-operator kuberay/kuberay-operator --version 1.2.2

    # Confirm that the operator is running in the namespace `default`.
    kubectl get pods
    # NAME                                READY   STATUS    RESTARTS   AGE
    # kuberay-operator-b498fcfdf-hsjvk    1/1     Running   0          29s


3. **Deploy a RayCluster Custom Resource**

The following configuration applies to running in MacOS M1 machines. Read the
`official documentation <https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html#raycluster-quickstart>`_
for deploying a RayCluster in other architectures.


.. code-block:: bash

    # Deploy a sample RayCluster CR from the KubeRay Helm chart repo:
    helm install raycluster kuberay/ray-cluster --version 1.2.2 --set 'image.tag=2.9.0-aarch64'

    # Once the RayCluster CR has been created, you can view it by running:
    kubectl get rayclusters
    # NAME                 DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
    # raycluster-kuberay   1                 1                   2      3G       0      ready    99s

    # View the pods in the RayCluster named "raycluster-kuberay"
    kubectl get pods --selector=ray.io/cluster=raycluster-kuberay

    # NAME                                          READY   STATUS    RESTARTS   AGE
    # raycluster-kuberay-head-wvzh2                 1/1     Running   0          XXs
    # raycluster-kuberay-worker-workergroup-4dfsb   1/1     Running   0          XXs

Wait for the pods to reach the ``Running`` state

4. **Expose the Port on Host Machine**

.. code-block:: bash

    kubectl get service raycluster-kuberay-head-svc

    # NAME                          TYPE        CLUSTER-IP    EXTERNAL-IP   PORT(S)                                         AGE
    # raycluster-kuberay-head-svc   ClusterIP   10.96.1.92    <none>        8265/TCP,8080/TCP,8000/TCP,10001/TCP,6379/TCP   25m

    # Execute this in a separate shell.
    kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265

5. Access the Ray Dashboard

Visit http://127.0.0.1:8265 in your browser.

6. Create a Kubernetes secret with your Docker Hub credentials

We highly encourage users to create a Kubernetes secret containing `Docker hub credentials <https://hub.docker.com/>`_
and add this to Kind, as illustrated below:

.. code-block:: bash

    kubectl create secret docker-registry my-registry-secret  --docker-server=https://index.docker.io/v1/ --docker-username=<dockerhub-username> --docker-password=<dockerhub-password>

Users should replace ``<dockerhub-username>`` and ``<dockerhub-password>`` with their own credentials.

The goal of this configuration is to overcome a common issue, when Docker Hub blocks Kind from pulling the images necessary to create a Ray cluster.
The side effect is that the ``raycluster-kuberay-head`` and ``raycluster-kuberay-workergroup``
Kubernetes Pods will not manage to get into the ``Running`` state. When describing them, users will identify the
``Error: ImagePullBackOff`` message and:

.. code-block::

    │ image "rayproject/ray-ml:latest": failed to pull and unpack image "docker.io/ray │

When using the Kubernetes secret with Docker hub credentials, such as ``my-registry-secret``,
users should make sure that their RayCluster K8s YAML contains:

.. code-block::

     spec:
        imagePullSecrets:
          - name: my-registry-secret

This approach is illustrated in the file ``dev/dags/scripts/ray.yaml``.


Additional steps in MacOS
=========================

When developing under MacOS (such as M1 instances), you may face some issues. The following steps describe how to overcome them.

Requirements
------------

- `Docker Mac Net Connect <https://github.com/chipmk/docker-mac-net-connect>`_
- `MetalLB <https://github.com/metallb/metallb>`_

1. Expose Kind Network to host
------------------------------

With Docker on Linux, you can send traffic directly to the LoadBalancer’s external IP if the IP space is within the Docker IP space.

On MacOS, Docker does not expose the Docker network to the host".

A workaround is to use docker-mac-net-connect:
https://github.com/chipmk/docker-mac-net-connect

.. code-block:: bash

    # Install via Homebrew
    $ brew install chipmk/tap/docker-mac-net-connect

    # Run the service and register it to launch at boot
    $ sudo brew services start chipmk/tap/docker-mac-net-connect

This will expose the Kind network to the host network seamlessly.


2. Enable the creation of LoadBalancers in Kind
-----------------------------------------------

When attempting to run Ray in Kind from Airflow using Astro, you may face issues when attempting to spin up the Kubernetes LoadBalancer.
This will happen, particularly, if your DAGs create and tear down the Ray cluster, and are not using a pre-created cluster.

A side-effect of this is that you will see the ``LoadBalancer`` hanging on the state ``<pending>`` indefinitely.

Example:

.. code-block::
    $ kubectl get svc

    NAME                     TYPE            CLUSTER-IP     EXTERNAL-IP   PORT(S)                                                                    AGE
    kubernetes               ClusterIP      10.96.0.1        <none>        443/TCP                                                                   5d21h
    my-raycluster-head-svc   LoadBalancer   10.96.124.7      <pending> 10001:31531/TCP,8265:30347/TCP,6379:31291/TCP,8080:30358/TCP,8000:32362/TCP   2m13s

In a kind cluster, the Kubernetes control plane lacks direct integration with a cloud provider.
Since ``LoadBalancer`` services rely on cloud infrastructure to provision external IPs, they cannot natively work in kind without additional setup.

By default:

- The service type LoadBalancer won't provision an external IP.
- Services remain in a <pending> state until a workaround or external load balancer is introduced.

You can use `MetalLB <https://github.com/metallb/metallb>`_, a load balancer implementation for bare-metal Kubernetes clusters. Here's how to set it up:

a) Install MetalLB: Apply the MetalLB manifests

.. code-block:: bash

    kubectl apply -f https://raw.githubusercontent.com/metallb/metallb/v0.13.10/config/manifests/metallb-native.yaml

b) Configure IP Address Pool: MetalLB requires a pool of IPs that it can assign. Create a ConfigMap with a range of available IPs:

.. code-block:: bash

    cat <<EOF | kubectl apply -f -
    apiVersion: metallb.io/v1beta1
    kind: IPAddressPool
    metadata:
      name: kind-pool
      namespace: metallb-system
    spec:
      addresses:
      - 172.18.255.1-172.18.255.254
    ---
    apiVersion: metallb.io/v1beta1
    kind: L2Advertisement
    metadata:
      name: kind-l2-advertisement
      namespace: metallb-system
    spec: {}
    EOF

Adjust the IP range (172.18.255.1-172.18.255.254) if your kind cluster uses a different subnet. This is the default one.


Setup Apache Airflow®
=====================

This section describes how to set up `Apache Airflow® <https://airflow.apache.org/>`_ using Astro CLI. For detailed instructions, please refer to the official guide: `Astro CLI Quick Start <https://www.astronomer.io/docs/astro/cli/get-started-cli>`_.

Prerequisites
-------------

- `Docker <https://docs.docker.com/desktop/>`_
- `Astro CLI <https://www.astronomer.io/docs/astro/cli/install-cli>`_

We have a `Makefile <https://github.com/astronomer/astro-provider-ray/blob/main/Makefile>`_ that wraps the Astro CLI. It installs the necessary packages into your image to run the DAG locally.


1. **Start Airflow Instance**

.. code-block:: bash

    make docker-run

To see other available Makefile targets, please run ``make help``.

In our demo setup, we are making sure that Astro CLI shares the same network as Kind, by using the following
``docker-compose.override.yml`` file:

.. code-block::

    version: '3.8'

    services:
      webserver:
        networks:
          - kind

      scheduler:
        networks:
          - kind

      triggerer:
        networks:
          - kind

    networks:
      kind:
        external: true

This is particularly relevant to make it easier for the Airflow containers to be able to access the Ray cluster resources
created in the Kind cluster.

2. **Make the Kubernetes config file available to Airflow**

As an example, if using Kind, you should do something similar to:

.. code-block:: bash

    cp ~/.kube/config dev/dags

This will make the Kubernetes config file available in ``/usr/local/airflow/dags/config``, when using Astro CLI.

The file ``dev/dags/config`` will look like something similar to:

.. code-block::

    apiVersion: v1
    clusters:
    - cluster:
        certificate-authority-data: some-value
        server: https://host.docker.internal:6443
      name: kind-local
    contexts:
    - context:
        cluster: kind-local
        user: kind-local
      name: kind-local
    current-context: kind-local
    kind: Config
    preferences: {}
    users:
    - name: kind-local
      user:
        client-certificate-data: some-value
        client-key-data: some-key

Make sure the value of the ``server`` property allows your Airflow installation to access the K8s cluster.

When using Astro CLI, this will be likely:

.. code-block::

    server: https://host.docker.internal:6443


3. **Create Airflow Connection**

- Visit http://localhost:8080/ in your browser.

- Log in with username: admin and password: admin.

- Click on Admin -> Connections -> Add a new record. Select Connection type ``Ray``

The most basic setup will look something like below:

- Ray dashboard url: Kind Ray cluster dashboard url (example: http://host.docker.internal:8265/)
- Kube config path: Provide the path to your Kubernetes config file and ensure it is accessible from the Airflow containers (example: ``/usr/local/airflow/dags/config``)
- Disable SSL: Tick the disable SSL boolean if needed

.. image::  ../_static/basic_local_kubernetes_conn.png

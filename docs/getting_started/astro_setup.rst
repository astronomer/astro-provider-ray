Deploy on Astro Cloud
#####################

Deploying your project on Astro Cloud allows you to easily manage and orchestrate Ray workflows. This section guides you through the deployment process.

Table of Contents
=================

- `Setup GKE Cluster`_
- `Deploy to Astro Cloud`_
- `Troubleshoot`_

Setup GKE Cluster
=================

This section describes how to create a GKE cluster that can be used to orchestrate Ray jobs using Astro Cloud.

Prerequisites
-------------

Install the following software:

- `Gcloud <https://cloud.google.com/sdk/docs/install>`_

Steps
-----

1. **Create a Google Cloud service account key JSON file:** Follow the instructions `here <https://cloud.google.com/iam/docs/keys-create-delete>`_

2. **Set up the gcloud credentials**

.. code-block:: bash

    gcloud auth activate-service-account --key-file=</path/to/your/service-account-key.json>

    gcloud config set project <YOUR_PROJECT_ID>

3. **Install the gcloud auth plugin**

.. code-block:: bash

    gcloud components install gke-gcloud-auth-plugin

4. **Create a GKE cluster**

.. code-block:: bash

    gcloud container clusters create <YOUR_CLUSTER_NAME> \
          --zone us-central1-a \
          --num-nodes 1 \
          --machine-type e2-standard-4 \
          --enable-autoscaling --min-nodes=1 --max-nodes=3 \
          --no-enable-ip-alias
          --project <YOUR_PROJECT_ID>

5. **Retrieve GKE cluster configuration**

.. code-block:: bash

    gcloud container clusters get-credentials <YOUR_CLUSTER_NAME> --zone us-central1-a

    kubectl config view --raw > kubeconfig.yaml

We will use the ``kubeconfig.yaml`` file to create an Airflow connection of type ``Ray`` in Astro Cloud. You need to mount the ``kubeconfig.yaml`` file into the Airflow Docker container and specify the path to this file within the Airflow connection.

6. ** Optional: Delete GKE cluster**

Once you no longer need the GKE cluster, you can delete it using the command below.

.. code-block:: bash

    gcloud container clusters delete <YOUR_CLUSTER_NAME>


Deploy to Astro Cloud
=====================

This section describes how to deploy the project and orchestrate Ray jobs on Astro Cloud.

Prerequisites
-------------

- `Docker <https://docs.docker.com/desktop/>`_
- `Astro CLI <https://www.astronomer.io/docs/astro/cli/install-cli>`_

Steps
-----

1. **Create an Astro deployment:** Follow the instructions `here <https://www.astronomer.io/docs/astro/create-deployment#:~:text=Create%20a%20Deployment%E2%80%8B,%2C%20executor%2C%20and%20worker%20resources.>`_

2. **Deploy the project on Astro Cloud**

.. code-block:: bash

    make deploy

This command will build a wheel from your branch and deploy the `project <https://github.com/astronomer/astro-provider-ray/tree/main/dev>`_ in Astro Cloud.

3. **Create an Airflow Connection**

- Navigate to Admin -> Connections -> Add a new record. Select the connection type ``Ray`` and set the parameter ``Kube config path`` to the path of ``kubeconfig.yaml``.

Troubleshoot
-------------

1. **I'm encountering the error: "You do not currently have an active account selected for RayCluster on GKE.**

This can occur if the environment isn't properly configured for using a service account. Please try running the command below on your machine.

.. code-block:: bash

    gcloud auth activate-service-account --key-file='/path/to/your/service-account-key.json'


Alternatively, you can add a start Airflow task to execute this command.

.. code-block:: python

    @task.bash
    def activate_service_account() -> str:
        return "gcloud auth activate-service-account --key-file='/path/to/your/service-account-key.json'"

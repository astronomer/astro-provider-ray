Cloud Platform instructions
===========================


Amazon Web Services (AWS)
-------------------------

The below steps are useful if you have your Kubernetes cluster hosted on AWS EKS

**1. Add AWS CLI to your Airflow worker**

.. code-block:: sh

   pip install awscli

Or add the package to your requirements.txt file. For other methods to install the CLI, please see `here <https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>`_

**2. Ensure that you are able to authenticate with AWS**

.. code-block:: sh

    AWS_ACCESS_KEY_ID=<your-access-key>
    AWS_SECRET_ACCESS_KEY=<your-secret-access-key>
    AWS_SESSION_TOKEN=<your-session-token>

The Airflow worker will need to access the Kubernetes cluster on AWS. Setup access using one of the methods listed `here1 <https://docs.aws.amazon.com/cli/v1/userguide/cli-chap-authentication.html>`_

Microsoft Azure
---------------

If you have your Kubernetes cluster hosted on Azure Kubernetes Service (AKS), follow these steps:

**1. Install Azure CLI on your Airflow worker**

.. code-block:: sh

   pip install azure-cli

For other installation methods, please refer to the `Azure CLI installation guide <https://docs.microsoft.com/en-us/cli/azure/install-azure-cli>`_.

**2. Authenticate with Azure**

To authenticate the Airflow worker with Azure, you can use one of the following methods:

a. Interactive login:

.. code-block:: sh

    az login

b. Service principal authentication:

.. code-block:: sh

    AZURE_CLIENT_ID=<your-client-id>
    AZURE_CLIENT_SECRET=<your-client-secret>
    AZURE_TENANT_ID=<your-tenant-id>

Replace ``<app-id>``, ``<password-or-cert>``, and ``<tenant>`` with your specific Azure credentials.

For more authentication options, see the `Azure authentication guide <https://docs.microsoft.com/en-us/cli/azure/authenticate-azure-cli>`_.


Google Cloud Platform (GCP)
---------------------------

**1. Install the Google Cloud CLI on your Airflow worker**

.. code-block:: sh

    pip install gcloud

**2. Authenticate with GCP**

.. code-block:: sh

    GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"

import os
import socket
import subprocess
import tempfile
import time
from functools import cached_property
from typing import Any, AsyncIterator

import requests
import yaml
from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes import client, config
from ray.job_submission import JobStatus, JobSubmissionClient


class RayHook(KubernetesHook):  # type: ignore
    """
    Airflow Hook for interacting with Ray Job Submission Client and Kubernetes Cluster.
    """

    conn_name_attr = "ray_conn_id"
    default_conn_name = "ray_default"
    conn_type = "ray"
    hook_name = "Ray"

    DEFAULT_NAMESPACE = "default"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, PasswordField, StringField

        return {
            "address": StringField(lazy_gettext("Ray address path"), widget=BS3TextFieldWidget()),
            "create_cluster_if_needed": BooleanField(lazy_gettext("Create cluster if needed")),
            "cookies": StringField(lazy_gettext("Ray job cookies"), widget=BS3TextFieldWidget()),
            "metadata": StringField(lazy_gettext("Ray job metadata"), widget=BS3TextFieldWidget()),
            "headers": StringField(lazy_gettext("Ray job headers"), widget=BS3TextFieldWidget()),
            "verify": BooleanField(lazy_gettext("Ray job verify")),
            "kube_config_path": StringField(lazy_gettext("Kube config path"), widget=BS3TextFieldWidget()),
            "kube_config": PasswordField(lazy_gettext("Kube config (JSON format)"), widget=BS3PasswordFieldWidget()),
            "namespace": StringField(lazy_gettext("Namespace"), widget=BS3TextFieldWidget()),
            "cluster_context": StringField(lazy_gettext("Cluster context"), widget=BS3TextFieldWidget()),
            "disable_verify_ssl": BooleanField(lazy_gettext("Disable SSL")),
            "disable_tcp_keepalive": BooleanField(lazy_gettext("Disable TCP keepalive")),
            "xcom_sidecar_container_image": StringField(
                lazy_gettext("XCom sidecar image"), widget=BS3TextFieldWidget()
            ),
            "xcom_sidecar_container_resources": StringField(
                lazy_gettext("XCom sidecar resources (JSON format)"), widget=BS3TextFieldWidget()
            ),
        }

    def __init__(
        self,
        conn_id: str = default_conn_name,
        xcom_dashboard_url: str | None = None,
    ) -> None:
        super().__init__(conn_id=conn_id)

        # We check for address in 3 places -- Connection, Operator input param & Env variable
        self.address = self._get_field("address") or xcom_dashboard_url or os.getenv("RAY_ADDRESS")
        self.log.info(f"Ray cluster address is: {self.address}")
        self.create_cluster_if_needed = self._get_field("create_cluster_if_needed") or False
        self.cookies = self._get_field("cookies")
        self.metadata = self._get_field("metadata")
        self.headers = self._get_field("headers")
        self.verify = self._get_field("verify") or False
        self.ray_client_instance = None

        self.namespace = self.get_namespace()
        self.kubeconfig = None

        # Handle in-cluster and kubeconfig setup
        cluster_context = self._get_field("cluster_context")
        kubeconfig_path = self._get_field("kube_config_path")
        kubeconfig_content = self._get_field("kube_config")

        num_selected_configuration = sum(1 for o in [kubeconfig_path, kubeconfig_content] if o)
        if num_selected_configuration > 1:
            raise AirflowException(
                "Invalid connection configuration. Options kube_config_path and "
                "kube_config are mutually exclusive. "
                "You can only use one option at a time."
            )

        if kubeconfig_path:
            self.log.debug("Loading kube_config from: %s", kubeconfig_path)
            self._is_in_cluster = False
            self.kubeconfig = kubeconfig_path
            config.load_kube_config(config_file=kubeconfig_path, context=cluster_context)
        elif kubeconfig_content:
            with tempfile.NamedTemporaryFile(delete=False) as temp_config:
                self.log.debug("Loading kube_config from connection kube_config content")
                self._is_in_cluster = False
                temp_config.write(kubeconfig_content.encode())
                temp_config.flush()
                self.kubeconfig = temp_config.name
                config.load_kube_config(config_file=self.kubeconfig, context=cluster_context)

    @cached_property
    def ray_client(self) -> JobSubmissionClient:
        """
        Establishes a connection to the Ray Job Submission Client.
        """
        if not self.ray_client_instance:
            try:
                self.ray_client_instance = JobSubmissionClient(
                    address=self.address,
                    create_cluster_if_needed=self.create_cluster_if_needed,
                    cookies=self.cookies,
                    metadata=self.metadata,
                    headers=self.headers,
                    verify=self.verify,
                )
            except Exception as e:
                raise AirflowException(f"Failed to create Ray JobSubmissionClient: {e}")
        return self.ray_client_instance

    def submit_ray_job(self, entrypoint: str, runtime_env: dict[str, Any] | None = None, **job_config: Any) -> str:
        """
        Submits a job to the Ray cluster.

        :param entrypoint: The command or script to run.
        :param runtime_env: The runtime environment for the job.
        :param job_config: Additional job configuration parameters.
        :return: Job ID of the submitted job.
        """
        client = self.ray_client
        job_id = client.submit_job(entrypoint=entrypoint, runtime_env=runtime_env, **job_config)
        self.log.info(f"Submitted job with ID: {job_id}")
        return str(job_id)

    def delete_ray_job(self, job_id: str) -> Any:
        client = self.ray_client
        self.log.info(f"Deleting job with ID: {job_id}")
        return client.delete_job(job_id=job_id)

    def get_ray_job_status(self, job_id: str) -> JobStatus:
        """
        Gets the status of a submitted job.

        :param job_id: The ID of the job.
        :return: Status of the job.
        """
        client = self.ray_client
        status = client.get_job_status(job_id=job_id)
        self.log.info(f"Job {job_id} status: {status}")
        return status

    def get_ray_job_logs(self, job_id: str) -> str:
        """
        Retrieves the logs of a submitted job.

        :param job_id: The ID of the job.
        :return: Logs of the job.
        """
        client = self.ray_client
        logs = client.get_job_logs(job_id=job_id)
        self.log.info(f"Logs for job {job_id}: {logs}")
        return str(logs)

    async def get_ray_tail_logs(self, job_id: str) -> AsyncIterator[str]:
        """
        Tails the logs of a submitted job asynchronously.

        :param job_id: The ID of the job.
        :return: An async iterator of log lines.
        """
        client = self.ray_client
        async for lines in client.tail_job_logs(job_id):
            yield lines

    def load_yaml_content(self, path_or_link: str) -> Any:
        if path_or_link.startswith("http"):
            response = requests.get(path_or_link)
            response.raise_for_status()
            return yaml.safe_load(response.text)

        with open(path_or_link) as f:
            return yaml.safe_load(f)

    def get_external_dns(self, service: client.V1Service) -> str | None:
        if service and service.status.load_balancer.ingress and service.status.load_balancer.ingress[0].hostname:
            external_dns: str = service.status.load_balancer.ingress[0].hostname
            self.log.info(f"External DNS name found: {external_dns}")
            return external_dns
        else:
            return None

    def wait_for_external_dns(self, service_name: str, max_retries: int = 30, retry_interval: int = 40) -> str | None:
        for attempt in range(max_retries):
            self.log.info(f"Attempt {attempt + 1}: Checking for service's external DNS name...")
            service = self.get_service(name=service_name)
            dns_name: str | None = self.get_external_dns(service=service)
            if dns_name:
                return dns_name
            self.log.info("External DNS name not yet available, waiting...")
            time.sleep(retry_interval)
        return None

    def _is_port_open(self, host: str, port: int) -> bool:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        try:
            s.connect((host, port))
            s.close()
            return True
        except OSError:
            return False

    def _endpoints_are_ready(self, service_name: str) -> bool:
        endpoints = self.get_endpoints(name=service_name)
        if endpoints and endpoints.subsets and all([subset.addresses for subset in endpoints.subsets]):
            return True
        return False

    def _all_ports_are_open(self, external_dns: str, service_name: str) -> bool:
        endpoints = self.get_endpoints(name=service_name)
        if not endpoints or not endpoints.subsets:
            return False

        for subset in endpoints.subsets:
            for port in subset.ports:
                if not self._is_port_open(external_dns, port.port):
                    return False
        return True

    def wait_for_endpoints(
        self, external_dns: str, service_name: str, max_retries: int = 30, retry_interval: int = 40
    ) -> bool:
        for attempt in range(max_retries):
            if self._endpoints_are_ready(service_name) and self._all_ports_are_open(external_dns, service_name):
                self.log.info("All associated pods are ready.")
                return True
            self.log.info(f"Pods not ready, waiting... (Attempt {attempt + 1})")
            time.sleep(retry_interval)
        return False

    def _run_bash_command(self, command: str, env: dict[str, str] | None = None) -> tuple[str | None, str | None]:
        # Use the current environment variables if no custom environment is provided
        custom_env = os.environ.copy()
        if env:
            custom_env.update(env)

        try:
            # Execute the bash command with the custom environment
            result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True, env=custom_env)

            # Print the standard output and standard error
            self.log.info("Standard Output: %s", result.stdout)
            self.log.info("Standard Error: %s", result.stderr)

            # Return the command result
            return result.stdout, result.stderr
        except subprocess.CalledProcessError as e:
            self.log.error("An error occurred while executing the command: %s", e)
            self.log.error("Return code: %s", e.returncode)
            self.log.error("Standard Output: %s", e.stdout)
            self.log.error("Standard Error: %s", e.stderr)
            return None, None

    def install_kuberay_operator(
        self, version: str = "1.0.0", env: dict[str, str] | None = None
    ) -> tuple[str | None, str | None]:
        """
        Install KubeRay operator using Helm.
        """
        helm_command = f"""
            helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && \
            helm repo update && \
            helm upgrade --install kuberay-operator kuberay/kuberay-operator \
            --version {version} --create-namespace --namespace {self.namespace} --kubeconfig {self.kubeconfig}
        """
        result = self._run_bash_command(helm_command, env)
        self.log.info(result)
        return result

    def uninstall_kuberay_operator(self) -> tuple[str | None, str | None]:
        """
        Uninstall KubeRay operator using Helm.
        """
        helm_command = f"""
            helm uninstall kuberay-operator --namespace {self.namespace} --kubeconfig {self.kubeconfig}
        """
        result = self._run_bash_command(helm_command)
        self.log.info(result)
        return result

    def get_daemon_set(self, name: str) -> client.V1DaemonSet | None:
        """
        Retrieve a DaemonSet resource in Kubernetes.

        :param name: The name of the DaemonSet.
        :return: The DaemonSet resource.
        """
        try:
            api_response = self.apps_v1_client.read_namespaced_daemon_set(name, self.namespace)
            self.log.info(f"DaemonSet {api_response.metadata.name} retrieved.")
            return api_response
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.log.error(f"DaemonSet not found: {e}")
                return None
            else:
                self.log.error(f"Exception when getting DaemonSet: {e}")
                raise e

    def create_daemon_set(self, name: str, body: dict[str, Any]) -> client.V1DaemonSet | None:
        """
        Create a DaemonSet resource in Kubernetes.

        :param name: The name of the DaemonSet.
        :param body: The body of the DaemonSet for the create action.
        :return: The DaemonSet resource.
        """
        if not body:
            self.log.error("Body must be provided for create action.")
            return None

        try:
            api_response = self.apps_v1_client.create_namespaced_daemon_set(namespace=self.namespace, body=body)
            self.log.info(f"DaemonSet {api_response.metadata.name} created.")
            return api_response
        except client.exceptions.ApiException as e:
            self.log.error(f"Exception when creating DaemonSet: {e}")
            return None

    def delete_daemon_set(self, name: str) -> client.V1Status | None:
        """
        Delete a DaemonSet resource in Kubernetes.

        :param name: The name of the DaemonSet.
        :return: The status of the delete operation.
        """
        try:
            delete_response = self.apps_v1_client.delete_namespaced_daemon_set(name=name, namespace=self.namespace)
            self.log.info(f"DaemonSet {name} deleted.")
            return delete_response
        except client.exceptions.ApiException as e:
            self.log.error(f"Exception when deleting DaemonSet: {e}")
            return None

    def get_service(self, name: str) -> client.V1Service | None:
        """
        Retrieve a Service resource in Kubernetes.

        :param name: The name of the Service.
        :return: The Service resource.
        """
        try:
            v1_service = self.core_v1_client.read_namespaced_service(name=name, namespace=self.namespace)
            self.log.info(f"Service {v1_service.metadata.name} retrieved.")
            return v1_service
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.log.error(f"Service not found: {e}")
                return None
            self.log.error(f"Exception when getting service: {e}")
            return None

    def create_service(self, name: str, body: dict[str, Any]) -> client.V1Service | None:
        """
        Create a Service resource in Kubernetes.

        :param name: The name of the Service.
        :param body: The body of the Service for the create action.
        :return: The Service resource.
        """
        if not body:
            self.log.error("Body must be provided for create action.")
            return None
        try:
            v1_service = self.core_v1_client.create_namespaced_service(namespace=self.namespace, body=body)
            self.log.info(f"Service {v1_service.metadata.name} created.")
            return v1_service
        except client.exceptions.ApiException as e:
            self.log.error(f"Exception when creating service: {e}")
            return None

    def delete_service(self, name: str) -> client.V1Status | None:
        """
        Delete a Service resource in Kubernetes.

        :param name: The name of the Service.
        :return: The status of the delete operation.
        """
        try:
            delete_response = self.core_v1_client.delete_namespaced_service(name=name, namespace=self.namespace)
            self.log.info(f"Service {name} deleted.")
            return delete_response
        except client.exceptions.ApiException as e:
            self.log.error(f"Exception when deleting service: {e}")
            return None

    def get_endpoints(self, name: str) -> client.V1Endpoints | None:
        """
        Retrieve an Endpoints resource in Kubernetes.

        :param name: The name of the Endpoints.
        :return: The Endpoints resource.
        """
        try:
            response = self.core_v1_client.read_namespaced_endpoints(name=name, namespace=self.namespace)
            self.log.info(f"Retrieved endpoints {name} from namespace {self.namespace}.")
            return response
        except client.exceptions.ApiException as e:
            self.log.error(f"Exception when getting endpoints: {e}")
            return None

    def create_endpoints(self, name: str, body: dict[str, Any]) -> client.V1Endpoints | None:
        """
        Create an Endpoints resource in Kubernetes.

        :param name: The name of the Endpoints.
        :param body: The body of the Endpoints for the create action.
        :return: The Endpoints resource.
        """
        if not body:
            self.log.error("Body must be provided for create action.")
            return None
        try:
            response = self.core_v1_client.create_namespaced_endpoints(namespace=self.namespace, body=body)
            self.log.info(f"Created endpoints {name} in namespace {self.namespace}.")
            return response
        except client.exceptions.ApiException as e:
            self.log.error(f"Exception when creating endpoints: {e}")
            return None

    def delete_endpoints(self, name: str) -> client.V1Status | None:
        """
        Delete an Endpoints resource in Kubernetes.

        :param name: The name of the Endpoints.
        :return: The status of the delete operation.
        """
        try:
            response = self.core_v1_client.delete_namespaced_endpoints(name=name, namespace=self.namespace)
            self.log.info(f"Deleted endpoints {name} from namespace {self.namespace}.")
            return response
        except client.exceptions.ApiException as e:
            self.log.error(f"Exception when deleting endpoints: {e}")
            return None

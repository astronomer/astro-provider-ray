from __future__ import annotations

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

    :param conn_id: The connection ID to use when fetching connection info.
    :param xcom_dashboard_url: The URL of the Ray dashboard (optional).
    """

    conn_name_attr = "ray_conn_id"
    default_conn_name = "ray_default"
    conn_type = "ray"
    hook_name = "Ray"

    DEFAULT_NAMESPACE = "default"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """
        Return custom field behaviour for the connection form.

        :return: A dictionary specifying custom field behaviour.
        """
        return {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """
        Return connection widgets to add to connection form.

        :return: A dictionary of connection form widgets.
        """
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, PasswordField, StringField

        return {
            "address": StringField(lazy_gettext("Ray dashboard url"), widget=BS3TextFieldWidget()),
            "create_cluster_if_needed": BooleanField(lazy_gettext("Create cluster if needed")),
            "cookies": StringField(lazy_gettext("Cookies"), widget=BS3TextFieldWidget()),
            "metadata": StringField(lazy_gettext("Metadata"), widget=BS3TextFieldWidget()),
            "headers": StringField(lazy_gettext("Headers"), widget=BS3TextFieldWidget()),
            "verify": BooleanField(lazy_gettext("Verify")),
            "kube_config_path": StringField(lazy_gettext("Kube config path"), widget=BS3TextFieldWidget()),
            "kube_config": PasswordField(lazy_gettext("Kube config (JSON format)"), widget=BS3PasswordFieldWidget()),
            "namespace": StringField(lazy_gettext("Namespace"), widget=BS3TextFieldWidget()),
            "cluster_context": StringField(lazy_gettext("Cluster context"), widget=BS3TextFieldWidget()),
            "disable_verify_ssl": BooleanField(lazy_gettext("Disable SSL")),
            "disable_tcp_keepalive": BooleanField(lazy_gettext("Disable TCP keepalive")),
        }

    def __init__(
        self,
        conn_id: str = default_conn_name,
        xcom_dashboard_url: str | None = None,
    ) -> None:
        super().__init__(conn_id=conn_id)
        self.conn_id = conn_id
        self.xcom_dashboard_url = xcom_dashboard_url

        self.address = self._get_field("address") or self.xcom_dashboard_url or os.getenv("RAY_ADDRESS")
        self.log.info(f"Ray cluster address is: {self.address}")
        self.create_cluster_if_needed = self._get_field("create_cluster_if_needed") or False
        self.cookies = self._get_field("cookies")
        self.metadata = self._get_field("metadata")
        self.headers = self._get_field("headers")
        self.verify = self._get_field("verify") or False
        self.ray_client_instance = None

        self.namespace = self.get_namespace() or self.DEFAULT_NAMESPACE
        self.kubeconfig: str | None = None
        self.in_cluster: bool | None = None
        self.client_configuration = None
        self.config_file = None
        self.disable_verify_ssl = None
        self.disable_tcp_keepalive = None
        self._is_in_cluster: bool | None = None

        self.cluster_context = self._get_field("cluster_context")
        self.kubeconfig_path = self._get_field("kube_config_path")
        self.kubeconfig_content = self._get_field("kube_config")

        self._setup_kubeconfig(self.kubeconfig_path, self.kubeconfig_content, self.cluster_context)

    def _setup_kubeconfig(
        self, kubeconfig_path: str | None, kubeconfig_content: str | None, cluster_context: str | None
    ) -> None:
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

        :return: An instance of JobSubmissionClient.
        :raises AirflowException: If the connection fails.
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
        """
        Deletes a job from the Ray cluster.

        :param job_id: The ID of the job to delete.
        :return: The result of the delete operation.
        """
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
        """
        Load YAML content from a file path or URL.

        :param path_or_link: The file path or URL of the YAML content.
        :return: The loaded YAML content.
        """
        if path_or_link.startswith("http"):
            response = requests.get(path_or_link)
            response.raise_for_status()
            return yaml.safe_load(response.text)

        with open(path_or_link) as f:
            return yaml.safe_load(f)

    def _is_port_open(self, host: str, port: int) -> bool:
        """
        Check if a port is open on a given host.

        :param host: The hostname or IP address to check.
        :param port: The port number to check.
        :return: True if the port is open, False otherwise.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            try:
                s.connect((host, port))
                return True
            except OSError:
                return False

    def _get_service(self, name: str, namespace: str) -> client.V1Service:
        """
        Get the Kubernetes service.

        :param name: The name of the service.
        :param namespace: The namespace of the service.
        :return: The Kubernetes service object.
        :raises AirflowException: If the service is not found.
        """
        try:
            return self.core_v1_client.read_namespaced_service(name, namespace)
        except client.exceptions.ApiException as e:
            self.log.error(f"Error getting service {name}: {e}")
            raise AirflowException(f"Service {name} not found")

    def _get_load_balancer_details(self, service: client.V1Service) -> dict[str, Any] | None:
        """
        Extract LoadBalancer details from the service.

        :param service: The Kubernetes service object.
        :return: A dictionary containing LoadBalancer details if available, None otherwise.
        """
        if service.status.load_balancer.ingress:
            ingress: client.V1LoadBalancerIngress = service.status.load_balancer.ingress[0]
            ip: str | None = ingress.ip
            hostname: str | None = ingress.hostname
            if ip or hostname:
                ports: list[dict[str, Any]] = [{"name": port.name, "port": port.port} for port in service.spec.ports]
                return {"ip": ip, "hostname": hostname, "ports": ports}
        return None

    def _check_load_balancer_readiness(self, lb_details: dict[str, Any]) -> str | None:
        """
        Check if the LoadBalancer is ready by testing port connectivity.

        :param lb_details: Dictionary containing LoadBalancer details.
        :return: The working address (IP or hostname) if ready, None otherwise.
        """
        ip: str | None = lb_details["ip"]
        hostname: str | None = lb_details["hostname"]

        for port_info in lb_details["ports"]:
            port = port_info["port"]
            if ip and self._is_port_open(ip, port):
                return ip
            if hostname and self._is_port_open(hostname, port):
                return hostname

        return None

    def wait_for_load_balancer(
        self,
        service_name: str,
        namespace: str = "default",
        max_retries: int = 30,
        retry_interval: int = 40,
    ) -> dict[str, Any]:
        """
        Wait for the LoadBalancer to be ready and return its details.

        :param service_name: The name of the LoadBalancer service.
        :param namespace: The namespace of the service.
        :param max_retries: Maximum number of retries.
        :param retry_interval: Interval between retries in seconds.
        :return: A dictionary containing LoadBalancer details.
        :raises AirflowException: If the LoadBalancer does not become ready within the specified retries.
        """
        for attempt in range(1, max_retries + 1):
            self.log.info(f"Attempt {attempt}: Checking LoadBalancer status...")

            try:
                service: client.V1Service = self._get_service(service_name, namespace)
                lb_details: dict[str, Any] | None = self._get_load_balancer_details(service)

                if not lb_details:
                    self.log.info("LoadBalancer details not available yet.")
                    continue

                working_address = self._check_load_balancer_readiness(lb_details)

                if working_address:
                    self.log.info("LoadBalancer is ready.")
                    lb_details["working_address"] = working_address
                    return lb_details

                self.log.info("LoadBalancer is not ready yet. Waiting...")

            except AirflowException:
                self.log.info("LoadBalancer service is not available yet...")

            time.sleep(retry_interval)

        raise AirflowException(f"LoadBalancer did not become ready after {max_retries} attempts")

    def _run_bash_command(self, command: str, env: dict[str, str] | None = None) -> tuple[str | None, str | None]:
        """
        Run a bash command with optional environment variables.

        :param command: The bash command to run.
        :param env: Optional dictionary of environment variables.
        :return: A tuple containing the command's stdout and stderr.
        """
        custom_env = os.environ.copy()
        if env:
            custom_env.update(env)

        try:
            result = subprocess.run(command, shell=True, check=True, text=True, capture_output=True, env=custom_env)
            self.log.info("Standard Output: %s", result.stdout)
            self.log.info("Standard Error: %s", result.stderr)
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

        :param version: The version of KubeRay operator to install.
        :param env: Optional dictionary of environment variables.
        :return: A tuple containing the installation output and error (if any).
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

        :return: A tuple containing the uninstallation output and error (if any).
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
        :return: The DaemonSet resource if found, None otherwise.
        """
        try:
            api_response = self.apps_v1_client.read_namespaced_daemon_set(name, self.namespace)
            self.log.info(f"DaemonSet {api_response.metadata.name} retrieved.")
            return api_response
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.log.warning(f"DaemonSet not found: {e}")
                return None
            else:
                self.log.error(f"Exception when getting DaemonSet: {e}")
                raise

    def create_daemon_set(self, name: str, body: dict[str, Any]) -> client.V1DaemonSet | None:
        """
        Create a DaemonSet resource in Kubernetes.

        :param name: The name of the DaemonSet.
        :param body: The body of the DaemonSet for the create action.
        :return: The created DaemonSet resource if successful, None otherwise.
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
        :return: The status of the delete operation if successful, None otherwise.
        """
        try:
            delete_response = self.apps_v1_client.delete_namespaced_daemon_set(name=name, namespace=self.namespace)
            self.log.info(f"DaemonSet {name} deleted.")
            return delete_response
        except client.exceptions.ApiException as e:
            self.log.error(f"Exception when deleting DaemonSet: {e}")
            return None

from __future__ import annotations

import os
import socket
import time
from datetime import timedelta
from functools import cached_property
from typing import Any, Dict, Optional, Union

import requests
import yaml
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodOperatorHookProtocol
from airflow.utils.context import Context
from kubernetes import client
from ray.job_submission import JobStatus

from ray_provider.hooks.ray import RayHook
from ray_provider.triggers.ray import RayJobTrigger


class SetupRayCluster(BaseOperator):

    def __init__(
        self,
        *,
        conn_id: str,
        ray_cluster_yaml: str,
        ray_svc_yaml: str,
        use_gpu: bool = False,
        kuberay_version: str = "1.0.0",
        gpu_device_plugin_yaml: Optional[
            str
        ] = "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ray_cluster_yaml = ray_cluster_yaml
        self.ray_svc_yaml = ray_svc_yaml
        self.use_gpu = use_gpu
        self.kuberay_version = kuberay_version
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml

        if not os.path.isfile(ray_cluster_yaml):
            raise AirflowException(f"The specified Ray cluster YAML file does not exist: {ray_cluster_yaml}")
        if not ray_cluster_yaml.endswith(".yaml") and not ray_cluster_yaml.endswith(".yml"):
            raise AirflowException("The specified Ray cluster YAML file must have a .yaml or .yml extension.")

    @cached_property
    def hook(self) -> RayHook:
        return RayHook(conn_id=self.conn_id)

    def _load_yaml_content(self, path_or_link: str) -> Union[dict, list, None]:
        if path_or_link.startswith("http"):
            response = requests.get(path_or_link)
            response.raise_for_status()
            return yaml.safe_load(response.text)

        with open(path_or_link) as f:
            return yaml.safe_load(f)

    def _wait_for_external_dns(
        self, service_name: str, max_retries: int = 30, retry_interval: int = 40
    ) -> Optional[str]:
        for attempt in range(max_retries):
            self.log.info(f"Attempt {attempt + 1}: Checking for service's external DNS name...")
            service = self.hook.get_service(name=service_name)
            if service.status.load_balancer.ingress and service.status.load_balancer.ingress[0].hostname:
                external_dns = service.status.load_balancer.ingress[0].hostname
                self.log.info(f"External DNS name found: {external_dns}")
                return external_dns
            self.log.info("External DNS name not yet available, waiting...")
            time.sleep(retry_interval)
        return None

    def _is_port_open(self, host, port):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        try:
            s.connect((host, port))
            s.close()
            return True
        except OSError:
            return False

    def _wait_for_endpoints(self, external_dns: str, service_name: str, max_retries=30, retry_interval=40) -> bool:
        for attempt in range(max_retries):
            endpoints = self.hook.get_endpoints(name=service_name)
            if endpoints and endpoints.subsets and all([subset.addresses for subset in endpoints.subsets]):
                all_ports_open = True
                for subset in endpoints.subsets:
                    for port in subset.ports:
                        if not self._is_port_open(external_dns, port.port):
                            all_ports_open = False
                            self.log.info(f"Port {port.port} on {external_dns} is not open yet.")
                            break
                    if not all_ports_open:
                        break

                if all_ports_open:
                    self.log.info("All associated pods are ready.")
                    return True
            self.log.info(f"Pods not ready, waiting... (Attempt {attempt + 1})")
            time.sleep(retry_interval)
        return False

    def _construct_service_urls(self, service, external_dns: str) -> Dict[str, str]:
        return {port.name: f"http://{external_dns}:{port.port}" for port in service.spec.ports}

    def execute(self, context: Context):
        self.hook.install_kuberay_operator(version=self.kuberay_version)

        self.log.info("Loading yaml content for Ray cluster CRD...")
        cluster_spec = self._load_yaml_content(self.ray_cluster_yaml)

        kind = cluster_spec["kind"]
        plural = kind.lower() + "s" if kind == "RayCluster" else kind
        name = cluster_spec["metadata"]["name"]
        namespace = self.hook.get_namespace()
        api_version = cluster_spec["apiVersion"]
        group, version = api_version.split("/") if "/" in api_version else ("", api_version)

        try:
            self.hook.get_custom_object(group=group, version=version, plural=plural, name=name, namespace=namespace)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.log.info(f"Creating a Ray cluster: {name}")
                self.hook.create_custom_object(
                    group=group, version=version, namespace=namespace, plural=plural, body=cluster_spec
                )
            else:
                self.log.error(f"Exception when checking if {kind} '{name}' exists: {e}")
                raise e

        if self.use_gpu:
            gpu_driver = self._load_yaml_content(self.gpu_device_plugin_yaml)
            gpu_driver_name = gpu_driver["metadata"]["name"]

            if not self.hook.get_daemon_set(gpu_driver_name):
                self.log.info("Creating DaemonSet for NVIDIA device plugin...")
                self.hook.create_daemon_set(gpu_driver_name, gpu_driver)

        urls = None
        if self.ray_svc_yaml:
            ray_sv_spec = self._load_yaml_content(self.ray_svc_yaml)
            service_name = ray_sv_spec["metadata"]["name"]
            self.log.info(f"Service name: {service_name}")

            existing_service = self.hook.get_service(service_name)
            if not existing_service:
                self.log.info(f"Creating service name: {service_name}")
                self.hook.create_service(service_name, ray_sv_spec)

            external_dns = self._wait_for_external_dns(service_name=service_name)
            if not external_dns:
                self.log.error("Failed to find the external DNS name for the service within the expected time.")
                raise AirflowException("Failed to find the external DNS name for the service within the expected time.")

            if not self._wait_for_endpoints(external_dns, service_name=service_name):
                self.log.error("Pods failed to become ready within the expected time.")
                raise AirflowException("Pods failed to become ready within the expected time.")

            urls = self._construct_service_urls(existing_service, external_dns)

            if urls:
                for key, value in urls.items():
                    context["task_instance"].xcom_push(key=key, value=value)
            else:
                self.log.info("No URLs to push to XCom.")

        return urls


class DeleteRayCluster(BaseOperator):

    def __init__(
        self,
        *,
        conn_id: str,
        ray_cluster_yaml: str,
        ray_svc_yaml: str,
        use_gpu: bool = False,
        gpu_device_plugin_yaml: Optional[
            str
        ] = "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml",
        **kwargs,
    ):

        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ray_cluster_yaml = ray_cluster_yaml
        self.ray_svc_yaml = ray_svc_yaml
        self.use_gpu = use_gpu
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml

        if not os.path.isfile(ray_cluster_yaml):
            raise AirflowException(f"The specified Ray cluster YAML file does not exist: {ray_cluster_yaml}")
        elif not ray_cluster_yaml.endswith(".yaml") and not ray_cluster_yaml.endswith(".yml"):
            raise AirflowException("The specified Ray cluster YAML file must have a .yaml or .yml extension.")

    @cached_property
    def hook(self) -> PodOperatorHookProtocol:
        return RayHook(conn_id=self.conn_id)

    def _load_yaml_content(self, path_or_link: str) -> Union[dict[Any, Any], list, None]:
        if path_or_link.startswith("http"):
            response = requests.get(path_or_link)
            response.raise_for_status()
            return yaml.safe_load(response.text)
        with open(path_or_link) as f:
            return yaml.safe_load(f)

    def execute(self, context: Context):
        # Delete Kubernetes service for Ray
        if self.ray_svc_yaml:
            ray_sv_spec = self._load_yaml_content(self.ray_svc_yaml)

            service_name = ray_sv_spec["metadata"]["name"]

            self.log.info(f"Service name: {service_name}")

            if self.hook.get_service(service_name):
                self.log.info(f"Deleting service name: {service_name}")
                self.hook.delete_service(service_name)

        # If GPU is to be used, delete NVIDIA device plugin resource
        if self.use_gpu:
            gpu_driver = self._load_yaml_content(self.gpu_device_plugin_yaml)
            gpu_driver_name = gpu_driver["metadata"]["name"]

            if not self.hook.get_daemon_set(gpu_driver_name):
                self.log.info("Deleting DaemonSet for NVIDIA device plugin...")
                self.hook.delete_daemon_set(gpu_driver_name)

        self.log.info("Loading yaml content for Ray cluster CRD...")
        cluster_spec = self._load_yaml_content(self.ray_cluster_yaml)

        kind = cluster_spec["kind"]
        plural = kind.lower() + "s" if kind == "RayCluster" else kind
        name = cluster_spec["metadata"]["name"]
        namespace = self.hook.get_namespace()
        api_version = cluster_spec["apiVersion"]
        group, version = api_version.split("/") if "/" in api_version else ("", api_version)

        try:
            if self.hook.get_custom_object(group=group, version=version, plural=plural, name=name, namespace=namespace):
                self.hook.delete_custom_object(
                    group=group, version=version, name=name, namespace=namespace, plural=plural
                )
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.log.info(f"Ray cluster: {name} not found. Skipping the delete step!")
            else:
                print(f"Exception when checking if {kind} '{name}' exists: {e}")
                raise e
        except ValueError as e:
            print(e)
            raise e

        # Uninstall KubeRay operator
        self.hook.uninstall_kuberay_operator()

        return


class SubmitRayJob(BaseOperator):
    """
    Operator to submit and monitor a Ray job.

    This operator handles the submission of a Ray job and monitors its status until completion.
    It supports deferring execution and resuming based on job status changes.

    :param entrypoint: Required. The command or script to execute.
    :param runtime_env: Required. The runtime environment for the job.
    :param num_cpus: Optional. Number of CPUs required for the job. Defaults to 0.
    :param num_gpus: Optional. Number of GPUs required for the job. Defaults to 0.
    :param memory: Optional. Amount of memory required for the job. Defaults to 0.
    :param resources: Optional. Additional resources required for the job. Defaults to None.
    :param timeout: Optional. Maximum time to wait for job completion in seconds. Defaults to 600 seconds.

    :raises AirflowException: If the job fails or is cancelled, or if an unexpected status is encountered.
    """

    template_fields = ("conn_id", "entrypoint", "runtime_env", "num_cpus", "num_gpus", "memory", "xcom_task_key")

    def __init__(
        self,
        *,
        conn_id: str,
        entrypoint: str,
        runtime_env: dict[str, Any],
        num_cpus: int = 0,
        num_gpus: int = 0,
        memory: int | float = 0,
        resources: dict[str, Any] | None = None,
        timeout: int = 600,
        xcom_task_key: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.entrypoint = entrypoint
        self.runtime_env = runtime_env
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.resources = resources
        self.timeout = timeout
        self.xcom_task_key = xcom_task_key
        self.dashboard_url = None
        self.job_id = None
        self.status_to_wait = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

    def on_kill(self) -> bool:
        if self.hook:
            return self.hook.delete_job(self.job_id)

    @cached_property
    def hook(self) -> PodOperatorHookProtocol:
        return RayHook(conn_id=self.conn_id, xcom_dashboard_url=self.dashboard_url)

    def execute(self, context: Context) -> str:

        if self.xcom_task_key:
            task, key = self.xcom_task_key.split(".")
            ti = context["ti"]
            self.dashboard_url = ti.xcom_pull(task_ids=task, key=key)
            self.log.info(f"Dashboard URL retrieved from XCom: {self.dashboard_url}")

        self.job_id = self.hook.submit_job(
            entrypoint=self.entrypoint,
            runtime_env=self.runtime_env,  # https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments
            entrypoint_num_cpus=self.num_cpus,
            entrypoint_num_gpus=self.num_gpus,
            entrypoint_memory=self.memory,
            entrypoint_resources=self.resources,
        )

        self.log.info(f"Ray job submitted with id: {self.job_id}")

        current_status = self.hook.get_job_status(self.job_id)
        self.log.info(f"Current job status for {self.job_id} is: {current_status}")
        if current_status not in self.status_to_wait:
            self.log.info("Deferring the polling to RayJobTrigger...")
            self.defer(
                timeout=timedelta(hours=self.timeout),
                trigger=RayJobTrigger(
                    job_id=self.job_id, conn_id=self.conn_id, xcom_dashboard_url=self.dashboard_url, poll_interval=10
                ),
                method_name="execute_complete",
            )
        elif current_status == JobStatus.SUCCEEDED:
            self.log.info("Job %s completed successfully", self.job_id)
            return
        elif current_status == JobStatus.FAILED:
            raise AirflowException(f"Job failed:\n{self.job_id}")
        elif current_status == JobStatus.STOPPED:
            raise AirflowException(f"Job was cancelled:\n{self.job_id}")
        else:
            raise Exception(f"Encountered unexpected state `{current_status}` for job_id `{self.job_id}`")

        return self.job_id

    def execute_complete(self, context: Context, event: Any = None) -> None:
        if event["status"] == "error" or event["status"] == "cancelled":
            self.log.info(f"Ray job {self.job_id} execution not completed...")
            raise AirflowException(event["message"])
        elif event["status"] == "success":
            self.log.info(f"Ray job {self.job_id} execution succeeded ...")
            return None

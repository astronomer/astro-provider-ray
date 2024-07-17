from __future__ import annotations

import os
from datetime import timedelta
from functools import cached_property
from typing import Any

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
        conn_id: str,
        ray_cluster_yaml: str,
        use_gpu: bool = False,
        kuberay_version: str = "1.0.0",
        gpu_device_plugin_yaml: str = "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ray_cluster_yaml = ray_cluster_yaml
        self.use_gpu = use_gpu
        self.kuberay_version = kuberay_version
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml

        self._validate_yaml_file(ray_cluster_yaml)

    @cached_property
    def hook(self) -> RayHook:
        return RayHook(conn_id=self.conn_id)

    def _validate_yaml_file(self, yaml_file: str) -> None:
        if not os.path.isfile(yaml_file):
            raise AirflowException(f"The specified YAML file does not exist: {yaml_file}")
        elif not yaml_file.endswith((".yaml", ".yml")):
            raise AirflowException("The specified YAML file must have a .yaml or .yml extension.")

        try:
            with open(yaml_file) as stream:
                yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise AirflowException(f"The specified YAML file is not valid YAML: {exc}")

    def execute(self, context: Context) -> None:

        self.hook.install_kuberay_operator(version=self.kuberay_version)

        self.log.info("Loading yaml content for Ray cluster CRD...")
        cluster_spec = self.hook.load_yaml_content(self.ray_cluster_yaml)

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
            gpu_driver = self.hook.load_yaml_content(self.gpu_device_plugin_yaml)
            gpu_driver_name = gpu_driver["metadata"]["name"]

            if not self.hook.get_daemon_set(gpu_driver_name):
                self.log.info("Creating DaemonSet for NVIDIA device plugin...")
                self.hook.create_daemon_set(gpu_driver_name, gpu_driver)

        lb_details: dict[str, Any] = self.hook.wait_for_load_balancer(
            service_name=name + "-head-svc", namespace=namespace
        )

        if lb_details:
            self.log.info(lb_details)
            dns = lb_details["ip_or_hostname"]
            for port in lb_details["ports"]:
                url = "http://" + dns + ":" + str(port["port"])
                context["task_instance"].xcom_push(key=port["name"], value=url)
        else:
            self.log.info("No URLs to push to XCom.")

        return


class DeleteRayCluster(BaseOperator):

    def __init__(
        self,
        conn_id: str,
        ray_cluster_yaml: str,
        use_gpu: bool = False,
        gpu_device_plugin_yaml: str = "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml",
        **kwargs: Any,
    ) -> None:

        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ray_cluster_yaml = ray_cluster_yaml
        self.use_gpu = use_gpu
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml

        self._validate_yaml_file(ray_cluster_yaml)

    @cached_property
    def hook(self) -> PodOperatorHookProtocol:
        return RayHook(conn_id=self.conn_id)

    def _validate_yaml_file(self, yaml_file: str) -> None:
        if not os.path.isfile(yaml_file):
            raise AirflowException(f"The specified YAML file does not exist: {yaml_file}")
        elif not yaml_file.endswith((".yaml", ".yml")):
            raise AirflowException("The specified YAML file must have a .yaml or .yml extension.")

    def _delete_gpu_daemonset(self) -> None:
        gpu_driver = self.hook.load_yaml_content(self.gpu_device_plugin_yaml)
        gpu_driver_name = gpu_driver["metadata"]["name"]

        if not self.hook.get_daemon_set(gpu_driver_name):
            self.log.info("Deleting DaemonSet for NVIDIA device plugin...")
            self.hook.delete_daemon_set(gpu_driver_name)

    def _delete_ray_cluster(self) -> None:
        self.log.info("Loading yaml content for Ray cluster CRD...")
        cluster_spec = self.hook.load_yaml_content(self.ray_cluster_yaml)

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
                self.log.error(f"Exception when checking if {kind} '{name}' exists: {e}")
                raise e
        except ValueError as e:
            self.log.error(e)
            raise e

    def execute(self, context: Context) -> None:
        try:
            if self.use_gpu:
                self._delete_gpu_daemonset()
            self._delete_ray_cluster()
            self.hook.uninstall_kuberay_operator()
        except Exception as e:
            self.log.error(f"Error executing task: {e}")
            raise AirflowException(f"Task execution failed: {e}")


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
        num_cpus: int | float = 0,
        num_gpus: int | float = 0,
        memory: int | float = 0,
        resources: dict[str, Any] | None = None,
        timeout: int = 600,
        poll_interval: int = 60,
        xcom_task_key: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id: str = conn_id
        self.entrypoint: str = entrypoint
        self.runtime_env: dict[str, Any] = runtime_env
        self.num_cpus: int | float = num_cpus
        self.num_gpus: int | float = num_gpus
        self.memory: int | float = memory
        self.ray_resources: dict[str, Any] | None = resources
        self.timeout: int | float = timeout
        self.poll_interval: int = poll_interval
        self.xcom_task_key: str | None = xcom_task_key
        self.dashboard_url: str | None = None
        self.job_id: str = ""
        self.terminal_state = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

    def on_kill(self) -> None:
        if self.hook:
            self.hook.delete_ray_job(self.job_id)

    @cached_property
    def hook(self) -> PodOperatorHookProtocol:
        return RayHook(conn_id=self.conn_id, xcom_dashboard_url=self.dashboard_url)

    def execute(self, context: Context) -> str:

        if self.xcom_task_key:
            task, key = self.xcom_task_key.split(".")
            ti = context["ti"]
            self.dashboard_url = ti.xcom_pull(task_ids=task, key=key)
            self.log.info(f"Dashboard URL retrieved from XCom: {self.dashboard_url}")

        self.job_id = self.hook.submit_ray_job(
            entrypoint=self.entrypoint,
            runtime_env=self.runtime_env,  # https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments
            entrypoint_num_cpus=self.num_cpus,
            entrypoint_num_gpus=self.num_gpus,
            entrypoint_memory=self.memory,
            entrypoint_resources=self.ray_resources,
        )

        self.log.info(f"Ray job submitted with id: {self.job_id}")

        current_status = self.hook.get_ray_job_status(self.job_id)
        self.log.info(f"Current job status for {self.job_id} is: {current_status}")
        if current_status not in self.terminal_state:
            self.log.info("Deferring the polling to RayJobTrigger...")
            self.defer(
                timeout=timedelta(hours=self.timeout),
                trigger=RayJobTrigger(
                    job_id=self.job_id,
                    conn_id=self.conn_id,
                    xcom_dashboard_url=self.dashboard_url,
                    poll_interval=self.poll_interval,
                ),
                method_name="execute_complete",
            )
        elif current_status == JobStatus.SUCCEEDED:
            self.log.info("Job %s completed successfully", self.job_id)
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

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
    """
    Operator to set up a Ray cluster on Kubernetes.

    :param conn_id: The connection ID for the Ray cluster.
    :param ray_cluster_yaml: Path to the YAML file defining the Ray cluster.
    :param use_gpu: Whether to use GPU for the cluster.
    :param kuberay_version: Version of KubeRay to install.
    :param gpu_device_plugin_yaml: URL or path to the NVIDIA GPU device plugin YAML.
    """

    def __init__(
        self,
        conn_id: str,
        ray_cluster_yaml: str,
        use_gpu: bool = False,
        kuberay_version: str = "1.0.0",
        gpu_device_plugin_yaml: str = "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml",
        update_if_exists: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.ray_cluster_yaml = ray_cluster_yaml
        self.use_gpu = use_gpu
        self.kuberay_version = kuberay_version
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml
        self.update_if_exists = update_if_exists

        self._validate_yaml_file(ray_cluster_yaml)

    @cached_property
    def hook(self) -> RayHook:
        """Lazily initialize and return the RayHook."""
        return RayHook(conn_id=self.conn_id)

    def _validate_yaml_file(self, yaml_file: str) -> None:
        """Validate the existence and format of the YAML file."""
        if not os.path.isfile(yaml_file):
            raise AirflowException(f"The specified YAML file does not exist: {yaml_file}")
        if not yaml_file.endswith((".yaml", ".yml")):
            raise AirflowException("The specified YAML file must have a .yaml or .yml extension.")

        try:
            with open(yaml_file) as stream:
                yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise AirflowException(f"The specified YAML file is not valid YAML: {exc}")

    def _create_or_update_cluster(
        self, group: str, version: str, plural: str, name: str, namespace: str, cluster_spec: dict[str, Any]
    ) -> None:
        """Create or update the Ray cluster based on the cluster specification."""
        try:
            self.hook.get_custom_object(group=group, version=version, plural=plural, name=name, namespace=namespace)
            if self.update_if_exists:
                self.log.info(f"Updating existing Ray cluster: {name}")
                self.hook.custom_object_client.patch_namespaced_custom_object(
                    group=group, version=version, namespace=namespace, plural=plural, name=name, body=cluster_spec
                )
        except client.exceptions.ApiException as e:
            if e.status == 404:
                self.log.info(f"Creating new Ray cluster: {name}")
                self.hook.create_custom_object(
                    group=group, version=version, namespace=namespace, plural=plural, body=cluster_spec
                )
            else:
                raise AirflowException(f"Error accessing Ray cluster '{name}': {e}")

    def _setup_gpu_driver(self) -> None:
        """Set up the NVIDIA GPU device plugin if GPU is enabled."""
        gpu_driver = self.hook.load_yaml_content(self.gpu_device_plugin_yaml)
        gpu_driver_name = gpu_driver["metadata"]["name"]

        if not self.hook.get_daemon_set(gpu_driver_name):
            self.log.info("Creating DaemonSet for NVIDIA device plugin...")
            self.hook.create_daemon_set(gpu_driver_name, gpu_driver)

    def _setup_load_balancer(self, name: str, namespace: str, context: Context) -> None:
        """Set up the load balancer and push URLs to XCom."""
        lb_details: dict[str, Any] = self.hook.wait_for_load_balancer(
            service_name=f"{name}-head-svc", namespace=namespace
        )

        if lb_details:
            self.log.info(lb_details)
            dns = lb_details["working_address"]
            for port in lb_details["ports"]:
                url = f"http://{dns}:{port['port']}"
                context["task_instance"].xcom_push(key=port["name"], value=url)
        else:
            self.log.info("No URLs to push to XCom.")

    def execute(self, context: Context) -> None:
        """Execute the operator to set up the Ray cluster."""
        try:
            self.log.info("::group::Add KubeRay operator")
            self.hook.install_kuberay_operator(version=self.kuberay_version)
            self.log.info("::endgroup::")

            self.log.info("::group::Create Ray Cluster")
            self.log.info("Loading yaml content for Ray cluster CRD...")
            cluster_spec = self.hook.load_yaml_content(self.ray_cluster_yaml)

            kind = cluster_spec["kind"]
            plural = f"{kind.lower()}s" if kind == "RayCluster" else kind
            name = cluster_spec["metadata"]["name"]
            namespace = self.hook.get_namespace()
            api_version = cluster_spec["apiVersion"]
            group, version = api_version.split("/") if "/" in api_version else ("", api_version)

            self._create_or_update_cluster(group, version, plural, name, namespace, cluster_spec)
            self.log.info("::endgroup::")

            if self.use_gpu:
                self._setup_gpu_driver()

            self.log.info("::group::Setup Load Balancer service")
            self._setup_load_balancer(name, namespace, context)
            self.log.info("::endgroup::")

        except Exception as e:
            self.log.error(f"Error setting up Ray cluster: {e}")
            raise AirflowException(f"Failed to set up Ray cluster: {e}")


class DeleteRayCluster(BaseOperator):
    """
    Operator to delete a Ray cluster from Kubernetes.

    :param conn_id: The connection ID for the Ray cluster.
    :param ray_cluster_yaml: Path to the YAML file defining the Ray cluster.
    :param use_gpu: Whether GPU was used for the cluster.
    :param gpu_device_plugin_yaml: URL or path to the NVIDIA GPU device plugin YAML.
    """

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
        """Lazily initialize and return the RayHook."""
        return RayHook(conn_id=self.conn_id)

    def _validate_yaml_file(self, yaml_file: str) -> None:
        """Validate the existence and format of the YAML file."""
        if not os.path.isfile(yaml_file):
            raise AirflowException(f"The specified YAML file does not exist: {yaml_file}")
        if not yaml_file.endswith((".yaml", ".yml")):
            raise AirflowException("The specified YAML file must have a .yaml or .yml extension.")

    def _delete_gpu_daemonset(self) -> None:
        """Delete the NVIDIA GPU device plugin DaemonSet if it exists."""
        gpu_driver = self.hook.load_yaml_content(self.gpu_device_plugin_yaml)
        gpu_driver_name = gpu_driver["metadata"]["name"]

        if self.hook.get_daemon_set(gpu_driver_name):
            self.log.info("Deleting DaemonSet for NVIDIA device plugin...")
            self.hook.delete_daemon_set(gpu_driver_name)

    def _delete_ray_cluster(self) -> None:
        """Delete the Ray cluster based on the cluster specification."""
        self.log.info("Loading yaml content for Ray cluster CRD...")
        cluster_spec = self.hook.load_yaml_content(self.ray_cluster_yaml)

        kind = cluster_spec["kind"]
        plural = f"{kind.lower()}s" if kind == "RayCluster" else kind
        name = cluster_spec["metadata"]["name"]
        namespace = self.hook.get_namespace()
        api_version = cluster_spec["apiVersion"]
        group, version = api_version.split("/") if "/" in api_version else ("", api_version)

        try:
            if self.hook.get_custom_object(group=group, version=version, plural=plural, name=name, namespace=namespace):
                self.hook.delete_custom_object(
                    group=group, version=version, name=name, namespace=namespace, plural=plural
                )
                self.log.info(f"Deleted Ray cluster: {name}")
            else:
                self.log.info(f"Ray cluster: {name} not found. Skipping the delete step.")
        except client.exceptions.ApiException as e:
            if e.status != 404:
                raise AirflowException(f"Error deleting Ray cluster '{name}': {e}")

    def execute(self, context: Context) -> None:
        """Execute the operator to delete the Ray cluster."""
        try:
            if self.use_gpu:
                self._delete_gpu_daemonset()
            self.log.info("::group:: Delete Ray Cluster")
            self._delete_ray_cluster()
            self.log.info("::endgroup::")
            self.hook.uninstall_kuberay_operator()
        except Exception as e:
            self.log.error(f"Error deleting Ray cluster: {e}")
            raise AirflowException(f"Failed to delete Ray cluster: {e}")


class SubmitRayJob(BaseOperator):
    """
    Operator to submit and monitor a Ray job.

    This operator handles the submission of a Ray job and monitors its status until completion.
    It supports deferring execution and resuming based on job status changes.

    :param conn_id: The connection ID for the Ray cluster.
    :param entrypoint: The command or script to execute.
    :param runtime_env: The runtime environment for the job.
    :param num_cpus: Number of CPUs required for the job. Defaults to 0.
    :param num_gpus: Number of GPUs required for the job. Defaults to 0.
    :param memory: Amount of memory required for the job. Defaults to 0.
    :param resources: Additional resources required for the job. Defaults to None.
    :param job_timeout_seconds: Maximum time to wait for job completion in seconds. Defaults to 600 seconds.
    :param poll_interval: Interval between job status checks in seconds. Defaults to 60 seconds.
    :param xcom_task_key: XCom key to retrieve dashboard URL. Defaults to None.
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
        fetch_logs: bool = True,
        wait_for_completion: bool = True,
        job_timeout_seconds: int = 600,
        poll_interval: int = 60,
        xcom_task_key: str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.entrypoint = entrypoint
        self.runtime_env = runtime_env
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.ray_resources = resources
        self.fetch_logs = fetch_logs
        self.wait_for_completion = wait_for_completion
        self.job_timeout_seconds = job_timeout_seconds
        self.poll_interval = poll_interval
        self.xcom_task_key = xcom_task_key
        self.dashboard_url: str | None = None
        self.job_id = ""
        self.terminal_state = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

    def on_kill(self) -> None:
        """Delete the Ray job if the task is killed."""
        if hasattr(self, "hook") and self.job_id:
            self.log.info(f"Deleting Ray job {self.job_id} due to task kill.")
            self.hook.delete_ray_job(self.job_id)

    @cached_property
    def hook(self) -> PodOperatorHookProtocol:
        """Lazily initialize and return the RayHook."""
        return RayHook(conn_id=self.conn_id, xcom_dashboard_url=self.dashboard_url)

    def execute(self, context: Context) -> str:
        """
        Execute the Ray job submission and monitoring.

        :param context: The context in which the task is being executed.
        :return: The job ID of the submitted Ray job.
        :raises AirflowException: If the job fails, is cancelled, or reaches an unexpected state.
        """
        if self.xcom_task_key:
            task, key = self.xcom_task_key.split(".")
            ti = context["ti"]
            self.dashboard_url = ti.xcom_pull(task_ids=task, key=key)
            self.log.info(f"Dashboard URL retrieved from XCom: {self.dashboard_url}")

        self.job_id = self.hook.submit_ray_job(
            entrypoint=self.entrypoint,
            runtime_env=self.runtime_env,
            entrypoint_num_cpus=self.num_cpus,
            entrypoint_num_gpus=self.num_gpus,
            entrypoint_memory=self.memory,
            entrypoint_resources=self.ray_resources,
        )
        self.log.info(f"Ray job submitted with id: {self.job_id}")

        if self.wait_for_completion:
            current_status = self.hook.get_ray_job_status(self.job_id)
            self.log.info(f"Current job status for {self.job_id} is: {current_status}")

            if current_status not in self.terminal_state:
                self.log.info("Deferring the polling to RayJobTrigger...")
                self.defer(
                    trigger=RayJobTrigger(
                        job_id=self.job_id,
                        conn_id=self.conn_id,
                        xcom_dashboard_url=self.dashboard_url,
                        poll_interval=self.poll_interval,
                        fetch_logs=self.fetch_logs,
                    ),
                    method_name="execute_complete",
                    timeout=timedelta(seconds=self.job_timeout_seconds),
                )
            elif current_status == JobStatus.SUCCEEDED:
                self.log.info("Job %s completed successfully", self.job_id)
            elif current_status == JobStatus.FAILED:
                raise AirflowException(f"Job failed:\n{self.job_id}")
            elif current_status == JobStatus.STOPPED:
                raise AirflowException(f"Job was cancelled:\n{self.job_id}")
            else:
                raise AirflowException(f"Encountered unexpected state `{current_status}` for job_id `{self.job_id}`")

        return self.job_id

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Handle the completion of a deferred Ray job execution.

        :param context: The context in which the task is being executed.
        :param event: The event containing the job execution result.
        :raises AirflowException: If the job execution fails or is cancelled.
        """
        if event["status"] in [JobStatus.STOPPED, JobStatus.FAILED]:
            self.log.info(f"Ray job {self.job_id} execution not completed...")
            raise AirflowException(event["message"])
        elif event["status"] == JobStatus.SUCCEEDED:
            self.log.info(f"Ray job {self.job_id} execution succeeded ...")
        else:
            raise AirflowException(f"Unexpected event status: {event['status']}")


from __future__ import annotations

import logging
import os
import shutil
import tempfile
import time
import warnings
import json
import yaml
import requests

from airflow.exceptions import AirflowException
from airflow.hooks.subprocess import SubprocessHook
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults
from airflow.utils.operator_helpers import context_to_airflow_vars
from datetime import timedelta
from functools import cached_property
from kubernetes import client, config, utils
from kubernetes.client.rest import ApiException
from kubernetes.client.api_client import ApiClient
from kubernetes.dynamic import DynamicClient
from ray_provider.triggers.kuberay import RayJobTrigger
from ray.job_submission import JobSubmissionClient, JobStatus
from typing import TYPE_CHECKING, Container, Sequence, cast

class RayClusterOperator(BaseOperator):

    def __init__(self,*,
                 cluster_name: str,
                 region: str,
                 kubeconfig: str,
                 ray_namespace: str,
                 ray_cluster_yaml : str,
                 ray_svc_yaml : str,
                 ray_gpu: bool = False,
                 env: dict = None,
                 **kwargs):
        
        super().__init__(**kwargs)
        self.cluster_name = cluster_name
        self.region = region
        self.kubeconfig = kubeconfig
        self.ray_namespace = ray_namespace
        self.ray_svc_yaml = ray_svc_yaml
        self.use_gpu = ray_gpu
        self.env = env
        self.output_encoding: str = "utf-8"
        self.cwd = tempfile.mkdtemp(prefix="tmp")

        if not self.cluster_name:
            raise AirflowException("EKS cluster name is required.")
        if not self.region:
            raise AirflowException("EKS region is required.")
        if not self.ray_namespace:
            raise AirflowException("EKS namespace is required.")
        
        # Check if ray cluster spec is provided
        if not ray_cluster_yaml:
            raise AirflowException("Ray Cluster spec is required")
        elif not os.path.isfile(ray_cluster_yaml):
            raise AirflowException(f"The specified Ray cluster YAML file does not exist: {ray_cluster_yaml}")
        elif not ray_cluster_yaml.endswith('.yaml') and not ray_cluster_yaml.endswith('.yml'):
            raise AirflowException("The specified Ray cluster YAML file must have a .yaml or .yml extension.")
        else:
            self.ray_cluster_yaml = ray_cluster_yaml

        if self.kubeconfig:
            os.environ['KUBECONFIG'] = self.kubeconfig
        
        self.k8Client = client.ApiClient()

    @cached_property
    def subprocess_hook(self):
        """Returns hook for running the bash command."""
        return SubprocessHook()

    def get_env(self, context):
        """Build the set of environment variables to be exposed for the bash command."""
        system_env = os.environ.copy()
        env = self.env
        if env is None:
            env = system_env
        else:
            system_env.update(env)
            env = system_env

        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.debug(
            "Exporting env vars: %s",
            " ".join(f"{k}={v!r}" for k, v in airflow_context_vars.items()),
        )
        env.update(airflow_context_vars)
        return env

    def execute_bash_command(self, bash_command:str, env: dict):
        
        bash_path = shutil.which("bash") or "bash"

        self.log.info("Running bash command: "+ bash_command)

        result = self.subprocess_hook.run_command(
            command=[bash_path, "-c", bash_command],
            env=env,
            output_encoding=self.output_encoding,
            cwd=self.cwd,
        )

        if result.exit_code != 0:
            raise AirflowException(
                f"Bash command failed. The command returned a non-zero exit code {result.exit_code}."
            )

        return result.output


    def add_kuberay_operator(self, env: dict):
        # Helm commands to add repo, update, and install KubeRay operator
        helm_commands = f"""
                    helm repo add kuberay https://ray-project.github.io/kuberay-helm/ && \
                    helm repo update && \
                    helm upgrade --install kuberay-operator kuberay/kuberay-operator \
                    --version 1.0.0 --create-namespace --namespace {self.ray_namespace}
                    """
        result = self.execute_bash_command(helm_commands, env)
        self.log.info(result)
        return result
    
    def create_resource(self, yaml_file_path):
        # Load kubeconfig
        config.load_kube_config(self.kubeconfig)

        # Load YAML file
        if yaml_file_path.startswith("http"):
            # If the path is a URL, fetch the content
            response = requests.get(yaml_file_path)
            response.raise_for_status()
            yaml_content = response.text
        else:
            # If the path is a local file, read the file
            with open(yaml_file_path, "r") as f:
                yaml_content = f.read()

        # Parse YAML
        resource_yaml = yaml.safe_load(yaml_content)

        # Extract necessary fields from the YAML
        kind = resource_yaml.get('kind')
        api_version = resource_yaml.get('apiVersion')
        metadata = resource_yaml.get('metadata', {})
        name = metadata.get('name')
        namespace = metadata.get('namespace', self.ray_namespace)  # default to 'default' if not specified

        if '/' in api_version:
            group, version = api_version.split('/')
        else:
            group = ""
            version = api_version

        body = resource_yaml  # dict | the JSON schema of the Resource to create

        try:
            api_instance = None
            existing_resource = None

            if kind == 'DaemonSet':
                # Create API client for DaemonSet
                api_instance = client.AppsV1Api()
                existing_resource = api_instance.read_namespaced_daemon_set(name, namespace)
            else:
                # Create API client for custom objects
                api_instance = client.CustomObjectsApi()
                plural = kind.lower() + 's'  # Simple heuristic for plural, can be improved
                existing_resource = api_instance.get_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    name=name
                )

            if existing_resource:
                print(f"{kind} '{name}' already exists in namespace '{namespace}'. Skipping creation.")
                return

        except client.exceptions.ApiException as e:
            if e.status != 404:
                print(f"Exception when checking if {kind} '{name}' exists: {e}")
                return
            # If the resource does not exist (404), proceed to create it
        except ValueError as e:
            print(e)
            return

        try:
            if kind == 'DaemonSet':
                # Create DaemonSet
                api_response = api_instance.create_namespaced_daemon_set(
                    namespace=namespace,
                    body=body
                )
                print(f"{kind} created.")
            else:
                # Create custom object
                api_response = api_instance.create_namespaced_custom_object(
                    group=group,
                    version=version,
                    namespace=namespace,
                    plural=plural,
                    body=body
                )
                print(f"{kind} created.")
        except client.exceptions.ApiException as e:
            print(f"Exception when creating {kind}: {e}\n")
        except ValueError as e:
            print(e)

    def create_k8_service(self, namespace: str = "default", yaml_file: str = "ray-head-service.yaml"):
        self.log.info("Creating service with yaml file: " + yaml_file)
        config.load_kube_config(self.kubeconfig)

        with open(yaml_file) as f:
            service_data = yaml.safe_load(f)

        v1 = client.CoreV1Api()
        service_name = service_data['metadata']['name']

        existing_service = self.check_service_exists(v1, service_name, namespace)
        if existing_service:
            self.log.info(f"Service '{service_name}' already exists in namespace '{namespace}'. Retrieving existing external DNS name...")
        else:
            existing_service = v1.create_namespaced_service(namespace=namespace, body=service_data)
            self.log.info(f"Service {existing_service.metadata.name} created. Waiting for an external DNS name...")

        external_dns = self.wait_for_external_dns(v1, existing_service.metadata.name, namespace)
        if not external_dns:
            self.log.error("Failed to find the external DNS name for the service within the expected time.")
            return None

        if not self.wait_for_endpoints(v1, existing_service.metadata.name, namespace):
            self.log.error("Pods failed to become ready within the expected time.")
            raise AirflowException("Pods failed to become ready within the expected time.")

        urls = self.construct_service_urls(existing_service, external_dns)
        for port_name, url in urls.items():
            self.log.info(f"Service URL for {port_name}: {url}")

        return urls

    def check_service_exists(self, v1, name, namespace):
        try:
            return v1.read_namespaced_service(name=name, namespace=namespace)
        except client.exceptions.ApiException as e:
            if e.status == 404:
                return None
            self.log.error(f"Exception when checking if service '{name}' exists: {e}")
            return None

    def wait_for_external_dns(self, v1, service_name, namespace, max_retries=30, retry_interval=40):
        for attempt in range(max_retries):
            self.log.info(f"Attempt {attempt + 1}: Checking for service's external DNS name...")
            service = v1.read_namespaced_service(name=service_name, namespace=namespace)
            if service.status.load_balancer.ingress and service.status.load_balancer.ingress[0].hostname:
                external_dns = service.status.load_balancer.ingress[0].hostname
                self.log.info(f"External DNS name found: {external_dns}")
                return external_dns
            self.log.info("External DNS name not yet available, waiting...")
            time.sleep(retry_interval)
        return None

    def wait_for_endpoints(self, v1, service_name, namespace, max_retries=30, retry_interval=40):
        for attempt in range(max_retries):
            endpoints = v1.read_namespaced_endpoints(name=service_name, namespace=namespace)
            if endpoints.subsets and all([subset.addresses for subset in endpoints.subsets]):
                self.log.info("All associated pods are ready.")
                return True
            self.log.info(f"Pods not ready, waiting... (Attempt {attempt + 1})")
            time.sleep(retry_interval)
        return False

    def construct_service_urls(self, service, external_dns):
        return {port.name: f"http://{external_dns}:{port.port}" for port in service.spec.ports}
    
    def execute(self, context: Context):

        env = self.get_env(context)

        self.add_kuberay_operator(env)

        self.create_resource(self.ray_cluster_yaml)

        if self.use_gpu:
            nvidia_yaml = "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml"
            self.create_resource(nvidia_yaml)

        if self.ray_svc_yaml:
            # Creating K8 services
            urls = self.create_k8_service(self.ray_namespace, self.ray_svc_yaml)

            if urls:
                for key,value in urls.items():
                    context['task_instance'].xcom_push(key=key, value=value)
            else:
                # Handle the case when urls is None or empty
                self.log.info("No URLs to push to XCom.")

        return urls

class SubmitRayJob(BaseOperator):

    template_fields = ('host','entrypoint','runtime_env','num_cpus','num_gpus','memory')

    def __init__(self,*,
                 host: str,
                 entrypoint: str,
                 runtime_env: dict,
                 num_cpus: int = 0,
                 num_gpus: int = 0,
                 memory: int | float = 0,
                 resources: dict = None, 
                 timeout: int = 600,
                 **kwargs):
        
        super().__init__(**kwargs)
        self.host = host
        self.entrypoint = entrypoint
        self.runtime_env = runtime_env
        self.num_cpus = num_cpus
        self.num_gpus = num_gpus
        self.memory = memory
        self.resources = resources
        self.timeout = timeout
        self.client = None
        self.job_id = None
        self.status_to_wait_for = {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}
    
    def __del__(self):
        if self.client:   
            return self.client.delete_job(self.job_id)
        else:
            return
        
    def on_kill(self):
        if self.client:   
            return self.client.delete_job(self.job_id)
        else:
            return
    def execute(self,context : Context):

        if not self.client:
            self.log.info(f"URL is: {self.host}")
            self.client = JobSubmissionClient(f"{self.host}")

        self.job_id = self.client.submit_job(
            entrypoint= self.entrypoint,
            runtime_env=self.runtime_env, #https://docs.ray.io/en/latest/ray-core/handling-dependencies.html#runtime-environments
            entrypoint_num_cpus = self.num_cpus,
            entrypoint_num_gpus = self.num_gpus,
            entrypoint_memory = self.memory,
            entrypoint_resources = self.resources)  
        
        self.log.info(f"Ray job submitted with id:{self.job_id}")

        current_status = self.get_current_status()
        if current_status in (JobStatus.RUNNING, JobStatus.PENDING):
            self.log.info("Deferring the polling to RayJobTrigger...")
            self.defer(
                timeout= timedelta(hours=1),
                trigger= RayJobTrigger(
                    host = self.host,
                    job_id = self.job_id,
                    end_time= time.time() + self.timeout,
                    poll_interval=2
                ),
                method_name="execute_complete",)
        elif current_status == JobStatus.SUCCEEDED:
            self.log.info("Job %s completed successfully", self.job_id)
            return
        elif current_status == JobStatus.FAILED:
            raise AirflowException(f"Job failed:\n{self.job_id}")
        elif current_status == JobStatus.STOPPED:
            raise AirflowException(f"Job was cancelled:\n{self.job_id}")
        else:
            raise Exception(f"Encountered unexpected state `{current_status}` for job_id `{self.job_id}")
        
        return self.job_id
    
    def get_current_status(self):
        
        job_status = self.client.get_job_status(self.job_id)
        self.log.info(f"Current job status for {self.job_id} is: {job_status}")
        return job_status
    
    def execute_complete(self, context: Context, event: Any = None) -> None:

        # Get logs
        #logs = self.client.get_job_logs(self.job_id)
        #for log in logs.split("\n"):
        #        self.log.info(log)

        if event["status"] == "error" or event["status"] == "cancelled":
            self.log.info(f"Ray job {self.job_id} execution not completed...")
            raise AirflowException(event["message"])
        elif event["status"] == "success":
            self.log.info(f"Ray job {self.job_id} execution succeeded ...")
            return None
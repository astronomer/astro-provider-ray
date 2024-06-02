import pytest
from unittest.mock import MagicMock, patch
from airflow.exceptions import AirflowException
from datetime import timedelta
from ray_provider.operators.kuberay import SubmitRayJob, RayClusterOperator
from ray_provider.triggers.kuberay import RayJobTrigger
from ray.job_submission import JobSubmissionClient, JobStatus

from airflow.exceptions import AirflowException
import tempfile
import os

class TestRayClusterOperator:

    @pytest.fixture
    def valid_args(self):
        return {
            "cluster_name": "test-cluster",
            "region": "us-west-2",
            "kubeconfig": "test-kubeconfig",
            "ray_namespace": "test-namespace",
            "ray_cluster_yaml": "test-ray-cluster.yaml",
            "ray_svc_yaml": "test-ray-svc.yaml",
            "ray_gpu": False,
            "env": {"TEST_ENV": "test"},
            "task_id": "Testcases"
        }

    @patch('os.path.isfile', return_value=True)
    def test_init_success(self, mock_isfile, valid_args):
        operator = RayClusterOperator(**valid_args)
        assert operator.cluster_name == valid_args["cluster_name"]
        assert operator.region == valid_args["region"]
        assert operator.kubeconfig == valid_args["kubeconfig"]
        assert operator.ray_namespace == valid_args["ray_namespace"]
        assert operator.ray_cluster_yaml == valid_args["ray_cluster_yaml"]
        assert operator.ray_svc_yaml == valid_args["ray_svc_yaml"]
        assert operator.use_gpu == valid_args["ray_gpu"]
        assert operator.env == valid_args["env"]

    def test_init_missing_cluster_name(self, valid_args):
        valid_args["cluster_name"] = ""
        with pytest.raises(AirflowException, match="Cluster name is required."):
            RayClusterOperator(**valid_args)

    def test_init_missing_region(self, valid_args):
        valid_args["region"] = ""
        with pytest.raises(AirflowException, match="Region is required."):
            RayClusterOperator(**valid_args)

    def test_init_missing_ray_namespace(self, valid_args):
        valid_args["ray_namespace"] = ""
        with pytest.raises(AirflowException, match="Namespace is required."):
            RayClusterOperator(**valid_args)

    def test_init_missing_ray_cluster_yaml(self, valid_args):
        valid_args["ray_cluster_yaml"] = ""
        with pytest.raises(AirflowException, match="Ray Cluster spec is required"):
            RayClusterOperator(**valid_args)

    @patch('os.path.isfile', return_value=False)
    def test_init_invalid_ray_cluster_yaml_file(self, mock_isfile, valid_args):
        valid_args["ray_cluster_yaml"] = "invalid-file.yaml"
        with pytest.raises(AirflowException, match="The specified Ray cluster YAML file does not exist: invalid-file.yaml"):
            RayClusterOperator(**valid_args)

    @patch('os.path.isfile', return_value=True)
    def test_init_invalid_ray_cluster_yaml_extension(self, mock_isfile, valid_args):
        valid_args["ray_cluster_yaml"] = "invalid-file.txt"
        with pytest.raises(AirflowException, match="The specified Ray cluster YAML file must have a .yaml or .yml extension."):
            RayClusterOperator(**valid_args)

    @patch('os.path.isfile', return_value=True)
    @patch('os.environ', {})
    def test_init_kubeconfig_env_set(self, mock_isfile, valid_args):
        RayClusterOperator(**valid_args)
        assert os.environ['KUBECONFIG'] == valid_args["kubeconfig"]
    
    @patch('tempfile.mkdtemp', return_value='/tmp/fake-dir')
    @patch('os.path.isfile', return_value=True)
    def test_init_temp_dir_creation(self, mock_isfile, mock_mkdtemp, valid_args):
        operator = RayClusterOperator(**valid_args)
        assert operator.cwd == '/tmp/fake-dir'




# Sample parameters for initialization
host = "http://localhost:8265"
entrypoint = "python script.py"
runtime_env = {"pip": ["requests"]}
num_cpus = 2
num_gpus = 1
memory = 1024
resources = {"CPU": 2}
timeout = 600
context = MagicMock()

@pytest.fixture
def operator():
    return SubmitRayJob(
        host=host,
        entrypoint=entrypoint,
        runtime_env=runtime_env,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        resources=resources,
        timeout=timeout,
        task_id = "Testcases"
    )

class TestSubmitRayJob:

    def test_init(self, operator):
        assert operator.host == host
        assert operator.entrypoint == entrypoint
        assert operator.runtime_env == runtime_env
        assert operator.num_cpus == num_cpus
        assert operator.num_gpus == num_gpus
        assert operator.memory == memory
        assert operator.resources == resources
        assert operator.timeout == timeout
        assert operator.client is None
        assert operator.job_id is None
        assert operator.status_to_wait_for == {JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED}

    @patch('ray.job_submission.JobSubmissionClient')
    def test_execute(self, mock_client_class, operator):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.submit_job.return_value = "job_12345"
        mock_client.get_job_status.return_value = JobStatus.RUNNING

        job_id = operator.execute(context)
        
        mock_client_class.assert_called_once_with(host)
        mock_client.submit_job.assert_called_once_with(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
            entrypoint_num_cpus=num_cpus,
            entrypoint_num_gpus=num_gpus,
            entrypoint_memory=memory,
            entrypoint_resources=resources
        )
        assert operator.job_id == "job_12345"
        assert job_id == "job_12345"

    @patch('ray.job_submission.JobSubmissionClient')
    def test_on_kill(self, mock_client_class, operator):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        operator.client = mock_client
        operator.job_id = "job_12345"

        operator.on_kill()
        
        mock_client.delete_job.assert_called_once_with("job_12345")

    def test_execute_complete_success(self, operator):
        event = {"status": "success", "message": "Job completed successfully"}
        operator.job_id = "job_12345"
        
        assert operator.execute_complete(context, event) is None

    def test_execute_complete_failure(self, operator):
        event = {"status": "error", "message": "Job failed"}
        operator.job_id = "job_12345"

        with pytest.raises(AirflowException, match="Job failed"):
            operator.execute_complete(context, event)
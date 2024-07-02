from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException, TaskDeferred
from ray.job_submission import JobStatus

from ray_provider.operators.ray import SubmitRayJob

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
        task_id="Testcases",
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

    @patch("ray_provider.operators.kuberay.JobSubmissionClient")
    def test_execute(self, mock_client_class, operator):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.submit_job.return_value = "job_12345"
        mock_client.get_job_status.return_value = JobStatus.RUNNING

        try:
            operator.execute(context)
        except TaskDeferred:
            pass

        mock_client_class.assert_called_once_with(host)
        mock_client.submit_job.assert_called_once_with(
            entrypoint=entrypoint,
            runtime_env=runtime_env,
            entrypoint_num_cpus=num_cpus,
            entrypoint_num_gpus=num_gpus,
            entrypoint_memory=memory,
            entrypoint_resources=resources,
        )
        assert operator.job_id == "job_12345"

    @patch("ray_provider.operators.kuberay.JobSubmissionClient")
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

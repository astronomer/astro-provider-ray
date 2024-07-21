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
job_timeout_seconds = 600
context = MagicMock()


@pytest.fixture
def operator():
    return SubmitRayJob(
        conn_id="test_conn",
        entrypoint=entrypoint,
        runtime_env=runtime_env,
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        resources=resources,
        job_timeout_seconds=job_timeout_seconds,
        task_id="Testcases",
    )


class TestSubmitRayJob:

    def test_init(self, operator):
        assert operator.conn_id == "test_conn"
        assert operator.entrypoint == entrypoint
        assert operator.runtime_env == runtime_env
        assert operator.num_cpus == num_cpus
        assert operator.num_gpus == num_gpus
        assert operator.memory == memory
        # assert operator.resources == resources
        assert operator.job_timeout_seconds == job_timeout_seconds

    @patch("ray_provider.operators.ray.SubmitRayJob.hook")
    def test_execute(self, mock_hook, operator):
        with pytest.raises(TaskDeferred):
            operator.execute(context)
        mock_hook.submit_ray_job.assert_called_once_with(
            entrypoint="python script.py",
            runtime_env={"pip": ["requests"]},
            entrypoint_num_cpus=2,
            entrypoint_num_gpus=1,
            entrypoint_memory=1024,
            entrypoint_resources={"CPU": 2},
        )

    @patch("ray_provider.operators.ray.SubmitRayJob.hook")
    def test_on_kill(self, mock_hook, operator):
        operator.job_id = "job_12345"

        operator.on_kill()
        mock_hook.delete_ray_job.assert_called_once_with("job_12345")

    def test_execute_complete_success(self, operator):
        event = {"status": JobStatus.SUCCEEDED, "message": "Job completed successfully"}
        operator.job_id = "job_12345"

        assert operator.execute_complete(context, event) is None

    def test_execute_complete_failure(self, operator):
        event = {"status": JobStatus.FAILED, "message": "Job failed"}
        operator.job_id = "job_12345"

        with pytest.raises(AirflowException, match="Job failed"):
            operator.execute_complete(context, event)

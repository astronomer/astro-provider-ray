import pytest
import asyncio
from unittest import mock
from airflow.triggers.base import TriggerEvent
from ray.dashboard.modules.job.sdk import JobSubmissionClient, JobStatus
from ray_provider.triggers.kuberay import RayJobTrigger
import time

class TestRayJobTrigger:

    @pytest.mark.asyncio
    async def test_run_no_job_id(self):
        trigger = RayJobTrigger(
            job_id="",
            host="localhost",
            end_time=time.time() + 60,
            poll_interval=1
        )

        generator = trigger.run()
        event = await generator.asend(None)
        assert event == TriggerEvent({
            "status": "error",
            "message": "No job_id provided to async trigger",
            "job_id": ""
        })

    @pytest.mark.asyncio
    async def test_run_job_succeeded(self):
        trigger = RayJobTrigger(
            job_id="test_job_id",
            host="localhost",
            end_time=time.time() + 60,
            poll_interval=1
        )

        client_mock = mock.MagicMock(spec=JobSubmissionClient)
        client_mock.get_job_status.return_value = JobStatus.SUCCEEDED

        async def async_generator():
            yield "log line 1"
            yield "log line 2"

        client_mock.tail_job_logs.return_value = async_generator()

        with mock.patch("ray_provider.triggers.kuberay.JobSubmissionClient", return_value=client_mock):
            generator = trigger.run()
            async for event in generator:
                assert event == TriggerEvent({
                    "status": "success",
                    "message": "Job run test_job_id has completed successfully.",
                    "job_id": "test_job_id"
                })
                break  # Stop after the first event for testing purposes
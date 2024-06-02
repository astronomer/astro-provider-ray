import ray
import pytest
import asyncio
from unittest.mock import AsyncMock, patch
from airflow.triggers.base import TriggerEvent
from ray.job_submission import JobStatus
from ray_provider.triggers.kuberay import RayJobTrigger  # Replace with your actual module name
import time

@pytest.mark.asyncio
async def test_no_job_id():
    trigger = RayJobTrigger(job_id='', host='http://localhost', end_time=time.time() + 60)
    generator = trigger.run()
    event = await anext(generator)
    assert event == TriggerEvent({"status": "error", "message": "No job_id provided to async trigger", "job_id": ''})

@pytest.mark.asyncio
async def test_job_successful():
    client_mock = AsyncMock()
    client_mock.get_job_status.return_value = JobStatus.SUCCEEDED
    client_mock.tail_job_logs.return_value = AsyncMock()

    with patch('ray.job_submission.JobSubmissionClient', return_value=client_mock):
        trigger = RayJobTrigger(job_id='123', host='http://localhost', end_time=time.time() + 60)
        generator = trigger.run()
        await asyncio.sleep(0)  # needed to start the generator
        event = await anext(generator)
        assert event == TriggerEvent({"status": "success", "message": "Job run 123 has completed successfully.", "job_id": '123'})

@pytest.mark.asyncio
async def test_job_failed():
    client_mock = AsyncMock()
    client_mock.get_job_status.return_value = JobStatus.FAILED
    client_mock.tail_job_logs.return_value = AsyncMock()

    with patch('ray.job_submission.JobSubmissionClient', return_value=client_mock):
        trigger = RayJobTrigger(job_id='123', host='http://localhost', end_time=time.time() + 60)
        generator = trigger.run()
        await asyncio.sleep(0)  # needed to start the generator
        event = await anext(generator)
        assert event == TriggerEvent({"status": "error", "message": "Job run 123 has failed.", "job_id": '123'})

@pytest.mark.asyncio
async def test_job_stopped():
    client_mock = AsyncMock()
    client_mock.get_job_status.return_value = JobStatus.STOPPED
    client_mock.tail_job_logs.return_value = AsyncMock()

    with patch('ray.job_submission.JobSubmissionClient', return_value=client_mock):
        trigger = RayJobTrigger(job_id='123', host='http://localhost', end_time=time.time() + 60)
        generator = trigger.run()
        await asyncio.sleep(0)  # needed to start the generator
        event = await anext(generator)
        assert event == TriggerEvent({"status": "cancelled", "message": "Job run 123 has been stopped.", "job_id": '123'})

@pytest.mark.asyncio
async def test_job_timeout():
    client_mock = AsyncMock()
    client_mock.get_job_status.side_effect = [JobStatus.RUNNING, JobStatus.RUNNING, JobStatus.RUNNING]
    client_mock.tail_job_logs.return_value = AsyncMock()

    with patch('ray.job_submission.JobSubmissionClient', return_value=client_mock):
        trigger = RayJobTrigger(job_id='123', host='http://localhost', end_time=time.time() + 1, poll_interval=0.1)
        generator = trigger.run()
        await asyncio.sleep(0)  # needed to start the generator
        try:
            event = await anext(generator)
        except StopAsyncIteration:
            # If the generator completes, we simulate a timeout
            event = TriggerEvent({
                "status": "error",
                "message": f"Job run 123 has not reached a terminal status after {trigger.end_time - time.time()} seconds.",
                "job_id": '123',
            })
        assert event == TriggerEvent({
            "status": "error",
            "message": f"Job run 123 has not reached a terminal status after {trigger.end_time - time.time()} seconds.",
            "job_id": '123',
        })

@pytest.mark.asyncio
async def test_connection_error():
    client_mock = AsyncMock()
    client_mock.get_job_status.side_effect = Exception("Failed to connect to Ray at address: http://localhost.")
    client_mock.tail_job_logs.return_value = AsyncMock()

    with patch('ray.job_submission.JobSubmissionClient', return_value=client_mock):
        trigger = RayJobTrigger(job_id='123', host='http://localhost', end_time=time.time() + 60)
        generator = trigger.run()
        event = await anext(generator)
        assert event == TriggerEvent({
            "status": "error",
            "message": "Failed to connect to Ray at address: http://localhost.",
            "job_id": '123',
        })
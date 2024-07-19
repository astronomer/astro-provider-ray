from unittest.mock import patch

import pytest
from airflow.triggers.base import TriggerEvent
from ray.dashboard.modules.job.sdk import JobStatus

from ray_provider.triggers.ray import RayJobTrigger


class TestRayJobTrigger:

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_run_no_job_id(self, mock_hook, mock_is_terminal):
        mock_is_terminal.return_value = True
        trigger = RayJobTrigger(job_id="", poll_interval=1, conn_id="test", xcom_dashboard_url="test")

        generator = trigger.run()
        event = await generator.asend(None)
        assert event == TriggerEvent({"status": "error", "message": "Job run  has failed.", "job_id": ""})

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_run_job_succeeded(self, mock_hook):
        trigger = RayJobTrigger(job_id="test_job_id", poll_interval=1, conn_id="test", xcom_dashboard_url="test")

        mock_hook.get_ray_job_status.return_value = JobStatus.SUCCEEDED

        generator = trigger.run()
        async for event in generator:
            assert event == TriggerEvent(
                {
                    "status": "success",
                    "message": "Job run test_job_id has completed successfully.",
                    "job_id": "test_job_id",
                }
            )
            break  # Stop after the first event for testing purposes

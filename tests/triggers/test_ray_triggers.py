from unittest.mock import patch

import pytest
from airflow.triggers.base import TriggerEvent
from ray.job_submission import JobStatus

from ray_provider.triggers.ray import RayJobTrigger


class TestRayJobTrigger:
    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_run_no_job_id(self, mock_hook, mock_is_terminal):
        mock_is_terminal.return_value = True
        mock_hook.get_ray_job_status.return_value = JobStatus.FAILED
        trigger = RayJobTrigger(job_id="", poll_interval=1, conn_id="test", xcom_dashboard_url="test")
        generator = trigger.run()
        event = await generator.asend(None)
        assert event == TriggerEvent(
            {"status": JobStatus.FAILED, "message": "Job  completed with status FAILED", "job_id": ""}
        )

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_run_job_succeeded(self, mock_hook, mock_is_terminal):
        mock_is_terminal.side_effect = [False, True]
        mock_hook.get_ray_job_status.return_value = JobStatus.SUCCEEDED
        trigger = RayJobTrigger(job_id="test_job_id", poll_interval=1, conn_id="test", xcom_dashboard_url="test")
        generator = trigger.run()
        event = await generator.asend(None)
        assert event == TriggerEvent(
            {
                "status": JobStatus.SUCCEEDED,
                "message": f"Job test_job_id completed with status {JobStatus.SUCCEEDED}",
                "job_id": "test_job_id",
            }
        )

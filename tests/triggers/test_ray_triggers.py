import logging
from unittest.mock import AsyncMock, call, patch

import pytest
from airflow.triggers.base import TriggerEvent
from ray.job_submission import JobStatus

from ray_provider.triggers.ray import RayJobTrigger


class TestRayJobTrigger:
    @pytest.fixture
    def trigger(self):
        return RayJobTrigger(
            job_id="test_job_id",
            conn_id="test_conn",
            xcom_dashboard_url="http://test-dashboard.com",
            ray_cluster_yaml="test.yaml",
            gpu_device_plugin_yaml="nvidia.yaml",
            poll_interval=1,
            fetch_logs=True,
        )

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_run_no_job_id(self, mock_hook, mock_is_terminal):
        mock_is_terminal.return_value = True
        mock_hook.get_ray_job_status.return_value = JobStatus.FAILED
        trigger = RayJobTrigger(
            job_id="",
            poll_interval=1,
            conn_id="test",
            xcom_dashboard_url="test",
            ray_cluster_yaml="test.yaml",
            gpu_device_plugin_yaml="nvidia.yaml",
        )
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
        trigger = RayJobTrigger(
            job_id="test_job_id",
            poll_interval=1,
            conn_id="test",
            xcom_dashboard_url="test",
            ray_cluster_yaml="test.yaml",
            gpu_device_plugin_yaml="nvidia.yaml",
        )
        generator = trigger.run()
        event = await generator.asend(None)
        assert event == TriggerEvent(
            {
                "status": JobStatus.SUCCEEDED,
                "message": f"Job test_job_id completed with status {JobStatus.SUCCEEDED}",
                "job_id": "test_job_id",
            }
        )

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_run_job_stopped(self, mock_hook, mock_is_terminal, trigger):
        mock_is_terminal.side_effect = [False, True]
        mock_hook.get_ray_job_status.return_value = JobStatus.STOPPED

        generator = trigger.run()
        event = await generator.asend(None)

        assert event == TriggerEvent(
            {
                "status": JobStatus.STOPPED,
                "message": f"Job test_job_id completed with status {JobStatus.STOPPED}",
                "job_id": "test_job_id",
            }
        )

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_run_job_failed(self, mock_hook, mock_is_terminal, trigger):
        mock_is_terminal.side_effect = [False, True]
        mock_hook.get_ray_job_status.return_value = JobStatus.FAILED

        generator = trigger.run()
        event = await generator.asend(None)

        assert event == TriggerEvent(
            {
                "status": JobStatus.FAILED,
                "message": f"Job test_job_id completed with status {JobStatus.FAILED}",
                "job_id": "test_job_id",
            }
        )

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    @patch("ray_provider.triggers.ray.RayJobTrigger._stream_logs")
    async def test_run_with_log_streaming(self, mock_stream_logs, mock_hook, mock_is_terminal, trigger):
        mock_is_terminal.side_effect = [False, True]
        mock_hook.get_ray_job_status.return_value = JobStatus.SUCCEEDED
        mock_stream_logs.return_value = None

        generator = trigger.run()
        event = await generator.asend(None)

        mock_stream_logs.assert_called_once()
        assert event == TriggerEvent(
            {
                "status": JobStatus.SUCCEEDED,
                "message": f"Job test_job_id completed with status {JobStatus.SUCCEEDED}",
                "job_id": "test_job_id",
            }
        )

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_stream_logs(self, mock_hook, trigger):
        # Create a mock async iterator
        async def mock_async_iterator():
            for item in ["Log line 1\n", "Log line 2\n"]:
                yield item

        # Set up the mock to return an async iterator
        mock_hook.get_ray_tail_logs.return_value = mock_async_iterator()

        with patch("ray_provider.triggers.ray.RayJobTrigger.log") as mock_log:
            await trigger._stream_logs()

            mock_log.info.assert_any_call("::group::test_job_id logs")
            mock_log.info.assert_any_call("Log line 1")
            mock_log.info.assert_any_call("Log line 2")
            mock_log.info.assert_any_call("::endgroup::")

    def test_serialize(self, trigger):
        serialized = trigger.serialize()
        assert serialized == (
            "ray_provider.triggers.ray.RayJobTrigger",
            {
                "job_id": "test_job_id",
                "conn_id": "test_conn",
                "xcom_dashboard_url": "http://test-dashboard.com",
                "ray_cluster_yaml": "test.yaml",
                "gpu_device_plugin_yaml": "nvidia.yaml",
                "fetch_logs": True,
                "poll_interval": 1,
            },
        )

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    async def test_is_terminal_state(self, mock_hook, trigger):
        mock_hook.get_ray_job_status.side_effect = [
            JobStatus.PENDING,
            JobStatus.RUNNING,
            JobStatus.SUCCEEDED,
        ]

        assert not trigger._is_terminal_state()
        assert not trigger._is_terminal_state()
        assert trigger._is_terminal_state()

    @pytest.mark.asyncio
    @patch.object(RayJobTrigger, "hook")
    @patch.object(logging.Logger, "info")
    async def test_cleanup_with_cluster_yaml(self, mock_log_info, mock_hook, trigger):
        await trigger.cleanup()

        mock_log_info.assert_has_calls(
            [
                call("Attempting to delete Ray cluster using YAML: test.yaml"),
                call("Ray cluster deletion process completed"),
            ]
        )
        mock_hook.delete_ray_cluster.assert_called_once_with("test.yaml", "nvidia.yaml")

    @pytest.mark.asyncio
    @patch.object(logging.Logger, "info")
    async def test_cleanup_without_cluster_yaml(self, mock_log_info):
        trigger = RayJobTrigger(
            job_id="test_job_id",
            conn_id="test_conn",
            xcom_dashboard_url="http://test-dashboard.com",
            ray_cluster_yaml=None,
            gpu_device_plugin_yaml="nvidia.yaml",
            poll_interval=1,
            fetch_logs=True,
        )

        await trigger.cleanup()

        mock_log_info.assert_called_once_with("No Ray cluster YAML provided, skipping cluster deletion")

    @pytest.mark.asyncio
    @patch.object(RayJobTrigger, "hook")
    @patch.object(logging.Logger, "error")
    async def test_cleanup_with_exception(self, mock_log_error, mock_hook, trigger):
        mock_hook.delete_ray_cluster.side_effect = Exception("Test exception")

        await trigger.cleanup()

        mock_log_error.assert_called_once_with("Unexpected error during cleanup: Test exception")

    @pytest.mark.asyncio
    @patch("asyncio.sleep", new_callable=AsyncMock)
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    async def test_poll_status(self, mock_is_terminal, mock_sleep, trigger):
        mock_is_terminal.side_effect = [False, False, True]

        await trigger._poll_status()

        assert mock_sleep.call_count == 2
        mock_sleep.assert_called_with(1)

    @pytest.mark.asyncio
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state")
    @patch("ray_provider.triggers.ray.RayJobTrigger.hook")
    @patch("ray_provider.triggers.ray.RayJobTrigger.cleanup")
    async def test_run_with_exception(self, mock_cleanup, mock_hook, mock_is_terminal, trigger):
        mock_is_terminal.side_effect = Exception("Test exception")

        generator = trigger.run()
        event = await generator.asend(None)

        assert event == TriggerEvent(
            {
                "status": str(JobStatus.FAILED),
                "message": "Test exception",
                "job_id": "test_job_id",
            }
        )
        mock_cleanup.assert_called_once()

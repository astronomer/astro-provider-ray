import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from airflow.exceptions import AirflowNotFoundException
from ray.job_submission import JobStatus

from ray_provider.triggers.ray import RayJobTrigger


class TestRayJobTrigger(unittest.TestCase):
    def setUp(self):
        self.trigger = RayJobTrigger(job_id="123", conn_id="ray_default", xcom_dashboard_url="http://example.com")

    @patch("ray_provider.hooks.ray.RayHook.get_ray_job_status")
    def test_is_terminal_state(self, mock_get_status):
        mock_get_status.return_value = JobStatus.SUCCEEDED
        self.assertTrue(self.trigger._is_terminal_state())

    @patch("ray_provider.hooks.ray.RayHook.get_ray_job_status")
    def test_is_not_terminal_state(self, mock_get_status):
        mock_get_status.return_value = JobStatus.RUNNING
        self.assertFalse(self.trigger._is_terminal_state())

    @patch("asyncio.sleep", return_value=None)
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state", side_effect=[False, False, True])
    @patch("ray_provider.hooks.ray.RayHook.get_ray_job_status", return_value=JobStatus.SUCCEEDED)
    async def test_run_successful_completion(self, mock_get_status, mock_is_terminal, mock_sleep):
        generator = self.trigger.run()
        event = await generator.asend(None)
        self.assertEqual(event.payload["status"], JobStatus.SUCCEEDED)
        self.assertEqual(event.payload["job_id"], "123")

    @patch("asyncio.sleep", return_value=None)
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state", return_value=False)
    @patch("time.time", side_effect=[0, 100, 200, 300, 10000])  # Simulating time passing and timeout
    async def test_run_timeout(self, mock_time, mock_is_terminal, mock_sleep):
        generator = self.trigger.run()
        event = await generator.asend(None)
        self.assertEqual(event.payload["status"], str(JobStatus.FAILED))
        self.assertTrue("Timeout", event.payload["message"])

    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state", side_effect=Exception("Error occurred"))
    async def test_run_exception(self, mock_is_terminal):
        generator = self.trigger.run()
        event = await generator.asend(None)
        self.assertEqual(event.payload["status"], str(JobStatus.FAILED))
        self.assertTrue("Error occurred", event.payload["message"])

    @patch("ray_provider.hooks.ray.RayHook.get_ray_tail_logs")
    @patch("ray_provider.triggers.ray.RayJobTrigger._is_terminal_state", side_effect=[False, True])
    @patch("ray_provider.hooks.ray.RayHook.get_ray_job_status", return_value=JobStatus.SUCCEEDED)
    @patch("asyncio.sleep", return_value=None)
    async def test_run_with_logs(self, mock_sleep, mock_get_status, mock_is_terminal, mock_get_job_logs):
        mock_get_job_logs.return_value = AsyncMock(return_value=["log line 1", "log line 2"])
        self.trigger.fetch_logs = True
        generator = self.trigger.run()
        event = await generator.asend(None)
        self.assertEqual(event.payload["status"], JobStatus.SUCCEEDED)
        mock_get_job_logs.assert_called_once()

    async def test_run_no_job_id_provided(self):
        trigger = RayJobTrigger(job_id="", conn_id="ray_default", xcom_dashboard_url="http://example.com")
        generator = trigger.run()
        event = await generator.asend(None)
        self.assertEqual(event.payload["status"], str(JobStatus.FAILED))
        self.assertTrue("No job_id provided", event.payload["message"])

    @patch(
        "ray_provider.hooks.ray.RayHook.__init__",
        side_effect=AirflowNotFoundException("The conn_id `ray_default` isn't defined"),
    )
    def test_hook_method(self, mock_hook_init):
        with self.assertRaises(AirflowNotFoundException) as context:
            # Accessing the hook property should now raise the exception
            _ = self.trigger.hook
        self.assertTrue("The conn_id `ray_default` isn't defined", str(context.exception))

    def test_serialize(self):
        result = self.trigger.serialize()
        expected_output = (
            "ray_provider.triggers.ray.RayJobTrigger",
            {
                "job_id": "123",
                "conn_id": "ray_default",
                "xcom_dashboard_url": "http://example.com",
                "fetch_logs": True,
                "poll_interval": 30,
            },
        )
        self.assertEqual(result, expected_output)

    @patch("ray_provider.hooks.ray.RayHook.get_ray_job_status")
    @patch("ray_provider.hooks.ray.RayHook.get_ray_tail_logs")
    @patch("asyncio.sleep", return_value=None)
    async def test_ray_run_trigger(self, mocked_sleep, mocked_get_job_logs, mocked_get_job_status):
        mocked_get_job_status.return_value = JobStatus.SUCCEEDED
        mocked_get_job_logs.return_value = AsyncMock(return_value=["log line 1", "log line 2"])

        trigger = RayJobTrigger(
            conn_id="test_conn",
            job_id="1234",
            poll_interval=1,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        self.assertFalse(task.done())

        await asyncio.sleep(2)
        result = await task

        self.assertEqual(result.payload["status"], JobStatus.SUCCEEDED)
        self.assertEqual(result.payload["message"], "Job 1234 completed with status JobStatus.SUCCEEDED")
        self.assertEqual(result.payload["job_id"], "1234")


if __name__ == "__main__":
    unittest.main()

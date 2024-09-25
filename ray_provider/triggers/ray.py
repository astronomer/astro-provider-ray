from __future__ import annotations

import asyncio
from functools import cached_property
from typing import Any, AsyncIterator

from airflow.triggers.base import BaseTrigger, TriggerEvent
from ray.job_submission import JobStatus

from ray_provider.hooks.ray import RayHook


class RayJobTrigger(BaseTrigger):
    """
    Triggers and monitors the status of a Ray job.

    This trigger periodically checks the status of a submitted job on a Ray cluster and
    yields events based on the job's status. It handles timeouts and errors during
    the polling process.

    :param job_id: The unique identifier of the Ray job.
    :param conn_id: The connection ID for the Ray cluster.
    :param xcom_dashboard_url: Optional URL for the Ray dashboard.
    :param poll_interval: The interval in seconds at which to poll the job status. Defaults to 30 seconds.
    :param fetch_logs: Whether to fetch and stream logs. Defaults to True.
    """

    def __init__(
        self,
        job_id: str,
        conn_id: str,
        xcom_dashboard_url: str | None,
        ray_cluster_yaml: str | None,
        gpu_device_plugin_yaml: str,
        poll_interval: int = 30,
        fetch_logs: bool = True,
    ):
        super().__init__()  # type: ignore[no-untyped-call]
        self.job_id = job_id
        self.conn_id = conn_id
        self.dashboard_url = xcom_dashboard_url
        self.ray_cluster_yaml = ray_cluster_yaml
        self.gpu_device_plugin_yaml = gpu_device_plugin_yaml
        self.fetch_logs = fetch_logs
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """
        Serializes the trigger's configuration.

        :return: A tuple containing the fully qualified class name and a dictionary of its parameters.
        """
        return (
            "ray_provider.triggers.ray.RayJobTrigger",
            {
                "job_id": self.job_id,
                "conn_id": self.conn_id,
                "xcom_dashboard_url": self.dashboard_url,
                "ray_cluster_yaml": self.ray_cluster_yaml,
                "gpu_device_plugin_yaml": self.gpu_device_plugin_yaml,
                "fetch_logs": self.fetch_logs,
                "poll_interval": self.poll_interval,
            },
        )

    @cached_property
    def hook(self) -> RayHook:
        """
        Lazily initializes and returns a RayHook instance.

        :return: An instance of RayHook configured with the connection ID and dashboard URL.
        """
        return RayHook(conn_id=self.conn_id)

    async def cleanup(self) -> None:
        """
        Cleanup method to ensure resources are properly deleted. This will be called when the trigger encounters an exception.

        Example scenario: A job is submitted using the @ray.task decorator with a Ray specification. After the cluster is started
        and the job is submitted, the trigger begins tracking its progress. However, if the job is stopped through the UI at this stage, the cluster
        resources are not deleted.

        """
        try:
            if self.ray_cluster_yaml:
                self.log.info(f"Attempting to delete Ray cluster using YAML: {self.ray_cluster_yaml}")
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None, self.hook.delete_ray_cluster, self.ray_cluster_yaml, self.gpu_device_plugin_yaml
                )
                self.log.info("Ray cluster deletion process completed")
            else:
                self.log.info("No Ray cluster YAML provided, skipping cluster deletion")
        except Exception as e:
            self.log.error(f"Unexpected error during cleanup: {str(e)}")

    async def _poll_status(self) -> None:
        while not self._is_terminal_state():
            await asyncio.sleep(self.poll_interval)

    async def _stream_logs(self) -> None:
        """
        Streams logs from the Ray job in real-time.
        """
        self.log.info(f"::group::{self.job_id} logs")
        async for log_lines in self.hook.get_ray_tail_logs(self.dashboard_url, self.job_id):
            for line in log_lines.split("\n"):
                if line.strip():  # Avoid logging empty lines
                    self.log.info(line.strip())
        self.log.info("::endgroup::")

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Asynchronously polls the job status and yields events based on the job's state.

        This method gets job status at each poll interval and streams logs if available.
        It yields a TriggerEvent upon job completion, cancellation, or failure.

        :yield: TriggerEvent containing the status, message, and job ID related to the job.
        """
        try:
            self.log.info(f"Polling for job {self.job_id} every {self.poll_interval} seconds...")

            tasks = [self._poll_status()]
            if self.fetch_logs:
                tasks.append(self._stream_logs())

            await asyncio.gather(*tasks)

            completed_status = self.hook.get_ray_job_status(self.dashboard_url, self.job_id)
            self.log.info(f"Status of completed job {self.job_id} is: {completed_status}")
            yield TriggerEvent(
                {
                    "status": completed_status,
                    "message": f"Job {self.job_id} completed with status {completed_status}",
                    "job_id": self.job_id,
                }
            )
        except Exception as e:
            self.log.error(f"Error occurred: {str(e)}")
            await self.cleanup()
            yield TriggerEvent({"status": str(JobStatus.FAILED), "message": str(e), "job_id": self.job_id})

    def _is_terminal_state(self) -> bool:
        """
        Checks if the Ray job is in a terminal state.

        A terminal state is one of the following: SUCCEEDED, STOPPED, or FAILED.

        :return: True if the job is in a terminal state, False otherwise.
        """
        return self.hook.get_ray_job_status(self.dashboard_url, self.job_id) in (
            JobStatus.SUCCEEDED,
            JobStatus.STOPPED,
            JobStatus.FAILED,
        )

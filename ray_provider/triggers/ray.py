from __future__ import annotations

import asyncio
from functools import cached_property, partial
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
    """

    def __init__(
        self,
        job_id: str,
        conn_id: str,
        xcom_dashboard_url: str | None,
        poll_interval: int = 30,
        fetch_logs: bool = True,
    ):
        super().__init__()  # type: ignore[no-untyped-call]
        self.job_id = job_id
        self.conn_id = conn_id
        self.dashboard_url = xcom_dashboard_url
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
        return RayHook(conn_id=self.conn_id, xcom_dashboard_url=self.dashboard_url)

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """
        Asynchronously polls the job status and yields events based on the job's state.

        This method gets job status at each poll interval and streams logs if available.
        It yields a TriggerEvent upon job completion, cancellation, or failure.

        :yield: TriggerEvent containing the status, message, and job ID related to the job.
        """
        try:
            self.log.info(f"Polling for job {self.job_id} every {self.poll_interval} seconds...")

            while not self._is_terminal_state():
                await asyncio.sleep(self.poll_interval)

            self.log.info(f"Fetch logs flag is set to : {self.fetch_logs}")
            if self.fetch_logs:
                # Stream logs if available
                loop = asyncio.get_event_loop()
                logs = await loop.run_in_executor(None, partial(self.hook.get_ray_job_logs, job_id=self.job_id))
                self.log.info(f"::group::{self.job_id} logs")
                for log in logs.split("\n"):
                    self.log.info(log)
                self.log.info("::endgroup::")

            completed_status = self.hook.get_ray_job_status(self.job_id)
            self.log.info(f"Status of completed job {self.job_id} is: {completed_status}")
            yield TriggerEvent(
                {
                    "status": completed_status,
                    "message": f"Job {self.job_id} completed with status {completed_status}",
                    "job_id": self.job_id,
                }
            )
        except Exception as e:
            yield TriggerEvent({"status": str(JobStatus.FAILED), "message": str(e), "job_id": self.job_id})

    def _is_terminal_state(self) -> bool:
        """
        Checks if the Ray job is in a terminal state.

        A terminal state is one of the following: SUCCEEDED, STOPPED, or FAILED.

        :return: True if the job is in a terminal state, False otherwise.
        """
        return self.hook.get_ray_job_status(self.job_id) in (JobStatus.SUCCEEDED, JobStatus.STOPPED, JobStatus.FAILED)

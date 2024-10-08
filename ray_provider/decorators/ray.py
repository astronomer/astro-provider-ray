from __future__ import annotations

import inspect
import os
import re
import shutil
import textwrap
from tempfile import mkdtemp
from typing import Any, Callable

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

from ray_provider.operators.ray import SubmitRayJob


class _RayDecoratedOperator(DecoratedOperator, SubmitRayJob):
    """
    A custom Airflow operator for Ray tasks.

    This operator combines the functionality of Airflow's DecoratedOperator
    with the Ray SubmitRayJob operator, allowing users to define tasks that
    submit jobs to a Ray cluster.

    :param config: Configuration dictionary for the Ray job.
    :param kwargs: Additional keyword arguments.
    """

    custom_operator_name = "@task.ray"

    template_fields: Any = (*SubmitRayJob.template_fields, "op_args", "op_kwargs")

    def __init__(self, config: dict[str, Any], **kwargs: Any) -> None:
        self.conn_id: str = config.get("conn_id", "")
        self.is_decorated_function = False if "entrypoint" in config else True
        self.entrypoint: str = config.get("entrypoint", "python script.py")
        self.runtime_env: dict[str, Any] = config.get("runtime_env", {})

        self.num_cpus: int | float = config.get("num_cpus", 1)
        self.num_gpus: int | float = config.get("num_gpus", 0)
        self.memory: int | float = config.get("memory", None)
        self.ray_resources: dict[str, Any] | None = config.get("resources", None)
        self.ray_cluster_yaml: str | None = config.get("ray_cluster_yaml", None)
        self.update_if_exists: bool = config.get("update_if_exists", False)
        self.kuberay_version: str = config.get("kuberay_version", "1.0.0")
        self.gpu_device_plugin_yaml: str = config.get(
            "gpu_device_plugin_yaml",
            "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml",
        )
        self.fetch_logs: bool = config.get("fetch_logs", True)
        self.wait_for_completion: bool = config.get("wait_for_completion", True)
        job_timeout_seconds: int = config.get("job_timeout_seconds", 600)
        self.poll_interval: int = config.get("poll_interval", 60)
        self.xcom_task_key: str | None = config.get("xcom_task_key", None)
        self.config = config

        if not isinstance(self.num_cpus, (int, float)):
            raise TypeError("num_cpus should be an integer or float value")
        if not isinstance(self.num_gpus, (int, float)):
            raise TypeError("num_gpus should be an integer or float value")

        super().__init__(
            conn_id=self.conn_id,
            entrypoint=self.entrypoint,
            runtime_env=self.runtime_env,
            num_cpus=self.num_cpus,
            num_gpus=self.num_gpus,
            memory=self.memory,
            resources=self.ray_resources,
            ray_cluster_yaml=self.ray_cluster_yaml,
            update_if_exists=self.update_if_exists,
            kuberay_version=self.kuberay_version,
            gpu_device_plugin_yaml=self.gpu_device_plugin_yaml,
            fetch_logs=self.fetch_logs,
            wait_for_completion=self.wait_for_completion,
            job_timeout_seconds=job_timeout_seconds,
            poll_interval=self.poll_interval,
            xcom_task_key=self.xcom_task_key,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        """
        Execute the Ray task.

        :param context: The context in which the task is being executed.
        :return: The result of the Ray job execution.
        :raises AirflowException: If job submission fails.
        """
        tmp_dir = None
        try:
            if self.is_decorated_function:
                self.log.info(
                    f"Entrypoint is not provided, is_decorated_function is set to {self.is_decorated_function}"
                )
                # Create a temporary directory that won't be immediately deleted
                temp_dir = mkdtemp(prefix="ray_")
                script_filename = os.path.join(temp_dir, "script.py")

                # Get the Python source code and extract just the function body
                full_source = inspect.getsource(self.python_callable)
                function_body = self._extract_function_body(full_source)
                if not function_body:
                    raise ValueError("Failed to retrieve Python source code")

                # Prepare the function call
                args_str = ", ".join(repr(arg) for arg in self.op_args)
                kwargs_str = ", ".join(f"{k}={repr(v)}" for k, v in self.op_kwargs.items())
                call_str = f"{self.python_callable.__name__}({args_str}, {kwargs_str})"

                # Write the script with function definition and call
                with open(script_filename, "w") as file:
                    file.write(function_body)
                    file.write(f"\n\n# Execute the function\n{call_str}\n")

                # Set up Ray job
                self.entrypoint = f"python {os.path.basename(script_filename)}"
                self.runtime_env["working_dir"] = temp_dir

            self.log.info("Running ray job...")
            result = super().execute(context)

            return result
        except Exception as e:
            self.log.error(f"Failed during execution with error: {e}")
            raise AirflowException("Job submission failed") from e
        finally:
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

    def _extract_function_body(self, source: str) -> str:
        """Extract the function, excluding only the ray.task decorator."""
        lines = source.split("\n")
        # Find the line where the ray.task decorator is
        ray_task_line = next((i for i, line in enumerate(lines) if re.match(r"^\s*@ray\.task", line.strip())), -1)

        # Include everything except the ray.task decorator line
        body = "\n".join(lines[:ray_task_line] + lines[ray_task_line + 1 :])
        # Dedent the body
        return textwrap.dedent(body)


class ray:
    @staticmethod
    def task(
        python_callable: Callable[..., Any] | None = None,
        multiple_outputs: bool | None = None,
        **kwargs: Any,
    ) -> TaskDecorator:
        """
        Decorator to define a task that submits a Ray job.

        :param python_callable: The callable function to decorate.
        :param multiple_outputs: If True, will return multiple outputs.
        :param kwargs: Additional keyword arguments.
        :return: The decorated task.
        """
        return task_decorator_factory(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            decorated_operator_class=_RayDecoratedOperator,
            **kwargs,
        )

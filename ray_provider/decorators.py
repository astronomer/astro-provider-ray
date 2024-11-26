from __future__ import annotations

import inspect
import os
import re
import tempfile
import textwrap
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

from ray_provider.operators import SubmitRayJob


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
    _config: None | dict[str, Any] | Callable[..., dict[str, Any]] = None

    template_fields: Any = (*SubmitRayJob.template_fields, "op_args", "op_kwargs")

    def __init__(self, config: dict[str, Any] | Callable[..., dict[str, Any]], **kwargs: Any) -> None:
        self._config = config
        self.kwargs = kwargs
        super().__init__(
            conn_id="",
            entrypoint="python script.py",
            runtime_env={},
            **kwargs
        )

    def _build_config(self, context: Context) -> dict:
        if isinstance(self._config, Callable):
            return self._build_config_from_callable(context)
        return self._config

    def _build_config_from_callable(self, context: Context) -> dict[str, Any]:
        config_params = inspect.signature(self._config).parameters

        config_kwargs = {k: v for k, v in self.kwargs.items() if k in config_params and k != "context"}

        if "context" in config_params:
            config_kwargs["context"] = context

        # Call config with the prepared arguments
        return self._config(**config_kwargs)

    def _load_config(self, config: dict) -> None:
        self.conn_id: str = config.get("conn_id", "")
        self.is_decorated_function = False if "entrypoint" in config else True
        self.entrypoint: str = config.get("entrypoint", "python script.py")
        self.runtime_env: dict[str, Any] = config.get("runtime_env", {})

        self.num_cpus: int | float = config.get("num_cpus", 1)
        self.num_gpus: int | float = config.get("num_gpus", 0)
        self.memory: int | float = config.get("memory")
        self.ray_resources: dict[str, Any] | None = config.get("resources")
        self.ray_cluster_yaml: str | None = config.get("ray_cluster_yaml")
        self.update_if_exists: bool = config.get("update_if_exists", False)
        self.kuberay_version: str = config.get("kuberay_version", "1.0.0")
        self.gpu_device_plugin_yaml: str = config.get("gpu_device_plugin_yaml")
        self.fetch_logs: bool = config.get("fetch_logs", True)
        self.wait_for_completion: bool = config.get("wait_for_completion", True)
        job_timeout_seconds = config.get("job_timeout_seconds", 600)
        self.job_timeout_seconds: int = timedelta(seconds=job_timeout_seconds) if job_timeout_seconds > 0 else None
        self.poll_interval: int = config.get("poll_interval", 60)
        self.xcom_task_key: str | None = config.get("xcom_task_key")

        if not isinstance(self.num_cpus, (int, float)):
            raise TypeError("num_cpus should be an integer or float value")
        if not isinstance(self.num_gpus, (int, float)):
            raise TypeError("num_gpus should be an integer or float value")

    def execute(self, context: Context) -> Any:
        """
        Execute the Ray task.

        :param context: The context in which the task is being executed.
        :return: The result of the Ray job execution.
        :raises AirflowException: If job submission fails.
        """
        config = self._build_config(context)
        self.log.info(f"Using the following config {config}")
        self._load_config(config)

        with tempfile.TemporaryDirectory(prefix="ray_") as tmpdirname:
            temp_dir = Path(tmpdirname)

            if self.is_decorated_function:
                self.log.info(
                    f"Entrypoint is not provided, is_decorated_function is set to {self.is_decorated_function}"
                )

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
                script_filename = os.path.join(temp_dir, "script.py")
                with open(script_filename, "w") as file:
                    file.write(function_body)
                    file.write(f"\n\n# Execute the function\n{call_str}\n")

                # Set up Ray job
                self.entrypoint = f"python {os.path.basename(script_filename)}"
                self.runtime_env["working_dir"] = temp_dir

            self.log.info("Running ray job...")
            result = super().execute(context)

            return result

    def _extract_function_body(self, source: str) -> str:
        """Extract the function, excluding only the ray.task decorator."""
        lines = source.split("\n")
        # TODO: This approach is extremely hacky. Review it.
        # It feels a mistake to have a user-facing module named the same as the offical ray SDK
        # In particular, the decorator is working in a very artificial way, where ray means two different things
        # at the scope of the task definition (Astro Ray Provider decorator) and inside the decorated method (Ray SDK)
        # Find the line where the ray.task decorator is
        ray_task_line = next((i for i, line in enumerate(lines) if re.match(r"^\s*@ray\.task", line.strip())), -1)

        # Include everything except the ray.task decorator line
        body = "\n".join(lines[:ray_task_line] + lines[ray_task_line + 1 :])
        self.log.info("Ray job that is going to be executed: \m %s", body)

        # Dedent the body
        return textwrap.dedent(body)


class ray:
    @staticmethod
    def task(
        python_callable: Callable[..., Any] | None = None,
        multiple_outputs: bool | None = None,
        config:  dict[str, Any] | Callable[[], dict[str, Any]] | None = None,
        **kwargs: Any,
    ) -> TaskDecorator:
        """
        Decorator to define a task that submits a Ray job.

        :param python_callable: The callable function to decorate.
        :param multiple_outputs: If True, will return multiple outputs.
        :param config: A dictionary of configuration or a callable that returns a dictionary.
        :param kwargs: Additional keyword arguments.
        :return: The decorated task.
        """
        config = config or {}
        return task_decorator_factory(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            decorated_operator_class=_RayDecoratedOperator,
            config=config,
            **kwargs,
        )

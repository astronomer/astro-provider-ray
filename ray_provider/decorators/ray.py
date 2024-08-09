from __future__ import annotations

import os
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

    template_fields: Any = (*DecoratedOperator.template_fields, *SubmitRayJob.template_fields)

    def __init__(self, config: dict[str, Any], **kwargs: Any) -> None:
        self.conn_id: str = config.get("conn_id", "")
        self.is_decorated_function = False if "entrypoint" in config else True
        self.entrypoint: str = config.get("entrypoint", "python script.py")
        self.runtime_env: dict[str, Any] = config.get("runtime_env", {})

        self.num_cpus: int | float = config.get("num_cpus", 1)
        self.num_gpus: int | float = config.get("num_gpus", 0)
        self.memory: int | float = config.get("memory", None)
        self.ray_resources: dict[str, Any] | None = config.get("resources", None)
        self.fetch_logs: bool = config.get("fetch_logs", True)
        self.wait_for_completion: bool = config.get("wait_for_completion", True)
        self.job_timeout_seconds: int = config.get("job_timeout_seconds", 600)
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
            fetch_logs=self.fetch_logs,
            wait_for_completion=self.wait_for_completion,
            job_timeout_seconds=self.job_timeout_seconds,
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
                tmp_dir = mkdtemp(prefix="ray_")
                py_source = self.get_python_source().splitlines()  # type: ignore
                function_body = textwrap.dedent("\n".join(py_source))

                script_filename = os.path.join(tmp_dir, "script.py")
                with open(script_filename, "w") as file:
                    all_args_str = self._build_args_str()
                    script_body = f"{function_body}\n{self._extract_function_name()}({all_args_str})"
                    file.write(script_body)

                self.entrypoint = "python script.py"
                self.runtime_env["working_dir"] = tmp_dir

            self.log.info("Running ray job...")

            result = super().execute(context)
            return result
        except Exception as e:
            self.log.error(f"Failed during execution with error: {e}")
            raise AirflowException("Job submission failed") from e
        finally:
            if tmp_dir and os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

    def _build_args_str(self) -> str:
        """
        Build the argument string for the function call.

        :return: A string representation of the function arguments.
        """
        args_str = ", ".join(repr(arg) for arg in self.op_args)
        kwargs_str = ", ".join(f"{k}={repr(v)}" for k, v in self.op_kwargs.items())
        return f"{args_str}, {kwargs_str}" if args_str and kwargs_str else args_str or kwargs_str

    def _extract_function_name(self) -> str:
        """
        Extract the name of the Python callable.

        :return: The name of the Python callable.
        """
        return self.python_callable.__name__


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

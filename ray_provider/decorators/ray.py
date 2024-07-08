from __future__ import annotations

import os
import shutil
import textwrap
from tempfile import mkdtemp
from typing import TYPE_CHECKING, Callable, Sequence

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.utils.context import Context

from ray_provider.operators.ray import SubmitRayJob

if TYPE_CHECKING:
    from airflow.utils.context import Context


class _RayDecoratedOperator(DecoratedOperator, SubmitRayJob):
    """
    A custom Airflow operator for Ray tasks.

    This operator combines the functionality of Airflow's DecoratedOperator
    with the Ray SubmitRayJob operator, allowing users to define tasks that
    submit jobs to a Ray cluster.

    :param custom_operator_name: Required. Custom operator name.
    :param template_fields: Required. Fields that are template-able.
    :param template_fields_renderers: Required. Fields renderers for templates.
    :param config: Required. Configuration dictionary for the Ray job.
    """

    custom_operator_name = "@task.ray"

    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *SubmitRayJob.template_fields)
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **SubmitRayJob.template_fields_renderers,
    }

    def __init__(self, config: dict, **kwargs) -> None:
        # Setting default values if not provided in the configuration
        self.conn_id = config.get("conn_id")
        self.entrypoint = config.get("entrypoint", "python script.py")  # Default entrypoint if not provided
        self.runtime_env = config.get("runtime_env", {})

        self.num_cpus = config.get("num_cpus")
        self.num_gpus = config.get("num_gpus")
        self.memory = config.get("memory")
        self.config = config

        if isinstance(self.num_cpus, str):
            raise TypeError("num_cpus should be an integer or float value")
        if isinstance(self.num_gpus, str):
            raise TypeError("num_gpus should be an integer or float value")

        # Ensuring we pass all necessary initialization parameters to the superclass
        super().__init__(
            conn_id=self.conn_id,
            entrypoint=self.entrypoint,
            runtime_env=self.runtime_env,
            num_cpus=self.num_cpus,
            num_gpus=self.num_gpus,
            memory=self.memory,
            **kwargs,
        )

    def execute(self, context: Context):
        tmp_dir = mkdtemp(prefix="ray_")  # Manually create a temp directory
        try:
            py_source = self.get_python_source().splitlines()
            function_body = textwrap.dedent("\n".join(py_source[1:]))

            script_filename = os.path.join(tmp_dir, "script.py")
            with open(script_filename, "w") as file:
                # Creating a function call string with arguments from function_args
                args_str = ", ".join(repr(arg) for arg in self.op_args)
                kwargs_str = ", ".join(f"{k}={repr(v)}" for k, v in self.op_kwargs.items())
                # Combine args_str and kwargs_str
                if args_str and kwargs_str:
                    all_args_str = f"{args_str}, {kwargs_str}"
                elif args_str:
                    all_args_str = args_str
                else:
                    all_args_str = kwargs_str

                script_body = f"{function_body}\n{self._extract_function_name()}({all_args_str})"
                file.write(script_body)

            self.log.info(script_body)

            self.entrypoint = "python script.py"
            self.runtime_env["working_dir"] = tmp_dir
            self.log.info("Running ray job...")

            result = super().execute(context)  # Execute the job
        except Exception as e:
            self.log.error(f"Failed during execution with error: {e}")
            raise AirflowException("Job submission failed")
        finally:
            # Cleanup: Delayed until after job execution confirmation
            if os.path.exists(tmp_dir):
                shutil.rmtree(tmp_dir)

        return result

    def _extract_function_name(self):
        # Directly using __name__ attribute to retrieve the function name
        return self.python_callable.__name__


def ray(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Decorator to define a task that submits a Ray job.

    This decorator allows defining a task that submits a Ray job, handling multiple outputs if needed.

    :param python_callable: Required. The callable function to decorate.
    :param multiple_outputs: Optional. If True, will return multiple outputs.
    :param kwargs: Additional keyword arguments.

    :returns: The decorated task.
    :rtype: TaskDecorator
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_RayDecoratedOperator,
        **kwargs,
    )


task.ray = ray  # Assign the ray decorator to task.ray
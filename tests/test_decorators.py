from datetime import timedelta
from unittest.mock import MagicMock, patch

import pytest
from airflow.utils.context import Context

from ray_provider.decorators import _RayDecoratedOperator, ray
from ray_provider.exceptions import RayAirflowException


class TestRayDecoratedOperator:
    def test_initialization(self):
        config = {
            "conn_id": "ray_default",
            "entrypoint": "python my_script.py",
            "runtime_env": {"pip": ["ray"]},
            "num_cpus": 2,
            "num_gpus": 1,
            "memory": 1024,
            "resources": {"custom_resource": 1},
            "fetch_logs": True,
            "wait_for_completion": True,
            "job_timeout_seconds": 300,
            "poll_interval": 30,
            "xcom_task_key": "ray_result",
        }

        def dummy_callable():
            pass

        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)
        operator._load_config(config)

        assert operator.conn_id == "ray_default"
        assert operator.entrypoint == "python my_script.py"
        assert operator.runtime_env == {"pip": ["ray"]}
        assert operator.num_cpus == 2
        assert operator.num_gpus == 1
        assert operator.memory == 1024
        assert operator.ray_resources == {"custom_resource": 1}
        assert operator.fetch_logs == True
        assert operator.wait_for_completion == True
        assert operator.job_timeout_seconds == timedelta(seconds=300)
        assert operator.poll_interval == 30
        assert operator.xcom_task_key == "ray_result"

    def test_initialization_defaults(self):
        config = {}

        def dummy_callable():
            pass

        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)
        operator._load_config(config)

        assert operator.conn_id == ""
        assert operator.entrypoint == "python script.py"
        assert operator.runtime_env == {}
        assert operator.num_cpus == 1
        assert operator.num_gpus == 0
        assert operator.memory == 1
        assert operator.ray_resources is None
        assert operator.fetch_logs == True
        assert operator.wait_for_completion == True
        assert operator.job_timeout_seconds == timedelta(seconds=600)
        assert operator.poll_interval == 60
        assert operator.xcom_task_key is None

    def test_callable_config(self):
        def dummy_callable():
            pass

        callable_config = lambda context: {"ray_cluster_yaml": "different.yml"}

        operator = _RayDecoratedOperator(task_id="test_task", config=callable_config, python_callable=dummy_callable)
        new_config = operator._build_config(context={})
        operator._load_config(new_config)

        assert operator.ray_cluster_yaml == "different.yml"

    def test_invalid_config_raises_exception(self):
        config = {
            "num_cpus": "invalid_number",
        }

        def dummy_callable():
            pass

        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)

        with pytest.raises(RayAirflowException):
            operator._load_config(config)

        config["num_cpus"] = 1
        config["num_gpus"] = "invalid_number"

        with pytest.raises(RayAirflowException):
            operator._load_config(config)

    @patch.object(_RayDecoratedOperator, "_extract_function_body")
    @patch("ray_provider.decorators.SubmitRayJob.execute")
    def test_execute_decorated_function(self, mock_super_execute, mock_extract_function_body):
        config = {
            "runtime_env": {"pip": ["ray"]},
        }

        def dummy_callable():
            pass

        context = MagicMock(spec=Context)
        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)
        mock_extract_function_body.return_value = "def dummy_callable():\n    pass\n"
        mock_super_execute.return_value = "success"

        result = operator.execute(context)

        assert result == "success"
        assert operator.entrypoint == "python script.py"
        assert "working_dir" in operator.runtime_env

    @patch("ray_provider.decorators.SubmitRayJob.execute")
    def test_execute_with_entrypoint(self, mock_super_execute):
        config = {
            "entrypoint": "python my_script.py",
        }

        def dummy_callable():
            pass

        context = MagicMock(spec=Context)
        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)
        mock_super_execute.return_value = "success"

        result = operator.execute(context)

        assert result == "success"
        assert operator.entrypoint == "python my_script.py"

    @patch("ray_provider.decorators.SubmitRayJob.execute")
    def test_execute_failure(self, mock_super_execute):
        config = {}

        def dummy_callable():
            pass

        context = MagicMock(spec=Context)
        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)
        mock_super_execute.side_effect = Exception("Ray job failed")

        with pytest.raises(Exception):
            operator.execute(context)

    def test_extract_function_body(self):
        config = {}

        @ray.task()
        def dummy_callable():
            return "dummy"

        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)

        function_body = operator._extract_function_body(
            """@ray.task()
        def dummy_callable():
            return "dummy"
        """
        )
        assert (
            function_body
            == """def dummy_callable():
    return "dummy"
"""
        )

    def test_extract_function_body_invalid_body(self):
        config = {}

        @ray.task()
        def dummy_callable():
            return "dummy"

        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)

        with pytest.raises(RayAirflowException) as exc_info:
            operator._extract_function_body(
                """@ray_decorator.task()
            def dummy_callable():
                return "dummy"
            """
            )
        assert str(exc_info.value) == "Unable to parse this body. Expects the `@ray.task` decorator."

    def test_extract_function_body_empty_body(self):
        config = {}

        @ray.task()
        def dummy_callable():
            return "dummy"

        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)

        with pytest.raises(RayAirflowException) as exc_info:
            operator._extract_function_body("""@ray.task()""")
        assert str(exc_info.value) == "Failed to extract Ray pipeline code decorated with @ray.task"


class TestRayTaskDecorator:
    def test_ray_task_decorator(self):
        @ray.task()
        def dummy_function():
            return "dummy"

        assert callable(dummy_function)
        assert hasattr(dummy_function, "operator_class")
        assert dummy_function.operator_class == _RayDecoratedOperator

    def test_ray_task_decorator_with_multiple_outputs(self):
        @ray.task(multiple_outputs=True)
        def dummy_function():
            return {"key": "value"}

        assert callable(dummy_function)
        assert hasattr(dummy_function, "operator_class")
        assert dummy_function.operator_class == _RayDecoratedOperator

    def test_ray_task_decorator_with_config(self):
        config = {
            "num_cpus": 2,
            "num_gpus": 1,
            "memory": 1024,
        }

        @ray.task(**config)
        def dummy_function():
            return "dummy"

        assert callable(dummy_function)
        assert hasattr(dummy_function, "operator_class")
        assert dummy_function.operator_class == _RayDecoratedOperator

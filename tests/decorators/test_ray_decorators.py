from unittest.mock import MagicMock, patch

import pytest
from airflow.decorators.base import _TaskDecorator
from airflow.utils.context import Context

from ray_provider.decorators.ray import _RayDecoratedOperator, ray_task
from ray_provider.operators.ray import SubmitRayJob

DEFAULT_DATE = "2023-01-01"


class TestRayDecoratedOperator:
    default_date = DEFAULT_DATE

    def test_initialization(self):
        config = {
            "host": "http://localhost:8265",
            "entrypoint": "python my_script.py",
            "runtime_env": {"pip": ["ray"]},
            "num_cpus": 2,
            "num_gpus": 1,
            "memory": "1G",
        }

        def dummy_callable():
            pass

        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)

        assert operator.entrypoint == "python my_script.py"
        assert operator.runtime_env == {"pip": ["ray"]}
        assert operator.num_cpus == 2
        assert operator.num_gpus == 1
        assert operator.memory == "1G"

    @patch.object(_RayDecoratedOperator, "get_python_source")
    @patch.object(SubmitRayJob, "execute")
    def test_execute(self, mock_super_execute, mock_get_python_source):
        config = {
            "entrypoint": "python my_script.py",
            "runtime_env": {"pip": ["ray"]},
        }

        def dummy_callable():
            pass

        context = MagicMock(spec=Context)
        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)

        mock_get_python_source.return_value = "def my_function():\n    pass\n"
        mock_super_execute.return_value = "success"

        result = operator.execute(context)

        assert result == "success"

    def test_missing_host_config(self):
        config = {
            "entrypoint": "python my_script.py",
        }

        def dummy_callable():
            pass

        operator = _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)
        assert operator.entrypoint == "python my_script.py"

    def test_invalid_config_raises_exception(self):
        config = {
            "host": "http://localhost:8265",
            "entrypoint": "python my_script.py",
            "runtime_env": {"pip": ["ray"]},
            "num_cpus": "invalid_number",
        }

        def dummy_callable():
            pass

        with pytest.raises(TypeError):
            _RayDecoratedOperator(task_id="test_task", config=config, python_callable=dummy_callable)


class TestRayTaskDecorator:

    def test_ray_task_decorator(self):
        def dummy_function():
            return "dummy"

        decorator = ray_task(python_callable=dummy_function)
        assert isinstance(decorator, _TaskDecorator)

    def test_ray_task_decorator_with_multiple_outputs(self):
        def dummy_function():
            return {"key": "value"}

        decorator = ray_task(python_callable=dummy_function, multiple_outputs=True)
        assert isinstance(decorator, _TaskDecorator)

from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.exceptions import AirflowException
from ray.job_submission import JobStatus

from ray_provider.operators.ray import DeleteRayCluster, SetupRayCluster, SubmitRayJob


class TestSetupRayCluster:
    @pytest.fixture
    def mock_hook(self):
        with patch("ray_provider.operators.ray.RayHook") as mock:
            yield mock.return_value

    @pytest.fixture
    def operator(self):
        return SetupRayCluster(task_id="test_setup_ray_cluster", conn_id="test_conn", ray_cluster_yaml="cluster.yaml")

    def test_init(self):
        operator = SetupRayCluster(
            task_id="test_setup_ray_cluster",
            conn_id="test_conn",
            ray_cluster_yaml="cluster.yaml",
            kuberay_version="1.1.0",
            gpu_device_plugin_yaml="custom_gpu_plugin.yaml",
            update_if_exists=True,
        )
        assert operator.conn_id == "test_conn"
        assert operator.ray_cluster_yaml == "cluster.yaml"
        assert operator.kuberay_version == "1.1.0"
        assert operator.gpu_device_plugin_yaml == "custom_gpu_plugin.yaml"
        assert operator.update_if_exists is True

    def test_init_default_values(self):
        operator = SetupRayCluster(
            task_id="test_setup_ray_cluster",
            conn_id="test_conn",
            ray_cluster_yaml="cluster.yaml",
        )
        assert operator.kuberay_version == "1.0.0"
        assert (
            operator.gpu_device_plugin_yaml
            == "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml"
        )
        assert operator.update_if_exists is False

    def test_hook_property(self, operator):
        with patch("ray_provider.operators.ray.RayHook") as mock_ray_hook:
            hook = operator.hook
            mock_ray_hook.assert_called_once_with(conn_id=operator.conn_id)
            assert hook == mock_ray_hook.return_value

    def test_execute(self, operator, mock_hook):
        context = MagicMock()
        operator.execute(context)
        mock_hook.setup_ray_cluster.assert_called_once_with(
            context=context,
            ray_cluster_yaml=operator.ray_cluster_yaml,
            kuberay_version=operator.kuberay_version,
            gpu_device_plugin_yaml=operator.gpu_device_plugin_yaml,
            update_if_exists=operator.update_if_exists,
        )


class TestDeleteRayCluster:
    @pytest.fixture
    def mock_hook(self):
        with patch("ray_provider.operators.ray.RayHook") as mock:
            yield mock.return_value

    @pytest.fixture
    def operator(self):
        return DeleteRayCluster(task_id="test_delete_ray_cluster", conn_id="test_conn", ray_cluster_yaml="cluster.yaml")

    def test_init(self):
        operator = DeleteRayCluster(
            task_id="test_delete_ray_cluster",
            conn_id="test_conn",
            ray_cluster_yaml="cluster.yaml",
            gpu_device_plugin_yaml="custom_gpu_plugin.yaml",
        )
        assert operator.conn_id == "test_conn"
        assert operator.ray_cluster_yaml == "cluster.yaml"
        assert operator.gpu_device_plugin_yaml == "custom_gpu_plugin.yaml"

    def test_init_default_gpu_plugin(self):
        operator = DeleteRayCluster(
            task_id="test_delete_ray_cluster",
            conn_id="test_conn",
            ray_cluster_yaml="cluster.yaml",
        )
        assert (
            operator.gpu_device_plugin_yaml
            == "https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/v0.9.0/nvidia-device-plugin.yml"
        )

    def test_hook_property(self, operator):
        with patch("ray_provider.operators.ray.RayHook") as mock_ray_hook:
            hook = operator.hook
            mock_ray_hook.assert_called_once_with(conn_id=operator.conn_id)
            assert hook == mock_ray_hook.return_value

    def test_execute(self, operator, mock_hook):
        context = MagicMock()
        operator.execute(context)
        mock_hook.delete_ray_cluster.assert_called_once_with(operator.ray_cluster_yaml, operator.gpu_device_plugin_yaml)


class TestSubmitRayJob:

    @pytest.fixture
    def mock_hook(self):
        with patch("ray_provider.operators.ray.RayHook") as mock:
            yield mock.return_value

    @pytest.fixture
    def operator(self):
        return SubmitRayJob(task_id="test_task", conn_id="test_conn", entrypoint="python script.py", runtime_env={})

    @pytest.fixture
    def task_instance(self):
        return Mock()

    @pytest.fixture
    def context(self, task_instance):
        return {"ti": task_instance, "task": Mock()}

    def test_init(self):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={"pip": ["package1", "package2"]},
            num_cpus=2,
            num_gpus=1,
            memory=1000,
            resources={"custom_resource": 1},
            ray_cluster_yaml="cluster.yaml",
            kuberay_version="1.0.0",
            update_if_exists=True,
            gpu_device_plugin_yaml="https://example.com/plugin.yml",
            fetch_logs=True,
            wait_for_completion=True,
            job_timeout_seconds=1200,
            poll_interval=30,
            xcom_task_key="task.key",
        )

        assert operator.conn_id == "test_conn"
        assert operator.entrypoint == "python script.py"
        assert operator.runtime_env == {"pip": ["package1", "package2"]}
        assert operator.num_cpus == 2
        assert operator.num_gpus == 1
        assert operator.memory == 1000
        assert operator.ray_resources == {"custom_resource": 1}
        assert operator.ray_cluster_yaml == "cluster.yaml"
        assert operator.kuberay_version == "1.0.0"
        assert operator.update_if_exists == True
        assert operator.gpu_device_plugin_yaml == "https://example.com/plugin.yml"
        assert operator.fetch_logs == True
        assert operator.wait_for_completion == True
        assert operator.job_timeout_seconds == 1200
        assert operator.poll_interval == 30
        assert operator.xcom_task_key == "task.key"

    def test_on_kill(self, mock_hook):
        operator = SubmitRayJob(task_id="test_task", conn_id="test_conn", entrypoint="python script.py", runtime_env={})
        operator.job_id = "test_job_id"
        operator.hook = mock_hook
        operator.dashboard_url = "http://dashboard.url"
        operator.ray_cluster_yaml = "cluster.yaml"

        with patch.object(operator, "_delete_cluster") as mock_delete_cluster:
            operator.on_kill()

            mock_hook.delete_ray_job.assert_called_once_with("http://dashboard.url", "test_job_id")
            mock_delete_cluster.assert_called_once()

    def test_get_dashboard_url_with_xcom(self, context, task_instance):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            xcom_task_key="task.key",
        )

        task_instance.xcom_pull.return_value = "http://dashboard.url"
        result = operator._get_dashboard_url(context)

        assert result == "http://dashboard.url"
        task_instance.xcom_pull.assert_called_once_with(task_ids="task", key="key")

    def test_get_dashboard_url_without_xcom(self, context):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
        )

        result = operator._get_dashboard_url(context)

        assert result is None

    @patch("ray_provider.operators.ray.RayHook")
    def test_setup_cluster(self, mock_ray_hook, context):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            ray_cluster_yaml="cluster.yaml",
            kuberay_version="1.0.0",
            update_if_exists=True,
            gpu_device_plugin_yaml="https://example.com/plugin.yml",
        )

        mock_hook = mock_ray_hook.return_value
        operator.hook = mock_hook

        operator._setup_cluster(context)

        mock_hook.setup_ray_cluster.assert_called_once_with(
            context=context,
            ray_cluster_yaml="cluster.yaml",
            kuberay_version="1.0.0",
            gpu_device_plugin_yaml="https://example.com/plugin.yml",
            update_if_exists=True,
        )

    @patch("ray_provider.operators.ray.RayHook")
    def test_delete_cluster(self, mock_ray_hook):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            ray_cluster_yaml="cluster.yaml",
            gpu_device_plugin_yaml="https://example.com/plugin.yml",
        )

        mock_hook = mock_ray_hook.return_value
        operator.hook = mock_hook

        operator._delete_cluster()

        mock_hook.delete_ray_cluster.assert_called_once_with(
            ray_cluster_yaml="cluster.yaml",
            gpu_device_plugin_yaml="https://example.com/plugin.yml",
        )

    def test_execute_without_wait(self, mock_hook, context):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=False,
        )

        mock_hook.submit_ray_job.return_value = "test_job_id"

        with patch.object(operator, "_setup_cluster") as mock_setup_cluster:
            result = operator.execute(context)

            mock_setup_cluster.assert_called_once_with(context=context)
            assert result == "test_job_id"
            mock_hook.submit_ray_job.assert_called_once_with(
                dashboard_url=None,
                entrypoint="python script.py",
                runtime_env={},
                entrypoint_num_cpus=0,
                entrypoint_num_gpus=0,
                entrypoint_memory=0,
                entrypoint_resources=None,
            )

    @pytest.mark.parametrize(
        "job_status,expected_action",
        [
            (JobStatus.PENDING, "defer"),
            (JobStatus.RUNNING, "defer"),
            (JobStatus.SUCCEEDED, None),
            (JobStatus.FAILED, "raise"),
            (JobStatus.STOPPED, "raise"),
        ],
    )
    def test_execute_with_wait(self, mock_hook, context, job_status, expected_action):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=True,
        )

        mock_hook.submit_ray_job.return_value = "test_job_id"
        mock_hook.get_ray_job_status.return_value = job_status

        with patch.object(operator, "_setup_cluster"):
            if expected_action == "defer":
                with patch.object(operator, "defer") as mock_defer:
                    operator.execute(context)
                    mock_defer.assert_called_once()
            elif expected_action == "raise":
                with pytest.raises(AirflowException):
                    operator.execute(context)
            else:
                result = operator.execute(context)
                assert result == "test_job_id"

    @pytest.mark.parametrize(
        "event_status,expected_action",
        [
            (JobStatus.SUCCEEDED, None),
            (JobStatus.FAILED, "raise"),
            (JobStatus.STOPPED, "raise"),
            ("UNEXPECTED", "raise"),
        ],
    )
    def test_execute_complete(self, operator, event_status, expected_action):
        operator.job_id = "test_job_id"
        event = {"status": event_status, "message": "Test message"}

        with patch.object(operator, "_delete_cluster") as mock_delete_cluster:
            if expected_action == "raise":
                with pytest.raises(AirflowException):
                    operator.execute_complete({}, event)
            else:
                operator.execute_complete({}, event)

            # _delete_cluster should be called in all cases
            mock_delete_cluster.assert_called_once()

    def test_template_fields(self):
        assert SubmitRayJob.template_fields == (
            "conn_id",
            "entrypoint",
            "runtime_env",
            "num_cpus",
            "num_gpus",
            "memory",
            "xcom_task_key",
            "ray_cluster_yaml",
            "job_timeout_seconds",
        )

    @patch("ray_provider.operators.ray.RayHook")
    def test_setup_cluster_exception(self, mock_ray_hook, context):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            ray_cluster_yaml="cluster.yaml",
        )

        mock_hook = mock_ray_hook.return_value
        operator.hook = mock_hook

        mock_hook.setup_ray_cluster.side_effect = Exception("Cluster setup failed")

        with pytest.raises(Exception) as exc_info:
            operator._setup_cluster(context)

        assert str(exc_info.value) == "Cluster setup failed"
        mock_hook.setup_ray_cluster.assert_called_once()

    @patch("ray_provider.operators.ray.RayHook")
    def test_delete_cluster_exception(self, mock_ray_hook):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            ray_cluster_yaml="cluster.yaml",
        )

        mock_hook = mock_ray_hook.return_value
        operator.hook = mock_hook

        mock_hook.delete_ray_cluster.side_effect = Exception("Cluster deletion failed")

        with pytest.raises(Exception) as exc_info:
            operator._delete_cluster()

        assert str(exc_info.value) == "Cluster deletion failed"
        mock_hook.delete_ray_cluster.assert_called_once()

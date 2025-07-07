from datetime import timedelta
from unittest.mock import MagicMock, Mock, patch

import pytest
from airflow.exceptions import TaskDeferred
from kubernetes.client.exceptions import ApiException
from ray.job_submission import JobStatus

from ray_provider.exceptions import RayAirflowException
from ray_provider.operators import DeleteRayCluster, SetupRayCluster, SubmitRayJob
from ray_provider.triggers import RayJobTrigger


class TestSetupRayCluster:

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
        assert not operator.gpu_device_plugin_yaml
        assert operator.update_if_exists is False

    @patch("ray_provider.operators.RayHook")
    def test_hook_property(self, mock_ray_hook):
        operator = SetupRayCluster(
            task_id="test_setup_ray_cluster", conn_id="test_conn", ray_cluster_yaml="cluster.yaml"
        )
        operator.hook
        mock_ray_hook.assert_called_once_with(conn_id=operator.conn_id)

    @patch("ray_provider.operators.SetupRayCluster.hook.setup_ray_cluster")
    @patch("ray_provider.operators.SetupRayCluster.hook")
    def test_execute(self, mock_ray_hook, mock_setup_ray_cluster):
        operator = SetupRayCluster(
            task_id="test_setup_ray_cluster", conn_id="test_conn", ray_cluster_yaml="cluster.yaml"
        )

        context = MagicMock()
        operator.execute(context)

        mock_setup_ray_cluster.assert_called_once_with(
            context=context,
            ray_cluster_yaml=operator.ray_cluster_yaml,
            kuberay_version=operator.kuberay_version,
            gpu_device_plugin_yaml=operator.gpu_device_plugin_yaml,
            update_if_exists=operator.update_if_exists,
        )


class TestDeleteRayCluster:

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
        assert not operator.gpu_device_plugin_yaml

    @patch("ray_provider.operators.RayHook")
    def test_hook_property(self, mock_ray_hook):
        operator = DeleteRayCluster(
            task_id="test_delete_ray_cluster", conn_id="test_conn", ray_cluster_yaml="cluster.yaml"
        )
        operator.hook
        mock_ray_hook.assert_called_once_with(conn_id=operator.conn_id)

    @patch("ray_provider.operators.DeleteRayCluster.hook")
    def test_execute(self, mock_hook):
        operator = DeleteRayCluster(
            task_id="test_delete_ray_cluster", conn_id="test_conn", ray_cluster_yaml="cluster.yaml"
        )
        context = MagicMock()
        operator.execute(context)
        mock_hook.delete_ray_cluster.assert_called_once_with(operator.ray_cluster_yaml, operator.gpu_device_plugin_yaml)


class TestSubmitRayJob:

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
        assert operator.job_timeout_seconds == timedelta(seconds=1200)
        assert operator.poll_interval == 30
        assert operator.xcom_task_key == "task.key"

    def test_init_no_timeout(self):
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
            job_timeout_seconds=0,
            poll_interval=30,
            xcom_task_key="task.key",
        )
        assert operator.job_timeout_seconds is None

    @patch("ray_provider.operators.SubmitRayJob._delete_cluster")
    @patch("ray_provider.operators.SubmitRayJob.hook.delete_ray_job")
    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_on_kill(self, mock_hook, mock_delete_ray_job, mock_delete_cluster):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            ray_cluster_yaml="cluster.yaml",
        )
        operator.job_id = "test_job_id"
        operator.dashboard_url = "http://dashboard.url"

        operator.on_kill()

        mock_delete_ray_job.assert_called_once_with("http://dashboard.url", "test_job_id")
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

    @patch("ray_provider.operators.RayHook")
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

        operator.hook = mock_ray_hook.return_value

        operator._setup_cluster(context)

        mock_ray_hook.return_value.setup_ray_cluster.assert_called_once_with(
            context=context,
            ray_cluster_yaml="cluster.yaml",
            kuberay_version="1.0.0",
            gpu_device_plugin_yaml="https://example.com/plugin.yml",
            update_if_exists=True,
        )

    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_delete_cluster(self, mock_ray_hook):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            ray_cluster_yaml="cluster.yaml",
            gpu_device_plugin_yaml="https://example.com/plugin.yml",
        )
        operator._delete_cluster()

        mock_ray_hook.delete_ray_cluster.assert_called_once_with(
            ray_cluster_yaml="cluster.yaml",
            gpu_device_plugin_yaml="https://example.com/plugin.yml",
        )

    @patch("ray_provider.operators.SubmitRayJob._setup_cluster")
    @patch("ray_provider.operators.SubmitRayJob.hook.submit_ray_job", return_value="test_job_id")
    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_execute_without_wait(self, mock_hook, mock_submit_ray_job, mock_setup_cluster, context):

        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=False,
        )

        result = operator.execute(context)
        assert result == "test_job_id"

        mock_setup_cluster.assert_called_once_with(context=context)

        mock_submit_ray_job.assert_called_once_with(
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
        ],
    )
    @patch("ray_provider.operators.SubmitRayJob._setup_cluster")
    @patch("ray_provider.operators.SubmitRayJob.hook.submit_ray_job", return_value="test_job_id")
    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_execute_with_wait(self, mock_hook, mock_setup_cluster, context, job_status, expected_action):
        mock_hook.get_ray_job_status.return_value = job_status

        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=True,
        )

        if expected_action == "defer":
            with patch.object(operator, "defer") as mock_defer:
                operator.execute(context)
                mock_defer.assert_called_once()
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
    @patch("ray_provider.operators.SubmitRayJob._delete_cluster")
    def test_execute_complete(self, mock_delete_cluster, event_status, expected_action):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
        )
        operator.job_id = "test_job_id"
        event = {"status": event_status, "message": "Test message"}

        if expected_action == "raise":
            with pytest.raises(RayAirflowException):
                operator.execute_complete({}, event)
        else:
            operator.execute_complete({}, event)

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

    @patch("ray_provider.operators.RayHook")
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

    @patch("ray_provider.operators.RayHook")
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

    @pytest.mark.parametrize(
        "xcom_task_key, expected_task, expected_key",
        [
            ("task.key", "task", "key"),
            ("single_key", None, "single_key"),
        ],
    )
    def test_get_dashboard_url_xcom_variants(self, context, xcom_task_key, expected_task, expected_key):
        operator = SubmitRayJob(task_id="test_task", conn_id="test_conn", entrypoint="python script.py", runtime_env={})
        operator.xcom_task_key = xcom_task_key
        context["ti"].xcom_pull.return_value = "http://dashboard.url"

        result = operator._get_dashboard_url(context)

        assert result == "http://dashboard.url"
        if expected_task:
            context["ti"].xcom_pull.assert_called_once_with(task_ids=expected_task, key=expected_key)
        else:
            context["ti"].xcom_pull.assert_called_once_with(task_ids=context["task"].task_id, key=expected_key)

    @patch("ray_provider.operators.SubmitRayJob._setup_cluster")
    @patch("ray_provider.operators.SubmitRayJob.hook.get_ray_job_status", return_value="UNEXPECTED_STATE")
    @patch("ray_provider.operators.SubmitRayJob.hook.submit_ray_job", return_value="test_job_id")
    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_execute_job_unexpected_state(
        self, mock_hook, mock_submit_ray_job, mock_get_ray_job_status, mock_setup_cluster, context
    ):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            operator.execute(context)

        assert isinstance(exc_info.value.trigger, RayJobTrigger)

    @pytest.mark.parametrize("dashboard_url", [None, "http://dashboard.url"])
    @patch("ray_provider.operators.SubmitRayJob._setup_cluster")
    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_execute_defer(self, mock_hook, mock_setup_cluster, context, dashboard_url):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=True,
            ray_cluster_yaml="cluster.yaml",
            gpu_device_plugin_yaml="gpu_plugin.yaml",
            poll_interval=30,
            fetch_logs=True,
            job_timeout_seconds=600,
        )
        mock_hook.submit_ray_job.return_value = "test_job_id"
        mock_hook.get_ray_job_status.return_value = JobStatus.PENDING

        with patch.object(operator, "_get_dashboard_url", return_value=dashboard_url), pytest.raises(
            TaskDeferred
        ) as exc_info:
            operator.execute(context)

        trigger = exc_info.value.trigger
        assert isinstance(trigger, RayJobTrigger)
        assert trigger.job_id == "test_job_id"
        assert trigger.conn_id == "test_conn"
        assert trigger.dashboard_url == dashboard_url
        assert trigger.ray_cluster_yaml == "cluster.yaml"
        assert trigger.gpu_device_plugin_yaml == "gpu_plugin.yaml"
        assert trigger.poll_interval == 30
        assert trigger.fetch_logs is True

    @patch("ray_provider.operators.SubmitRayJob._delete_cluster")
    def test_execute_complete_unexpected_status(self, mock_delete_cluster):
        operator = SubmitRayJob(task_id="test_task", conn_id="test_conn", entrypoint="python script.py", runtime_env={})
        event = {"status": "UNEXPECTED", "message": "Unexpected status"}
        with pytest.raises(RayAirflowException) as exc_info:
            operator.execute_complete({}, event)

        assert "Encountered unexpected state" in str(exc_info.value)

    @patch("ray_provider.operators.SubmitRayJob._delete_cluster")
    def test_execute_complete_cleanup_on_exception(self, mock_delete_cluster):
        operator = SubmitRayJob(task_id="test_task", conn_id="test_conn", entrypoint="python script.py", runtime_env={})
        event = {"status": JobStatus.FAILED, "message": "Job failed"}
        with pytest.raises(RayAirflowException):
            operator.execute_complete({}, event)

        mock_delete_cluster.assert_called_once()

    @patch("ray_provider.operators.SubmitRayJob._setup_cluster")
    @patch("ray_provider.operators.SubmitRayJob._delete_cluster")
    @patch("ray_provider.operators.SubmitRayJob.hook.submit_ray_job", side_effect=Exception("Job submission failed"))
    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_execute_exception_handling(self, mock_hook, mock_delete, mock_setup, context):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            ray_cluster_yaml="cluster.yaml",
        )

        with pytest.raises(Exception) as exc_info:
            operator.execute(context)

        assert "Job submission failed" == str(exc_info.value)

    @patch("ray_provider.operators.SubmitRayJob._delete_cluster")
    @patch(
        "ray_provider.operators.SubmitRayJob.hook.setup_ray_cluster", side_effect=ApiException("Cluster setup failed")
    )
    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_execute_cluster_setup_exception(self, mock_hook, mock_setup_cluster, mock_delete_cluster, context):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            ray_cluster_yaml="cluster.yaml",
        )

        with pytest.raises(ApiException) as exc_info:
            operator.execute(context)

        assert "Cluster setup failed" in str(exc_info.value)
        mock_delete_cluster.assert_called_once()

    @patch("ray_provider.operators.RayHook")
    def test_execute_with_wait_and_defer(self, mock_hook, context):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=True,
            poll_interval=30,
            fetch_logs=True,
            job_timeout_seconds=600,
        )

        mock_hook.submit_ray_job.return_value = "test_job_id"
        mock_hook.get_ray_job_status.return_value = JobStatus.PENDING

        with patch.object(operator, "_setup_cluster"), patch.object(operator, "defer") as mock_defer:
            operator.execute(context)

            mock_defer.assert_called_once()
            args, kwargs = mock_defer.call_args
            assert isinstance(kwargs["trigger"], RayJobTrigger)
            assert kwargs["method_name"] == "execute_complete"
            assert kwargs["timeout"].total_seconds() == 600

    @patch("ray_provider.operators.SubmitRayJob._delete_cluster")
    @patch("ray_provider.operators.SubmitRayJob._setup_cluster")
    @patch("ray_provider.operators.SubmitRayJob.hook.submit_ray_job", return_value="test_job_id")
    @patch("ray_provider.operators.SubmitRayJob.hook")
    def test_execute_without_wait_no_cleanup(
        self, mock_hook, mock_submit, mock_setup_cluster, mock_delete_cluster, context
    ):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=False,
        )

        result = operator.execute(context)
        assert result == "test_job_id"

        mock_setup_cluster.assert_called_once_with(context=context)
        mock_submit.assert_called_once_with(
            dashboard_url=None,
            entrypoint="python script.py",
            runtime_env={},
            entrypoint_num_cpus=0,
            entrypoint_num_gpus=0,
            entrypoint_memory=0,
            entrypoint_resources=None,
        )
        mock_delete_cluster.assert_not_called()

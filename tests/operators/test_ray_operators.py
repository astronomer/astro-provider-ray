from unittest.mock import Mock, mock_open, patch

import pytest
from airflow.exceptions import AirflowException
from kubernetes import client
from ray.job_submission import JobStatus

from ray_provider.operators.ray import DeleteRayCluster, SetupRayCluster, SubmitRayJob


class TestSetupRayCluster:

    @pytest.fixture
    def mock_hook(self):
        with patch("ray_provider.operators.ray.RayHook") as mock:
            yield mock.return_value

    @pytest.fixture
    def mock_file_ops(self):
        with patch("os.path.isfile", return_value=True), patch("builtins.open", mock_open(read_data="key: value")):
            yield

    @pytest.fixture
    def operator(self, mock_file_ops):
        return SetupRayCluster(task_id="test_setup_ray_cluster", conn_id="test_conn", ray_cluster_yaml="cluster.yaml")

    def test_init(self, mock_file_ops):
        operator = SetupRayCluster(
            task_id="test_setup_ray_cluster",
            conn_id="test_conn",
            ray_cluster_yaml="cluster.yaml",
            use_gpu=True,
            kuberay_version="1.1.0",
            gpu_device_plugin_yaml="custom_gpu_plugin.yaml",
        )

        assert operator.conn_id == "test_conn"
        assert operator.ray_cluster_yaml == "cluster.yaml"
        assert operator.use_gpu is True
        assert operator.kuberay_version == "1.1.0"
        assert operator.gpu_device_plugin_yaml == "custom_gpu_plugin.yaml"

    def test_validate_yaml_file_not_exist(self):
        with patch("os.path.isfile", return_value=False):
            with pytest.raises(AirflowException, match="The specified YAML file does not exist"):
                SetupRayCluster(
                    task_id="test_setup_ray_cluster", conn_id="test_conn", ray_cluster_yaml="nonexistent.yaml"
                )

    def test_validate_yaml_file_invalid_extension(self):
        with patch("os.path.isfile", return_value=True):
            with pytest.raises(AirflowException, match="The specified YAML file must have a .yaml or .yml extension"):
                SetupRayCluster(task_id="test_setup_ray_cluster", conn_id="test_conn", ray_cluster_yaml="invalid.txt")

    def test_validate_yaml_file_invalid_yaml(self):
        with patch("os.path.isfile", return_value=True), patch(
            "builtins.open", mock_open(read_data="invalid: yaml: content")
        ):
            with pytest.raises(AirflowException, match="The specified YAML file is not valid YAML"):
                SetupRayCluster(task_id="test_setup_ray_cluster", conn_id="test_conn", ray_cluster_yaml="invalid.yaml")

    def test_create_or_update_cluster_create(self, mock_hook, operator):
        mock_hook.get_custom_object.side_effect = client.exceptions.ApiException(status=404)

        operator.hook = mock_hook
        operator._create_or_update_cluster("test_group", "v1", "rayclusters", "test-cluster", "default", {})

        mock_hook.create_custom_object.assert_called_once()

    def test_create_or_update_cluster_update(self, mock_hook, operator):
        mock_hook.get_custom_object.return_value = {}

        operator.hook = mock_hook
        operator._create_or_update_cluster("test_group", "v1", "rayclusters", "test-cluster", "default", {})

        mock_hook.custom_object_client.patch_namespaced_custom_object.assert_called_once()

    def test_setup_gpu_driver(self, mock_hook, operator):
        mock_hook.load_yaml_content.return_value = {"metadata": {"name": "gpu-plugin"}}
        mock_hook.get_daemon_set.return_value = None

        operator.hook = mock_hook
        operator._setup_gpu_driver()

        mock_hook.create_daemon_set.assert_called_once_with("gpu-plugin", {"metadata": {"name": "gpu-plugin"}})

    def test_setup_load_balancer(self, mock_hook, operator):
        mock_hook.wait_for_load_balancer.return_value = {
            "ip_or_hostname": "test.example.com",
            "ports": [{"name": "dashboard", "port": 8265}],
        }
        context = {"task_instance": Mock()}

        operator.hook = mock_hook
        operator._setup_load_balancer("test-cluster", "default", context)

        context["task_instance"].xcom_push.assert_called_once_with(
            key="dashboard", value="http://test.example.com:8265"
        )

    @patch.object(SetupRayCluster, "_create_or_update_cluster")
    @patch.object(SetupRayCluster, "_setup_gpu_driver")
    @patch.object(SetupRayCluster, "_setup_load_balancer")
    def test_execute(self, mock_setup_lb, mock_setup_gpu, mock_create_update, mock_hook, operator):
        mock_hook.load_yaml_content.return_value = {
            "kind": "RayCluster",
            "apiVersion": "ray.io/v1",
            "metadata": {"name": "test-cluster"},
        }
        mock_hook.get_namespace.return_value = "default"

        operator.hook = mock_hook
        operator.use_gpu = True
        operator.execute(context={})

        mock_hook.install_kuberay_operator.assert_called_once()
        mock_create_update.assert_called_once()
        mock_setup_gpu.assert_called_once()
        mock_setup_lb.assert_called_once()

    @patch.object(SetupRayCluster, "_create_or_update_cluster")
    def test_execute_error(self, mock_create_update, mock_hook, operator):
        mock_create_update.side_effect = Exception("Test error")
        operator.hook = mock_hook

        with pytest.raises(AirflowException, match="Failed to set up Ray cluster: Test error"):
            operator.execute(context={})

    def test_hook_property(self, operator):
        with patch("ray_provider.operators.ray.RayHook") as mock_ray_hook:
            hook = operator.hook
            mock_ray_hook.assert_called_once_with(conn_id=operator.conn_id)
            assert hook == mock_ray_hook.return_value


class TestDeleteRayCluster:

    @pytest.fixture
    def mock_hook(self):
        with patch("ray_provider.operators.ray.RayHook") as mock:
            yield mock.return_value

    @pytest.fixture
    def mock_isfile(self):
        with patch("os.path.isfile") as mock:
            mock.return_value = True
            yield mock

    @pytest.fixture
    def operator(self, mock_isfile):
        return DeleteRayCluster(task_id="test_delete_ray_cluster", conn_id="test_conn", ray_cluster_yaml="cluster.yaml")

    def test_init(self, mock_isfile):
        operator = DeleteRayCluster(
            task_id="test_delete_ray_cluster",
            conn_id="test_conn",
            ray_cluster_yaml="cluster.yaml",
            use_gpu=True,
            gpu_device_plugin_yaml="custom_gpu_plugin.yaml",
        )

        assert operator.conn_id == "test_conn"
        assert operator.ray_cluster_yaml == "cluster.yaml"
        assert operator.use_gpu is True
        assert operator.gpu_device_plugin_yaml == "custom_gpu_plugin.yaml"

    def test_validate_yaml_file_not_exist(self):
        with patch("os.path.isfile", return_value=False):
            with pytest.raises(AirflowException, match="The specified YAML file does not exist"):
                DeleteRayCluster(
                    task_id="test_delete_ray_cluster", conn_id="test_conn", ray_cluster_yaml="nonexistent.yaml"
                )

    def test_validate_yaml_file_invalid_extension(self, mock_isfile):
        with pytest.raises(AirflowException, match="The specified YAML file must have a .yaml or .yml extension"):
            DeleteRayCluster(task_id="test_delete_ray_cluster", conn_id="test_conn", ray_cluster_yaml="invalid.txt")

    def test_delete_gpu_daemonset(self, mock_hook, operator):
        mock_hook.load_yaml_content.return_value = {"metadata": {"name": "gpu-plugin"}}
        mock_hook.get_daemon_set.return_value = True

        operator.use_gpu = True
        operator.hook = mock_hook
        operator._delete_gpu_daemonset()

        mock_hook.delete_daemon_set.assert_called_once_with("gpu-plugin")

    def test_delete_ray_cluster(self, mock_hook, operator):
        mock_hook.load_yaml_content.return_value = {
            "kind": "RayCluster",
            "apiVersion": "ray.io/v1",
            "metadata": {"name": "test-cluster"},
        }
        mock_hook.get_namespace.return_value = "default"
        mock_hook.get_custom_object.return_value = True

        operator.hook = mock_hook
        operator._delete_ray_cluster()

        mock_hook.delete_custom_object.assert_called_once_with(
            group="ray.io", version="v1", name="test-cluster", namespace="default", plural="rayclusters"
        )

    def test_delete_ray_cluster_not_found(self, mock_hook, operator):
        mock_hook.load_yaml_content.return_value = {
            "kind": "RayCluster",
            "apiVersion": "ray.io/v1",
            "metadata": {"name": "test-cluster"},
        }
        mock_hook.get_namespace.return_value = "default"
        mock_hook.get_custom_object.return_value = False

        operator.hook = mock_hook
        operator._delete_ray_cluster()

        mock_hook.delete_custom_object.assert_not_called()

    @patch.object(DeleteRayCluster, "_delete_gpu_daemonset")
    @patch.object(DeleteRayCluster, "_delete_ray_cluster")
    def test_execute(self, mock_delete_cluster, mock_delete_gpu, mock_hook, operator):
        operator.use_gpu = True
        operator.hook = mock_hook

        operator.execute(context={})

        mock_delete_gpu.assert_called_once()
        mock_delete_cluster.assert_called_once()
        mock_hook.uninstall_kuberay_operator.assert_called_once()

    @patch.object(DeleteRayCluster, "_delete_ray_cluster")
    def test_execute_error(self, mock_delete_cluster, mock_hook, operator):
        mock_delete_cluster.side_effect = Exception("Test error")
        operator.hook = mock_hook

        with pytest.raises(AirflowException, match="Failed to delete Ray cluster: Test error"):
            operator.execute(context={})

    def test_hook_property(self, operator):
        with patch("ray_provider.operators.ray.RayHook") as mock_ray_hook:
            hook = operator.hook
            mock_ray_hook.assert_called_once_with(conn_id=operator.conn_id)
            assert hook == mock_ray_hook.return_value


class TestSubmitRayJob:

    @pytest.fixture
    def mock_hook(self):
        with patch("ray_provider.operators.ray.RayHook") as mock:
            yield mock.return_value

    @pytest.fixture
    def task_instance(self):
        return Mock()

    @pytest.fixture
    def context(self, task_instance):
        return {"ti": task_instance}

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
        assert operator.job_timeout_seconds == 1200
        assert operator.poll_interval == 30
        assert operator.xcom_task_key == "task.key"

    def test_on_kill(self, mock_hook):
        operator = SubmitRayJob(task_id="test_task", conn_id="test_conn", entrypoint="python script.py", runtime_env={})
        operator.job_id = "test_job_id"
        operator.hook = mock_hook

        operator.on_kill()

        mock_hook.delete_ray_job.assert_called_once_with("test_job_id")

    def test_execute_without_xcom(self, mock_hook, context):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            wait_for_completion=False,
        )

        mock_hook.submit_ray_job.return_value = "test_job_id"

        result = operator.execute(context)

        assert result == "test_job_id"
        mock_hook.submit_ray_job.assert_called_once_with(
            entrypoint="python script.py",
            runtime_env={},
            entrypoint_num_cpus=0,
            entrypoint_num_gpus=0,
            entrypoint_memory=0,
            entrypoint_resources=None,
        )

    def test_execute_with_xcom(self, mock_hook, context, task_instance):
        operator = SubmitRayJob(
            task_id="test_task",
            conn_id="test_conn",
            entrypoint="python script.py",
            runtime_env={},
            xcom_task_key="task.key",
            wait_for_completion=False,
        )

        task_instance.xcom_pull.return_value = "http://dashboard.url"
        mock_hook.submit_ray_job.return_value = "test_job_id"

        result = operator.execute(context)

        assert result == "test_job_id"
        assert operator.dashboard_url == "http://dashboard.url"
        task_instance.xcom_pull.assert_called_once_with(task_ids="task", key="key")

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
    def test_execute_complete(self, event_status, expected_action):
        operator = SubmitRayJob(task_id="test_task", conn_id="test_conn", entrypoint="python script.py", runtime_env={})
        operator.job_id = "test_job_id"

        event = {"status": event_status, "message": "Test message"}

        if expected_action == "raise":
            with pytest.raises(AirflowException):
                operator.execute_complete({}, event)
        else:
            operator.execute_complete({}, event)

    def test_template_fields(self):
        assert SubmitRayJob.template_fields == (
            "conn_id",
            "entrypoint",
            "runtime_env",
            "num_cpus",
            "num_gpus",
            "memory",
            "xcom_task_key",
        )

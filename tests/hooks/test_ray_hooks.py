from unittest.mock import MagicMock, mock_open, patch

from ray.job_submission import JobStatus

from ray_provider.hooks.ray import RayHook


class TestRayHook:

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    def test_init(self, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        hook = RayHook(conn_id="test_conn", xcom_dashboard_url="http://test-url")

        # Assert that the parent class's __init__ was called with the correct arguments
        mock_kubernetes_init.assert_called_once_with(conn_id="test_conn")

        # Assert that the RayHook attributes are set correctly
        assert hook.conn_id == "test_conn"
        assert hook.xcom_dashboard_url == "http://test-url"

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.JobSubmissionClient")
    def test_ray_client(self, mock_job_client, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_job_client.return_value = MagicMock()
        hook = RayHook(conn_id="test_conn")
        client = hook.ray_client
        assert client == mock_job_client.return_value

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.JobSubmissionClient")
    def test_submit_ray_job(self, mock_job_client, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_client_instance = mock_job_client.return_value
        mock_client_instance.submit_job.return_value = "test_job_id"
        hook = RayHook(conn_id="test_conn")
        job_id = hook.submit_ray_job(entrypoint="test_entry")
        assert job_id == "test_job_id"

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.JobSubmissionClient")
    def test_delete_ray_job(self, mock_job_client, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_client_instance = mock_job_client.return_value
        mock_client_instance.delete_job.return_value = "deleted"
        hook = RayHook(conn_id="test_conn")
        result = hook.delete_ray_job("test_job_id")
        assert result == "deleted"

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.JobSubmissionClient")
    def test_get_ray_job_status(self, mock_job_client, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_client_instance = mock_job_client.return_value
        mock_client_instance.get_job_status.return_value = JobStatus.SUCCEEDED
        hook = RayHook(conn_id="test_conn")
        status = hook.get_ray_job_status("test_job_id")
        assert status == JobStatus.SUCCEEDED

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.JobSubmissionClient")
    def test_get_ray_job_logs(self, mock_job_client, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_client_instance = mock_job_client.return_value
        mock_client_instance.get_job_logs.return_value = "logs"
        hook = RayHook(conn_id="test_conn")
        logs = hook.get_ray_job_logs("test_job_id")
        assert logs == "logs"

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.requests.get")
    @patch("builtins.open", new_callable=mock_open, read_data="key: value\n")
    def test_load_yaml_content(self, mock_open, mock_requests, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        hook = RayHook(conn_id="test_conn")
        result = hook.load_yaml_content("test_path")
        assert result == {"key": "value"}

        mock_requests.return_value.status_code = 200
        mock_requests.return_value.text = "key: value\n"
        result = hook.load_yaml_content("http://test-url")
        assert result == {"key": "value"}

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.socket.socket")
    def test_is_port_open(self, mock_socket, mock_get_connection):
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_socket_instance = mock_socket.return_value

        # Test successful connection
        mock_socket_instance.connect.return_value = None
        hook = RayHook(conn_id="test_conn")
        result = hook._is_port_open("localhost", 8080)
        assert result is True

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.subprocess.run")
    def test_install_kuberay_operator(self, mock_subprocess_run, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_subprocess_run.return_value = MagicMock(stdout="install output", stderr="")

        hook = RayHook(conn_id="test_conn")
        stdout, stderr = hook.install_kuberay_operator(version="1.0.0")

        assert "install output" in stdout
        assert stderr == ""

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.subprocess.run")
    def test_uninstall_kuberay_operator(self, mock_subprocess_run, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_subprocess_run.return_value = MagicMock(stdout="uninstall output", stderr="")

        hook = RayHook(conn_id="test_conn")
        stdout, stderr = hook.uninstall_kuberay_operator()

        assert "uninstall output" in stdout
        assert stderr == ""

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.client.AppsV1Api.read_namespaced_daemon_set")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_get_daemon_set(
        self, mock_load_kube_config, mock_read_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        # Configure the mock to return the expected values
        mock_metadata = MagicMock()
        mock_metadata.name = "test-daemonset"
        mock_read_daemon_set.return_value = MagicMock(metadata=mock_metadata)

        hook = RayHook(conn_id="test_conn")
        daemon_set = hook.get_daemon_set(name="test-daemonset")

        assert daemon_set.metadata.name == "test-daemonset"

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.client.AppsV1Api.create_namespaced_daemon_set")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_create_daemon_set(
        self, mock_load_kube_config, mock_create_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        # Configure the mock to return the expected values
        mock_metadata = MagicMock()
        mock_metadata.name = "test-daemonset"
        mock_create_daemon_set.return_value = MagicMock(metadata=mock_metadata)

        hook = RayHook(conn_id="test_conn")
        body = {"metadata": {"name": "test-daemonset"}}
        daemon_set = hook.create_daemon_set(name="test-daemonset", body=body)

        assert daemon_set.metadata.name == "test-daemonset"

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.client.AppsV1Api.delete_namespaced_daemon_set")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_delete_daemon_set(
        self, mock_load_kube_config, mock_delete_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_delete_daemon_set.return_value = MagicMock(status="Success")

        hook = RayHook(conn_id="test_conn")
        response = hook.delete_daemon_set(name="test-daemonset")

        assert response.status == "Success"

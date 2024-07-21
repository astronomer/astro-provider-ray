from unittest.mock import MagicMock, mock_open, patch

import pytest
from airflow.exceptions import AirflowException
from kubernetes.client.exceptions import ApiException
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

    def test_get_ui_field_behaviour(self):
        expected_fields = {
            "hidden_fields": ["host", "schema", "login", "password", "port", "extra"],
            "relabeling": {},
        }
        assert RayHook.get_ui_field_behaviour() == expected_fields

    def test_get_connection_form_widgets(self):
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, PasswordField, StringField

        expected_widgets = {
            "address": StringField(lazy_gettext("Ray address path"), widget=BS3TextFieldWidget()),
            "create_cluster_if_needed": BooleanField(lazy_gettext("Create cluster if needed")),
            "cookies": StringField(lazy_gettext("Ray job cookies"), widget=BS3TextFieldWidget()),
            "metadata": StringField(lazy_gettext("Ray job metadata"), widget=BS3TextFieldWidget()),
            "headers": StringField(lazy_gettext("Ray job headers"), widget=BS3TextFieldWidget()),
            "verify": BooleanField(lazy_gettext("Ray job verify")),
            "kube_config_path": StringField(lazy_gettext("Kube config path"), widget=BS3TextFieldWidget()),
            "kube_config": PasswordField(lazy_gettext("Kube config (JSON format)"), widget=BS3PasswordFieldWidget()),
            "namespace": StringField(lazy_gettext("Namespace"), widget=BS3TextFieldWidget()),
            "cluster_context": StringField(lazy_gettext("Cluster context"), widget=BS3TextFieldWidget()),
            "disable_verify_ssl": BooleanField(lazy_gettext("Disable SSL")),
            "disable_tcp_keepalive": BooleanField(lazy_gettext("Disable TCP keepalive")),
            "xcom_sidecar_container_image": StringField(
                lazy_gettext("XCom sidecar image"), widget=BS3TextFieldWidget()
            ),
            "xcom_sidecar_container_resources": StringField(
                lazy_gettext("XCom sidecar resources (JSON format)"), widget=BS3TextFieldWidget()
            ),
        }

        widgets = RayHook.get_connection_form_widgets()

        assert len(widgets) == len(expected_widgets)
        for key in expected_widgets:
            assert key in widgets
            assert type(widgets[key]) == type(expected_widgets[key])

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_setup_kubeconfig_path(self, mock_load_kube_config, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        hook = RayHook(conn_id="test_conn")
        hook._setup_kubeconfig("/tmp/fake_kubeconfig", None, "test_context")

        assert hook.kubeconfig == "/tmp/fake_kubeconfig"
        mock_load_kube_config.assert_called_once_with(config_file="/tmp/fake_kubeconfig", context="test_context")

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    @patch("tempfile.NamedTemporaryFile")
    def test_setup_kubeconfig_content(
        self, mock_tempfile, mock_load_kube_config, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        mock_tempfile.return_value.__enter__.return_value.name = "/tmp/fake_kubeconfig"
        mock_tempfile.return_value.__enter__.return_value.write = MagicMock()

        hook = RayHook(conn_id="test_conn")
        kubeconfig_content = "apiVersion: v1\nclusters:\n- cluster:\n    server: https://127.0.0.1:6443"

        hook._setup_kubeconfig(None, kubeconfig_content, "test_context")

        mock_tempfile.return_value.__enter__.return_value.write.assert_called_once_with(kubeconfig_content.encode())
        mock_load_kube_config.assert_called_once_with(config_file="/tmp/fake_kubeconfig", context="test_context")

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    def test_setup_kubeconfig_invalid_config(self, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        hook = RayHook(conn_id="test_conn")
        with pytest.raises(AirflowException) as exc_info:
            hook._setup_kubeconfig("/tmp/fake_kubeconfig", "kubeconfig_content", "test_context")

        assert str(exc_info.value) == (
            "Invalid connection configuration. Options kube_config_path and "
            "kube_config are mutually exclusive. You can only use one option at a time."
        )

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
    @patch("ray_provider.hooks.ray.client.AppsV1Api.read_namespaced_daemon_set")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_get_daemon_set_not_found(
        self, mock_load_kube_config, mock_read_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_read_daemon_set.side_effect = ApiException(status=404, reason="Not Found")

        hook = RayHook(conn_id="test_conn")
        daemon_set = hook.get_daemon_set(name="test-daemonset")

        assert daemon_set is None

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
    @patch("ray_provider.hooks.ray.client.AppsV1Api.create_namespaced_daemon_set")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_create_daemon_set_no_body(
        self, mock_load_kube_config, mock_create_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        hook = RayHook(conn_id="test_conn")
        daemon_set = hook.create_daemon_set(name="test-daemonset", body=None)

        assert daemon_set is None

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.client.AppsV1Api.create_namespaced_daemon_set")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_create_daemon_set_exception(
        self, mock_load_kube_config, mock_create_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_create_daemon_set.side_effect = ApiException(status=500, reason="Internal Server Error")

        hook = RayHook(conn_id="test_conn")
        body = {"metadata": {"name": "test-daemonset"}}
        daemon_set = hook.create_daemon_set(name="test-daemonset", body=body)

        assert daemon_set is None

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

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.client.AppsV1Api.delete_namespaced_daemon_set")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_delete_daemon_set_not_found(
        self, mock_load_kube_config, mock_delete_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_delete_daemon_set.side_effect = ApiException(status=404, reason="Not Found")

        hook = RayHook(conn_id="test_conn")
        response = hook.delete_daemon_set(name="test-daemonset")

        assert response is None

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    @patch("ray_provider.hooks.ray.client.AppsV1Api.delete_namespaced_daemon_set")
    @patch("ray_provider.hooks.ray.config.load_kube_config")
    def test_delete_daemon_set_exception(
        self, mock_load_kube_config, mock_delete_daemon_set, mock_kubernetes_init, mock_get_connection
    ):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})
        mock_delete_daemon_set.side_effect = ApiException(status=500, reason="Internal Server Error")

        hook = RayHook(conn_id="test_conn")
        response = hook.delete_daemon_set(name="test-daemonset")

        assert response is None

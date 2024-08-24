import subprocess
from unittest.mock import MagicMock, Mock, mock_open, patch

import pytest
from airflow.exceptions import AirflowException
from kubernetes import client
from kubernetes.client.exceptions import ApiException
from ray.job_submission import JobStatus

from ray_provider.hooks.ray import RayHook


class TestRayHook:

    @pytest.fixture
    def ray_hook(self):
        with patch("ray_provider.hooks.ray.KubernetesHook.get_connection") as mock_get_connection:
            mock_connection = Mock()
            mock_connection.extra_dejson = {"kube_config_path": None, "kube_config": None, "cluster_context": None}
            mock_get_connection.return_value = mock_connection

            with patch("ray_provider.hooks.ray.KubernetesHook.__init__", return_value=None):
                hook = RayHook(conn_id="test_conn")
                # Manually set the necessary attributes
                hook.namespace = "default"
                hook.kubeconfig = "/path/to/kubeconfig"
                return hook

    @patch("ray_provider.hooks.ray.KubernetesHook.get_connection")
    @patch("ray_provider.hooks.ray.KubernetesHook.__init__")
    def test_init(self, mock_kubernetes_init, mock_get_connection):
        mock_kubernetes_init.return_value = None
        mock_get_connection.return_value = MagicMock(conn_id="test_conn", extra_dejson={})

        hook = RayHook(conn_id="test_conn")

        # Assert that the parent class's __init__ was called with the correct arguments
        mock_kubernetes_init.assert_called_once_with(conn_id="test_conn")

        # Assert that the RayHook attributes are set correctly
        assert hook.conn_id == "test_conn"

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
            "address": StringField(lazy_gettext("Ray dashboard url"), widget=BS3TextFieldWidget()),
            "create_cluster_if_needed": BooleanField(lazy_gettext("Create cluster if needed")),
            "cookies": StringField(lazy_gettext("Cookies"), widget=BS3TextFieldWidget()),
            "metadata": StringField(lazy_gettext("Metadata"), widget=BS3TextFieldWidget()),
            "headers": StringField(lazy_gettext("Headers"), widget=BS3TextFieldWidget()),
            "verify": BooleanField(lazy_gettext("Verify")),
            "kube_config_path": StringField(lazy_gettext("Kube config path"), widget=BS3TextFieldWidget()),
            "kube_config": PasswordField(lazy_gettext("Kube config (JSON format)"), widget=BS3PasswordFieldWidget()),
            "namespace": StringField(lazy_gettext("Namespace"), widget=BS3TextFieldWidget()),
            "cluster_context": StringField(lazy_gettext("Cluster context"), widget=BS3TextFieldWidget()),
            "disable_verify_ssl": BooleanField(lazy_gettext("Disable SSL")),
            "disable_tcp_keepalive": BooleanField(lazy_gettext("Disable TCP keepalive")),
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

    @patch("ray_provider.hooks.ray.RayHook.core_v1_client")
    def test_get_service_success(self, mock_core_v1_client, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_core_v1_client.read_namespaced_service.return_value = mock_service

        service = ray_hook._get_service("test-service", "default")

        assert service == mock_service
        mock_core_v1_client.read_namespaced_service.assert_called_once_with("test-service", "default")

    @patch("ray_provider.hooks.ray.RayHook.core_v1_client")
    def test_get_service_not_found(self, mock_core_v1_client, ray_hook):
        mock_core_v1_client.read_namespaced_service.side_effect = client.exceptions.ApiException(status=404)

        with pytest.raises(AirflowException) as exc_info:
            ray_hook._get_service("non-existent-service", "default")

        assert "Service non-existent-service not found" in str(exc_info.value)

    def test_get_load_balancer_details_with_ingress(self, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_ingress = Mock(spec=client.V1LoadBalancerIngress)
        mock_ingress.ip = "192.168.1.1"
        mock_ingress.hostname = None
        mock_service.status.load_balancer.ingress = [mock_ingress]

        mock_port = Mock()
        mock_port.name = "http"
        mock_port.port = 80
        mock_service.spec.ports = [mock_port]

        lb_details = ray_hook._get_load_balancer_details(mock_service)

        assert lb_details == {"ip": "192.168.1.1", "hostname": None, "ports": [{"name": "http", "port": 80}]}

    def test_get_load_balancer_details_with_hostname(self, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_ingress = Mock(spec=client.V1LoadBalancerIngress)
        mock_ingress.ip = None
        mock_ingress.hostname = "example.com"
        mock_service.status.load_balancer.ingress = [mock_ingress]

        mock_port = Mock()
        mock_port.name = "https"
        mock_port.port = 443
        mock_service.spec.ports = [mock_port]

        lb_details = ray_hook._get_load_balancer_details(mock_service)

        assert lb_details == {"hostname": "example.com", "ip": None, "ports": [{"name": "https", "port": 443}]}

    def test_get_load_balancer_details_no_ingress(self, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_service.status.load_balancer.ingress = None

        lb_details = ray_hook._get_load_balancer_details(mock_service)

        assert lb_details is None

    def test_get_load_balancer_details_no_ip_or_hostname(self, ray_hook):
        mock_service = Mock(spec=client.V1Service)
        mock_ingress = Mock(spec=client.V1LoadBalancerIngress)
        mock_ingress.ip = None
        mock_ingress.hostname = None
        mock_service.status.load_balancer.ingress = [mock_ingress]

        lb_details = ray_hook._get_load_balancer_details(mock_service)

        assert lb_details is None

    @patch("ray_provider.hooks.ray.RayHook.log")
    @patch("ray_provider.hooks.ray.subprocess.run")
    def test_run_bash_command_exception(self, mock_subprocess_run, mock_log, ray_hook):
        # Simulate a CalledProcessError
        mock_subprocess_run.side_effect = subprocess.CalledProcessError(
            returncode=1, cmd="test command", output="test output", stderr="test error"
        )

        # Call the method
        stdout, stderr = ray_hook._run_bash_command("test command")

        # Assert that the method returned None for both stdout and stderr
        assert stdout is None
        assert stderr is None

        # Assert that the error was logged
        mock_log.error.assert_any_call(
            "An error occurred while executing the command: %s", mock_subprocess_run.side_effect
        )
        mock_log.error.assert_any_call("Return code: %s", 1)
        mock_log.error.assert_any_call("Standard Output: %s", "test output")
        mock_log.error.assert_any_call("Standard Error: %s", "test error")

        # Verify that subprocess.run was called with the correct arguments
        mock_subprocess_run.assert_called_once_with(
            "test command",
            shell=True,
            check=True,
            text=True,
            capture_output=True,
            env=ray_hook._run_bash_command.__globals__["os"].environ.copy(),
        )

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

    @patch("ray_provider.hooks.ray.RayHook._get_service")
    @patch("ray_provider.hooks.ray.RayHook._get_load_balancer_details")
    @patch("ray_provider.hooks.ray.RayHook._check_load_balancer_readiness")
    def test_wait_for_load_balancer_success(
        self, mock_check_readiness, mock_get_lb_details, mock_get_service, ray_hook
    ):
        mock_service = Mock(spec=client.V1Service)
        mock_get_service.return_value = mock_service

        mock_get_lb_details.return_value = {
            "hostname": "test-lb.example.com",
            "ip": None,
            "ports": [{"name": "http", "port": 80}, {"name": "https", "port": 443}],
        }

        mock_check_readiness.return_value = "test-lb.example.com"

        result = ray_hook.wait_for_load_balancer("test-service", namespace="default", max_retries=1, retry_interval=1)

        assert result == {
            "hostname": "test-lb.example.com",
            "ip": None,
            "ports": [{"name": "http", "port": 80}, {"name": "https", "port": 443}],
            "working_address": "test-lb.example.com",
        }
        mock_get_service.assert_called_once_with("test-service", "default")
        mock_get_lb_details.assert_called_once_with(mock_service)
        mock_check_readiness.assert_called_once()

    @patch("ray_provider.hooks.ray.RayHook._get_service")
    @patch("ray_provider.hooks.ray.RayHook._get_load_balancer_details")
    @patch("ray_provider.hooks.ray.RayHook._is_port_open")
    def test_wait_for_load_balancer_timeout(self, mock_is_port_open, mock_get_lb_details, mock_get_service, ray_hook):
        # Mock the service
        mock_service = Mock(spec=client.V1Service)
        mock_get_service.return_value = mock_service

        # Mock the load balancer details
        mock_get_lb_details.return_value = {
            "hostname": "test-lb.example.com",
            "ip": None,
            "ports": [{"name": "http", "port": 80}],
        }

        # Mock the port check to return False (port is not open)
        mock_is_port_open.return_value = False

        # Call the method and expect an AirflowException
        with pytest.raises(AirflowException) as exc_info:
            ray_hook.wait_for_load_balancer("test-service", namespace="default", max_retries=2, retry_interval=1)

        assert "LoadBalancer did not become ready after 2 attempts" in str(exc_info.value)

    @patch("ray_provider.hooks.ray.RayHook._get_service")
    def test_wait_for_load_balancer_service_not_found(self, mock_get_service, ray_hook):
        # Mock the service to raise an AirflowException (service not found)
        mock_get_service.side_effect = AirflowException("Service test-service not found")

        # Call the method and expect an AirflowException
        with pytest.raises(AirflowException) as exc_info:
            ray_hook.wait_for_load_balancer("test-service", namespace="default", max_retries=1, retry_interval=1)

        assert "LoadBalancer did not become ready after 1 attempts" in str(exc_info.value)

    @patch("ray_provider.hooks.ray.RayHook._is_port_open")
    def test_check_load_balancer_readiness_ip(self, mock_is_port_open, ray_hook):
        mock_is_port_open.return_value = True
        lb_details = {"ip": "192.168.1.1", "hostname": None, "ports": [{"name": "http", "port": 80}]}

        result = ray_hook._check_load_balancer_readiness(lb_details)

        assert result == "192.168.1.1"
        mock_is_port_open.assert_called_once_with("192.168.1.1", 80)

    @patch("ray_provider.hooks.ray.RayHook._is_port_open")
    def test_check_load_balancer_readiness_hostname(self, mock_is_port_open, ray_hook):
        mock_is_port_open.side_effect = [False, True]
        lb_details = {
            "ip": "192.168.1.1",
            "hostname": "example.com",
            "ports": [{"name": "http", "port": 80}, {"name": "https", "port": 443}],
        }

        result = ray_hook._check_load_balancer_readiness(lb_details)

        assert result == "example.com"
        mock_is_port_open.assert_any_call("192.168.1.1", 80)
        mock_is_port_open.assert_any_call("example.com", 80)

    @patch("ray_provider.hooks.ray.RayHook._is_port_open")
    def test_check_load_balancer_readiness_not_ready(self, mock_is_port_open, ray_hook):
        mock_is_port_open.return_value = False
        lb_details = {"ip": "192.168.1.1", "hostname": "example.com", "ports": [{"name": "http", "port": 80}]}

        result = ray_hook._check_load_balancer_readiness(lb_details)

        assert result is None
        mock_is_port_open.assert_any_call("192.168.1.1", 80)
        mock_is_port_open.assert_any_call("example.com", 80)

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

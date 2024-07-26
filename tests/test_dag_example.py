import os
from pathlib import Path

import pytest
from airflow.models import Connection, DagBag
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session

# Correctly construct the example DAGs directory path
EXAMPLE_DAGS_DIR = Path(__file__).parent.parent / "example_dags"
print(f"EXAMPLE_DAGS_DIR: {EXAMPLE_DAGS_DIR}")


def get_dags(dag_folder=None):
    dag_bag = (
        DagBag(dag_folder=str(dag_folder), include_examples=False) if dag_folder else DagBag(include_examples=False)
    )

    def strip_path_prefix(path):
        return os.path.relpath(path, os.environ.get("AIRFLOW_HOME", ""))

    dags_info = [(k, v, strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]
    for dag_id, dag, fileloc in dags_info:
        print(f"DAG ID: {dag_id}, File Location: {fileloc}")
    return dags_info


@pytest.fixture(scope="module")
def setup_airflow_db():
    os.system("airflow db init")
    conn_id = "ray_conn"
    # Explicitly create the tables if necessary
    create_default_connections()

    kubeconfig_path = os.environ.get("KUBECONFIG")
    if not kubeconfig_path:
        raise ValueError("KUBECONFIG environment variable is not set.")

    print(f"KUBECONFIG is set to: {kubeconfig_path}")
    if not os.path.exists(kubeconfig_path):
        raise FileNotFoundError(f"KUBECONFIG file not found at {kubeconfig_path}")

    with create_session() as session:
        conn_exists = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if conn_exists:
            session.delete(conn_exists)
            session.commit()
        conn = Connection(
            conn_id=conn_id,
            conn_type="ray",
            extra={
                "kube_config_path": kubeconfig_path,
                "namespace": "ray",
                "cluster_context": None,  # Set to None as we don't know how to get cluster context for a kind cluster
            },
        )
        session.add(conn)
        session.commit()

    dags = get_dags(EXAMPLE_DAGS_DIR)
    print(f"Discovered DAGs: {dags}")
    return dags


@pytest.mark.integration
@pytest.mark.parametrize("dag_id,dag,fileloc", setup_airflow_db(), ids=lambda x: x[2])
def test_dag_runs(dag_id, dag, fileloc):
    print(f"Testing DAG: {dag_id}, located at: {fileloc}")
    assert dag is not None, f"DAG {dag_id} not found!"

    try:
        dag.test()
    except Exception as e:
        pytest.fail(f"Error running DAG {dag_id}: {e}")

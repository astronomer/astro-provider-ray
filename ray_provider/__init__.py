from __future__ import annotations

import warnings

__version__ = "0.4.0"

from typing import Any

warnings.warn(
    "astro-provider-ray is no longer actively maintained by Astronomer. Development has been "
    "paused and we are not accepting new contributions, bug fixes or releases. Google Cloud users "
    "can use the Ray operators available in the official Apache Airflow Google provider "
    "(apache-airflow-providers-google). If you're interested in adopting or taking over this "
    "project, reach us at oss@astronomer.io.",
    DeprecationWarning,
    stacklevel=2,
)


# This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "astro-provider-ray",  # Required
        "name": "Ray",  # Required
        "description": "An integration between airflow and ray",  # Required
        "connection-types": [{"connection-type": "ray", "hook-class-name": "ray_provider.hooks.RayHook"}],
        "versions": [__version__],  # Required
    }

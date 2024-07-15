__version__ = "1.0.0"
from typing import Any


## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info() -> dict[str, Any]:
    return {
        "package-name": "astro-provider-ray",  # Required
        "name": "Ray",  # Required
        "description": "An integration between airflow and ray",  # Required
        "connection-types": [{"connection-type": "ray", "hook-class-name": "ray_provider.hooks.ray.RayHook"}],
        "versions": [__version__],  # Required
    }

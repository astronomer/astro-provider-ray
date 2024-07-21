import pytest

from ray_provider import __version__, get_provider_info


def test_get_provider_info():
    expected_info = {
        "package-name": "astro-provider-ray",
        "name": "Ray",
        "description": "An integration between airflow and ray",
        "connection-types": [{"connection-type": "ray", "hook-class-name": "ray_provider.hooks.ray.RayHook"}],
        "versions": [__version__],
    }

    result = get_provider_info()

    assert result == expected_info
    assert isinstance(result, dict)
    assert "package-name" in result
    assert "name" in result
    assert "description" in result
    assert "connection-types" in result
    assert "versions" in result
    assert isinstance(result["versions"], list)
    assert result["versions"][0] == __version__


if __name__ == "__main__":
    pytest.main()

__version__ = "1.0.0"


## This is needed to allow Airflow to pick up specific metadata fields it needs for certain features.
def get_provider_info():
    return {
        "package-name": "astro-provider-ray",  # Required
        "name": "Ray",  # Required
        "description": "An integration between airflow and ray",  # Required
        "versions": [__version__],  # Required
    }

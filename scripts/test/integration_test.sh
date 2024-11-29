pytest -vv \
    --cov=ray_provider \
    --cov-report=term-missing \
    --cov-report=xml \
    -m integration \
    -s \
    --log-cli-level=DEBUG

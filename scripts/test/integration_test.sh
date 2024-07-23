pytest -vv \
    --cov=ray_provider \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    --strict-markers \
    -m integration \
    -s \
    --log-cli-level=DEBUG

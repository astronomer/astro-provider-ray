pytest -vv \
    --cov=ray_provider \
    --cov-report=term-missing \
    --cov-report=xml \
    --durations=0 \
    -m integration \
    -s \
    --log-cli-level=DEBUG

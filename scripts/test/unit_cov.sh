pytest \
    -vv \
    --cov=ray_provider \
    --cov-report=term-missing \
    --cov-report=xml \
    -m "not (integration or perf)" \
    --ignore=tests/test_dag_example.py

FROM quay.io/astronomer/astro-runtime:12.2.0

USER root

RUN curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh

USER astro

RUN pip install /usr/local/airflow/include/*.whl

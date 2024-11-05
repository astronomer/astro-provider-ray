FROM quay.io/astronomer/astro-runtime:12.2.0

ENV AIRFLOW__CORE__TEST_CONNECTION=Enabled
ENV GOOGLE_APPLICATION_CREDENTIALS=</path/to/your/service-account-key.json>

USER root

RUN apt-get update -y
RUN apt-get install -y --no-install-recommends && \
        apt-get install -y curl gnupg && \
        echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
        curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
        apt-get update -y && \
        apt-get install python3 google-cloud-sdk -y && \
        apt-get install google-cloud-sdk-gke-gcloud-auth-plugin -y

RUN curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 && \
    chmod 700 get_helm.sh && \
    ./get_helm.sh

USER astro

RUN pip install /usr/local/airflow/include/*.whl

FROM apache/airflow:2.6.1-python3.10

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    vim \
    apt-transport-https \
    ca-certificates \ 
    gnupg \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* 

#Install GCloud[GCP] packages
# RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list \
#     && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.gpg \
#     && apt-get update -y \
#     && apt-get install google-cloud-sdk google-cloud-sdk-gke-gcloud-auth-plugin kubectl -y
RUN curl -L -o "google-cloud-sdk.tar.gz" "https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-cli-434.0.0-darwin-arm.tar.gz?hl=pt-br" \ 
    && tar -xzf "google-cloud-sdk.tar.gz" && ./google-cloud-sdk/install.sh --quiet 
#&& ./google-cloud-sdk/bin/gcloud init --skip-diagnostics

USER airflow

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt 

RUN pip install --upgrade google-api-python-client && pip install google-cloud-storage

ENV PATH=$PATH::/usr/local/gcloud/google-cloud-sdk/bin/

COPY gcp_account.json .

RUN ./google-cloud-sdk/bin/gcloud auth activate-service-account --key-file=gcp_account.json
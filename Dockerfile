FROM apache/airflow:2.6.3-python3.9

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
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && apt-get update -y && apt-get install google-cloud-sdk google-cloud-sdk-gke-gcloud-auth-plugin kubectl -y

USER airflow

COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install -r requirements.txt 

RUN pip install --upgrade google-api-python-client && pip install google-cloud-storage

ENV PATH=$PATH::/usr/local/gcloud/google-cloud-sdk/bin/

COPY gcp_account.json .

RUN gcloud auth activate-service-account --key-file=gcp_account.json
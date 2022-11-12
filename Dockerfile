FROM apache/airflow:latest-python3.10

USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip \ 
    && pip install -r requirements.txt

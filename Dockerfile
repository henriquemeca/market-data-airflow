FROM apache/airflow:2.4.1

USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip \ 
    && pip install -r requirements.txt
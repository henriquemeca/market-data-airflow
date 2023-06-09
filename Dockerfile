FROM apache/airflow:2.6.1-python3.8

USER airflow
COPY requirements.txt .
RUN pip install --upgrade pip \ 
    && pip install -r requirements.txt

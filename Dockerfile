ARG AIRFLOW_VERSION=3.0.0

FROM apache/airflow:${AIRFLOW_VERSION}

USER airflow

COPY requirements-airflow.txt /requirements-airflow.txt

RUN pip install --no-cache-dir -r /requirements-airflow.txt
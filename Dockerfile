FROM apache/airflow:3.0.2

ENV AIRFLOW_HOME=/opt/airflow
ENV KAGGLEHUB_CACHE=/opt/airflow/raw

USER root

RUN apt-get update && apt-get install -y nano

USER airflow

ADD requirements.txt .
COPY ./dbt /opt/airflow/dbt
COPY ./raw /opt/airflow/raw
COPY ./database /opt/airflow/database

RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt
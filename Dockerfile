FROM apache/airflow:2.10.0-python3.8

USER root
RUN apt-get update && apt-get install -y iputils-ping && chown -R root:root /opt/airflow
USER airflow

COPY requirements.txt /requirements.txt

RUN pip install -r /requirements.txt
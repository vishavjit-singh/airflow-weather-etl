# Airflow image with the weather_etl package baked in (used by docker-compose.yml).
FROM apache/airflow:2.10.3-python3.11

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==2.10.3" -r /tmp/requirements.txt

COPY --chown=airflow:root config /opt/airflow/project/config
COPY --chown=airflow:root src /opt/airflow/project/src
COPY --chown=airflow:root dags /opt/airflow/dags
ENV PYTHONPATH=/opt/airflow/project/src \
    WEATHER_ETL_CONFIG=/opt/airflow/project/config/pipeline.yaml

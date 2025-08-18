FROM apache/airflow:2.9.3

USER airflow
COPY --chown=airflow:airflow requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

ENV AIRFLOW__CORE__ENABLE_XCOM_PICKLING=true \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=false
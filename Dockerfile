FROM apache/airflow:latest
USER airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/workspaces/airflow-template/dags
WORKDIR /workspaces/airflow-template
RUN pip install --no-cache-dir uv
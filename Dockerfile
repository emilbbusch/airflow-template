FROM apache/airflow:latest
ENV AIRFLOW__CORE__DAGS_FOLDER=/workspaces/airflow_template/dags
WORKDIR /workspaces/airflow_template
RUN pip install --no-cache-dir uv
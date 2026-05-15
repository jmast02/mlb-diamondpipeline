FROM apache/airflow:2.10.2-python3.11

ARG AIRFLOW_VERSION=2.10.2
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir \
    --constraint "${CONSTRAINT_URL}" \
    -r /tmp/airflow-requirements.txt

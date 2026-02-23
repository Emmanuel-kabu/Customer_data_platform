FROM apache/airflow:2.8.1-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libpq-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

# Copy DAGs, scripts, and plugins
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY plugins/ /opt/airflow/plugins/

WORKDIR /opt/airflow

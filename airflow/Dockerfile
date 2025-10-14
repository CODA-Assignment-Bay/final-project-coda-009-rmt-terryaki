# =================================================
# Airflow Dockerfile for ETL Project (no Spark)
# =================================================
FROM apache/airflow:2.10.2-python3.11

# Switch to root temporarily (optional but safe)
USER root

# Install system dependencies (minimal)
RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

WORKDIR /opt/airflow

# Copy and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DAGs, scripts, and data
COPY dags/ /opt/airflow/dags/
COPY scripts/ /opt/airflow/scripts/
COPY data/ /opt/airflow/data/

# Default command
CMD ["airflow", "standalone"]

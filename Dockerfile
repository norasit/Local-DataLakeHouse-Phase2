# Base: official Airflow image (already includes Airflow 2.10.4)
FROM apache/airflow:2.10.4

# Ensure consistent logs/timestamps
ENV TZ=UTC
USER airflow

# Install only what we need (pinned in requirements.txt)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

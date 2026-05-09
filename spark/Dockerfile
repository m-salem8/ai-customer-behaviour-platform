# =============================================================================
# Dockerfile for the Spark streaming job.
#
# Uses a lightweight Python-on-Spark base image (bitnami) and installs the
# necessary dependencies for Kafka ingestion and PostgreSQL JDBC writing.
# =============================================================================

FROM bitnami/spark:latest

USER root

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# JAR required for JDBC writes to PostgreSQL
RUN mkdir -p /opt/bitnami/spark/jars
COPY postgresql-42.7.5.jar /opt/bitnami/spark/jars/

# Copy application code
COPY *.py /app/
COPY checkpoints/ /app/checkpoints/

WORKDIR /app

# Default command: run the streaming job
CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "streaming_job.py"]
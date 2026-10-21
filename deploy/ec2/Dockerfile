# AutoPulse pipeline image: Python + Java + PySpark.
# Runs the vehicle simulator and the Spark Structured Streaming job on EC2.
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

WORKDIR /app

COPY deploy/ec2/requirements-pipeline.txt /app/requirements-pipeline.txt
RUN pip install --no-cache-dir -r /app/requirements-pipeline.txt

COPY src /app/src

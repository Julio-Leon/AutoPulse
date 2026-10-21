"""
AutoPulse | Vehicle telemetry Bronze -> Silver, every 15 minutes.

The EC2 host streams simulator events (Kafka -> Spark Structured Streaming)
into S3 Bronze continuously. This DAG runs the incremental Glue job that
turns the newly arrived Bronze files into Silver (and quarantine).

Snowpipe then loads the new Silver files into Snowflake, and the Snowflake
task merges them into FACT_VEHICLE_TELEMETRY.

Cost note: each run is a small Glue job (about $0.02-0.04). Change the
interval with AUTOPULSE_TELEMETRY_SCHEDULE (cron) on the EC2 host.
"""

import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

with DAG(
    dag_id="autopulse_vehicle_telemetry",
    description="Vehicle telemetry: incremental Glue Bronze-to-Silver",
    schedule=os.getenv("AUTOPULSE_TELEMETRY_SCHEDULE", "*/15 * * * *"),
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,                       # never overlap runs
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "owner": "autopulse",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["autopulse", "telemetry"],
) as dag:

    GlueJobOperator(
        task_id="telemetry_bronze_to_silver",
        job_name="autopulse-vehicle-telemetry-silver",
        wait_for_completion=True,
        verbose=False,
        region_name=REGION,
    )

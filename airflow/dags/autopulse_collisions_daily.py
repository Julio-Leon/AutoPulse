"""
AutoPulse | Daily NYC collisions pipeline.

    Lambda  autopulse-collisions-ingestion   NYC API -> S3 Bronze (latest 30 days of available data)
       |
       v
    Glue    autopulse-collisions-silver      Bronze -> Silver

Snowpipe then loads the new Silver files into Snowflake, and the Snowflake
task merges them into FACT_COLLISION within about 5 minutes.

Credentials: the EC2 host's IAM role (AutoPulseEc2HostRole).
"""

import json
import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
DAYS_BACK = int(os.getenv("AUTOPULSE_COLLISIONS_DAYS_BACK", "30"))

with DAG(
    dag_id="autopulse_collisions_daily",
    description="NYC collisions: Lambda ingestion -> Glue Bronze-to-Silver",
    schedule=os.getenv("AUTOPULSE_COLLISIONS_SCHEDULE", "0 6 * * *"),   # 06:00 New York time
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "autopulse",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["autopulse", "collisions"],
) as dag:

    ingest = LambdaInvokeFunctionOperator(
        task_id="ingest_collisions_to_bronze",
        function_name="autopulse-collisions-ingestion",
        payload=json.dumps({"days_back": DAYS_BACK}),
        invocation_type="RequestResponse",
        log_type="Tail",
        region_name=REGION,
        # The function may run for minutes; don't let the HTTP call time out first.
        botocore_config={"read_timeout": 900, "connect_timeout": 10, "retries": {"max_attempts": 0}},
    )

    to_silver = GlueJobOperator(
        task_id="collisions_bronze_to_silver",
        job_name="autopulse-collisions-silver",
        wait_for_completion=True,
        verbose=False,
        region_name=REGION,
    )

    ingest >> to_silver

"""
AutoPulse - vehicle telemetry pipeline, every 15 minutes, Monday-Friday 8:00-17:45 New York time.

    new_bronze_files_arrived     skip the rest of the run if the stream wrote nothing new
                                 since the last successful run (saves two Glue runs, e.g. after 17:00)
    telemetry_bronze_to_silver   Glue autopulse-telemetry-silver   (new Bronze files -> Silver + quarantine)
    telemetry_silver_to_gold     Glue autopulse-telemetry-gold     (-> fact_vehicle_telemetry, dim_vehicle)
    validate_data_lake           checks Bronze, Silver and Gold against each other

The EC2 streaming host fills Bronze every 5 minutes by itself; Snowpipe loads
the new Gold files into Snowflake by itself.
"""

import boto3
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from airflow.sdk import DAG

from autopulse_common import AWS_REGION, BUCKET, DEFAULT_ARGS, START_DATE, glue_job, validate_data_lake


def new_bronze_files_arrived(**context):
    """True if a Bronze telemetry file was written after the last successful run started."""
    since = context.get("prev_start_date_success")
    if since is None:
        return True                                    # first run ever
    s3 = boto3.client("s3", region_name=AWS_REGION)
    newest = None
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="bronze/vehicle_telemetry/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet") and (newest is None or obj["LastModified"] > newest):
                newest = obj["LastModified"]
    arrived = newest is not None and newest > since
    print(f"Newest Bronze file: {newest}; last successful run started {since}; new files: {arrived}")
    return arrived


with DAG(
    dag_id="autopulse_telemetry_every_15_minutes",
    description="Vehicle telemetry: Bronze -> Silver -> Gold, then validate",
    schedule="*/10 8-17 * * 1-5",          # runs at 8:00, 8:15 ... 17:45, Mon-Fri (New York time)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,                     # never two runs at once
    default_args=DEFAULT_ARGS,
    tags=["autopulse", "telemetry", "near-real-time"],
) as dag:
    check_new_files = ShortCircuitOperator(
        task_id="new_bronze_files_arrived",
        python_callable=new_bronze_files_arrived,
        retries=0,
    )
    silver = glue_job("telemetry_bronze_to_silver", "autopulse-telemetry-silver")
    gold = glue_job("telemetry_silver_to_gold", "autopulse-telemetry-gold")
    validate = PythonOperator(
        task_id="validate_data_lake",
        python_callable=validate_data_lake,
        op_kwargs={"pipeline": "telemetry"},
        retries=0,
    )

    check_new_files >> silver >> gold >> validate

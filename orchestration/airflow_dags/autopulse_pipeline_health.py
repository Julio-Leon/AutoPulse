"""
AutoPulse - pipeline health checks, every 15 minutes, Monday-Friday 8:00-18:45 New York time.

Each check is its own task, so the Airflow grid shows which part is unhealthy:

    streaming_host_is_producing      a Bronze telemetry file arrived in the last 15 minutes
                                     (else the EC2 streaming host has stopped)
    snowpipe_has_no_load_errors      no file failed to load into Snowflake in the last 24 hours
    dynamic_tables_refreshing        no CORE dynamic table refresh failed in the last 2 hours
    telemetry_is_fresh_in_snowflake  the newest vehicle event in Snowflake is under 60 minutes old
    data_quality                     no duplicate keys, no facts pointing at missing dimension
                                     rows, no impossible values (see DATA_QUALITY_CHECKS)

Checks that only make sense while the fleet drives are SKIPPED outside those hours.
(The Glue runs themselves are watched by the two pipeline DAGs.)

Snowflake is queried as AUTOPULSE_AIRFLOW with the read-only role AUTOPULSE_MONITOR
(snowflake/10_airflow_monitoring_user.sql). Checks run only on weekdays during
working hours, so they wake the Snowflake warehouse only then.
"""

from datetime import datetime, timezone

import boto3
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

from autopulse_common import AWS_REGION, BUCKET, NEW_YORK, START_DATE, snowflake_records

LANDING_TABLES = ["DIM_DATE", "DIM_LOCATION", "DIM_VEHICLE", "FACT_COLLISION", "FACT_VEHICLE_TELEMETRY"]

# Each query returns ONE number: how many problem rows. Anything above 0 fails the check.
DATA_QUALITY_CHECKS = {
    "telemetry: duplicate event_id": """
        SELECT COUNT(*) FROM (SELECT EVENT_ID FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY
                              GROUP BY EVENT_ID HAVING COUNT(*) > 1)""",
    "collisions: duplicate collision_id": """
        SELECT COUNT(*) FROM (SELECT COLLISION_ID FROM AUTOPULSE.CORE.FACT_COLLISION
                              GROUP BY COLLISION_ID HAVING COUNT(*) > 1)""",
    "telemetry: date_key missing in DIM_DATE": """
        SELECT COUNT(*) FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY t
        LEFT JOIN AUTOPULSE.CORE.DIM_DATE d ON d.DATE_KEY = t.DATE_KEY WHERE d.DATE_KEY IS NULL""",
    "telemetry: cell_id missing in DIM_LOCATION": """
        SELECT COUNT(*) FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY t
        LEFT JOIN AUTOPULSE.CORE.DIM_LOCATION l ON l.CELL_ID = t.CELL_ID
        WHERE t.CELL_ID IS NOT NULL AND l.CELL_ID IS NULL""",
    "telemetry: vehicle missing in DIM_VEHICLE": """
        SELECT COUNT(DISTINCT t.VEHICLE_ID) FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY t
        LEFT JOIN AUTOPULSE.CORE.DIM_VEHICLE v ON v.VEHICLE_ID = t.VEHICLE_ID WHERE v.VEHICLE_ID IS NULL""",
    "collisions: date_key missing in DIM_DATE": """
        SELECT COUNT(*) FROM AUTOPULSE.CORE.FACT_COLLISION c
        LEFT JOIN AUTOPULSE.CORE.DIM_DATE d ON d.DATE_KEY = c.DATE_KEY WHERE d.DATE_KEY IS NULL""",
    "collisions: cell_id missing in DIM_LOCATION": """
        SELECT COUNT(*) FROM AUTOPULSE.CORE.FACT_COLLISION c
        LEFT JOIN AUTOPULSE.CORE.DIM_LOCATION l ON l.CELL_ID = c.CELL_ID
        WHERE c.CELL_ID IS NOT NULL AND l.CELL_ID IS NULL""",
    "telemetry: impossible values": """
        SELECT COUNT(*) FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY
        WHERE SPEED_MPH < 0 OR SPEED_MPH > 100 OR FUEL_LEVEL_PCT NOT BETWEEN 0 AND 100
           OR LATITUDE NOT BETWEEN 40.49 AND 40.92 OR LONGITUDE NOT BETWEEN -74.27 AND -73.68""",
    "collisions: negative injury/death counts": """
        SELECT COUNT(*) FROM AUTOPULSE.CORE.FACT_COLLISION
        WHERE NUMBER_OF_PERSONS_INJURED < 0 OR NUMBER_OF_PERSONS_KILLED < 0""",
}


def local_time():
    return pendulum.now(NEW_YORK)


def during_driving(start=(8, 10), end=(17, 5)):
    now = local_time()
    return now.weekday() < 5 and start <= (now.hour, now.minute) < end          # Mon=0 ... Fri=4


# --------------------------------------------------
# Checks
# --------------------------------------------------

def streaming_host_is_producing():
    if not during_driving():
        raise AirflowSkipException("Outside driving hours (Mon-Fri 8:10-17:05 New York)")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    newest = None
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix="bronze/vehicle_telemetry/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet") and (newest is None or obj["LastModified"] > newest):
                newest = obj["LastModified"]
    if newest is None:
        raise AirflowFailException("No Bronze telemetry files at all: is the EC2 streaming host running?")
    minutes = (datetime.now(timezone.utc) - newest).total_seconds() / 60
    print(f"Newest Bronze telemetry file: {newest} ({minutes:.0f} min ago)")
    if minutes > 15:
        raise AirflowFailException(f"No Bronze telemetry for {minutes:.0f} min: check the EC2 streaming host")


def snowpipe_has_no_load_errors():
    problems = []
    for table in LANDING_TABLES:
        rows = snowflake_records(f"""
            SELECT FILE_NAME, STATUS, FIRST_ERROR_MESSAGE
            FROM TABLE(AUTOPULSE.INFORMATION_SCHEMA.COPY_HISTORY(
                TABLE_NAME => 'AUTOPULSE.LANDING.{table}',
                START_TIME => DATEADD(hour, -24, CURRENT_TIMESTAMP())))
            WHERE STATUS <> 'Loaded'""")
        print(f"{table}: {len(rows)} file(s) not loaded in the last 24 h")
        problems += [f"{table}: {name} ({status}: {error})" for name, status, error in rows]
    if problems:
        raise AirflowFailException("Snowpipe load errors:\n" + "\n".join(problems[:20]))


def dynamic_tables_refreshing():
    rows = snowflake_records("""
        SELECT NAME, STATE, STATE_MESSAGE, REFRESH_START_TIME
        FROM TABLE(AUTOPULSE.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
            NAME_PREFIX => 'AUTOPULSE.CORE.', ERROR_ONLY => TRUE))
        WHERE REFRESH_START_TIME > DATEADD(hour, -2, CURRENT_TIMESTAMP())""")
    if rows:
        details = "\n".join(f"{name} {state} at {start}: {message}" for name, state, message, start in rows)
        raise AirflowFailException("Dynamic table refreshes failed:\n" + details)
    print("No failed dynamic table refreshes in the last 2 hours")


def telemetry_is_fresh_in_snowflake():
    # Expected delay: stream (<=5 min) + this pipeline (<=15 min + Glue) + Snowpipe (~1 min)
    # + dynamic table (<=5 min): well under 60 minutes.
    if not during_driving(start=(9, 0), end=(18, 0)):
        raise AirflowSkipException("Outside 9:00-18:00 Mon-Fri: no fresh telemetry expected")
    minutes = snowflake_records(
        "SELECT DATEDIFF(minute, MAX(EVENT_TIMESTAMP), SYSDATE()) FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY"
    )[0][0]
    print(f"Newest vehicle event in Snowflake: {minutes} min ago")
    if minutes is None or minutes > 60:
        raise AirflowFailException(f"Newest telemetry in Snowflake is {minutes} min old (limit 60)")


def data_quality():
    failures = []
    for name, sql in DATA_QUALITY_CHECKS.items():
        problems = snowflake_records(sql)[0][0]
        print(f"{'PASS' if problems == 0 else 'FAIL'}  {name}: {problems}")
        if problems:
            failures.append(f"{name}: {problems}")
    if failures:
        raise AirflowFailException("Data quality checks failed:\n" + "\n".join(failures))


# --------------------------------------------------
# DAG
# --------------------------------------------------

with DAG(
    dag_id="autopulse_pipeline_health",
    description="Streaming host, Snowpipe, dynamic tables, freshness and data quality",
    schedule="*/15 8-18 * * 1-5",          # runs at 8:00, 8:15 ... 18:45, Mon-Fri (New York time)
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "autopulse", "retries": 0},    # a failed health check is the finding
    tags=["autopulse", "monitoring"],
) as dag:
    for check in (streaming_host_is_producing, snowpipe_has_no_load_errors, dynamic_tables_refreshing,
                  telemetry_is_fresh_in_snowflake, data_quality):
        PythonOperator(task_id=check.__name__, python_callable=check)   # independent: all run each time

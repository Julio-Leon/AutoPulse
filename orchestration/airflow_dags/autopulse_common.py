"""
AutoPulse - pieces shared by the Airflow DAGs in this folder (not a DAG itself).

    glue_job(...)              a task that runs one Glue job and waits for it to finish
    validate_data_lake(...)    a task function: checks the S3 layers after a pipeline run
    snowflake_records(sql)     runs a query in Snowflake (monitoring user, read-only)

Written for Airflow 3: DAG authors import from airflow.sdk (the "Task SDK"), and
the Python operators come from the "standard" provider package.
"""

import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

PROJECT_ROOT = Path(__file__).resolve().parents[2]          # /home/ec2-user/AutoPulse
NEW_YORK = "America/New_York"
START_DATE = pendulum.datetime(2026, 10, 1, tz=NEW_YORK)    # schedules count from here (no backfill)
AWS_REGION = "us-east-1"
BUCKET = "autopulse-julio-leon"
SNOWFLAKE_CONN_ID = "snowflake_autopulse"                    # defined by the host's settings file

DEFAULT_ARGS = {
    "owner": "autopulse",
    "retries": 1,                              # a failed task is tried once more ...
    "retry_delay": timedelta(minutes=2),       # ... 2 minutes later
}


def glue_job(task_id, job_name):
    """Start a Glue job and wait until it finishes; the task fails if the job fails."""
    return GlueJobOperator(
        task_id=task_id,
        job_name=job_name,              # the job already exists in Glue; nothing is created or changed
        region_name=AWS_REGION,
        aws_conn_id=None,               # AWS access comes from the instance's IAM role
        wait_for_completion=True,
        verbose=False,
        stop_job_run_on_kill=True,      # stopping the task in Airflow also stops the Glue run
    )


def validate_data_lake(pipeline, **context):
    """
    Run validation/data_lake/validate_data_lake.py for one pipeline. Every Bronze
    file written before this DAG run started must have been processed by it.
    """
    sys.path.insert(0, str(PROJECT_ROOT / "validation" / "data_lake"))
    import validate_data_lake

    report = validate_data_lake.run(pipeline, cutoff=context["dag_run"].start_date)
    for result in report["results"]:
        print(f"{result['status']:<5} {result['check']:<38} {result['detail']}")
    if not report["passed"]:
        failed = ", ".join(f["check"] for f in report["failures"])
        raise AirflowFailException(f"Data lake validation failed: {failed}")


def snowflake_records(sql):
    """Rows returned by a Snowflake query."""
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    return SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_records(sql)

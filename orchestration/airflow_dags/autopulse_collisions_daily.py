"""
AutoPulse - collisions pipeline, every day at 06:00 New York time.

    ingest_collisions_to_bronze    Glue autopulse-collisions-ingestion  (NYC API -> Bronze, latest 30 days)
    collisions_bronze_to_silver    Glue autopulse-collisions-silver     (new Bronze files -> Silver)
    collisions_silver_to_gold      Glue autopulse-collisions-gold       (new Silver files -> fact_collision)
    validate_data_lake             checks Bronze, Silver and Gold against each other

Each step starts only when the previous one succeeded. Snowpipe then loads the
new Gold files into Snowflake by itself.
"""

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

from autopulse_common import DEFAULT_ARGS, START_DATE, glue_job, validate_data_lake

with DAG(
    dag_id="autopulse_collisions_daily",
    description="NYC collisions: API -> Bronze -> Silver -> Gold, then validate",
    schedule="0 6 * * 1-5",                  # runs at 06:00 every day (New York time)
    start_date=START_DATE,
    catchup=False,                         # don't run the days missed before it was switched on
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["autopulse", "collisions", "batch"],
) as dag:
    ingest = glue_job("ingest_collisions_to_bronze", "autopulse-collisions-ingestion")
    silver = glue_job("collisions_bronze_to_silver", "autopulse-collisions-silver")
    gold = glue_job("collisions_silver_to_gold", "autopulse-collisions-gold")
    validate = PythonOperator(
        task_id="validate_data_lake",
        python_callable=validate_data_lake,
        op_kwargs={"pipeline": "collisions"},
        retries=0,                         # a failed check is a finding, not a glitch to retry
    )

    ingest >> silver >> gold >> validate

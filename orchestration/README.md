# AutoPulse orchestration: Apache Airflow 3

Airflow 3.3 runs on its own EC2 instance (`autopulse-airflow-host`; setup in
`infrastructure/ec2_airflow_host/`) as three services: the **DAG processor** reads
the DAG files, the **scheduler** starts runs and their tasks, and the **API server**
serves the web page (port 8080) and the API that running tasks report back to. It **schedules and runs** the batch side of
the pipeline and **monitors** everything else. DAGs are in `airflow_dags/`.

```
DAG                                      schedule (New York time)       tasks
---------------------------------------  -----------------------------  ------------------------------------------
autopulse_collisions_daily               06:00 every day                Glue collisions-ingestion -> collisions-silver
                                                                        -> collisions-gold -> validate_data_lake
autopulse_telemetry_every_15_minutes     every 15 min, Mon-Fri          new_bronze_files_arrived? -> Glue telemetry-silver
                                         8:00-17:45                     -> telemetry-gold -> validate_data_lake
autopulse_pipeline_health                every 15 min, Mon-Fri          streaming host producing, Snowpipe load errors,
                                         8:00-18:45                     dynamic table refreshes, telemetry freshness
                                                                        in Snowflake, data quality
```

Not run by Airflow, because they run by themselves:
- the EC2 streaming host (simulator -> Kafka -> Spark -> Bronze), always on;
- Snowflake: Snowpipe loads each Gold file ~1 minute after it lands, dynamic
  tables refresh themselves, a Snowflake task deletes old telemetry nightly.
Airflow *checks* both (pipeline_health DAG).

## How runs behave

- Each Glue task **starts the job and waits until it finishes**; a failed task is
  retried once after 2 minutes, then the run stops there and shows **failed** (red).
- `new_bronze_files_arrived` skips the telemetry run (pink "skipped" in the grid)
  when the stream wrote nothing since the last successful run, e.g. after 17:00:
  no Glue job is started, so no Glue cost.
- `validate_data_lake` runs `validation/data_lake/validate_data_lake.py`; any FAIL
  fails the task, and its log lists every check.
- Health checks that only make sense while the fleet drives are **skipped** outside
  those hours. A red health task = something to look at.
- `catchup=False`: switching a DAG on doesn't replay the past; `max_active_runs=1`:
  never two runs of a DAG at once.
- A schedule like `*/15 8-17 * * 1-5` runs **at** those times (Airflow 3 default):
  the 8:00 run starts at 8:00.

DAG files use Airflow 3 imports: `DAG` and the exceptions from `airflow.sdk`,
`PythonOperator` / `ShortCircuitOperator` from `airflow.providers.standard`.

## Files

| File | What it is |
|---|---|
| `airflow_dags/autopulse_common.py` | Shared pieces: Glue task, data lake validation, Snowflake queries |
| `airflow_dags/autopulse_collisions_daily.py` | Collisions DAG |
| `airflow_dags/autopulse_telemetry_every_15_minutes.py` | Telemetry DAG |
| `airflow_dags/autopulse_pipeline_health.py` | Health checks DAG (list of data-quality queries at the top) |

Access: AWS through the instance's IAM role (`policies/ec2_airflow_host/`);
Snowflake through the read-only service user `AUTOPULSE_AIRFLOW`
(`snowflake/10_airflow_monitoring_user.sql`), key pair kept on the host.

## Deploying DAG changes

Edit the DAG on the laptop, upload it, then copy it onto the Airflow host:

    aws s3 sync orchestration s3://autopulse-julio-leon/deployment/autopulse/orchestration --exclude "*__pycache__*"
    (on the host)  sudo aws s3 sync s3://autopulse-julio-leon/deployment/autopulse/orchestration /home/ec2-user/AutoPulse/orchestration
                   sudo chown -R ec2-user:ec2-user /home/ec2-user/AutoPulse

The DAG processor notices changed DAG files within about a minute; no restart needed.

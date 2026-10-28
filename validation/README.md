# AutoPulse validation

Two kinds of checks, both run by Airflow:

| | Data lake validation | Snowflake health & data quality |
|---|---|---|
| Question | Did the pipeline move the data correctly through S3? | Is the data we serve loaded, fresh and correct? |
| Code | `validation/data_lake/validate_data_lake.py` | `orchestration/airflow_dags/autopulse_pipeline_health.py` |
| Runs | Last task of each pipeline DAG (`validate_data_lake`), or by hand from a laptop | Every 15 min on weekdays (DAG `autopulse_pipeline_health`) |
| On failure | That pipeline run turns red in Airflow | That health task turns red in Airflow |

## Data lake validation

Reads only each Parquet file's footer (row count and column names), so it is
fast and cheap. Checks:

| Check | Fails when |
|---|---|
| `telemetry_stream_is_fresh` | during working hours, no Bronze telemetry file in the last 15 min (EC2 stream down) |
| `telemetry_bronze_fully_processed` | Silver + quarantine rows don't add up to the Bronze files the Silver job read (rows lost or duplicated), or a Bronze file written before the run wasn't processed (warns when files wait 30+ min) |
| `telemetry_quarantine_rate` | more than 1% of telemetry rows were rejected |
| `telemetry_gold_matches_silver` | Gold fact rows differ from Silver rows |
| `dim_vehicle_complete` | `gold/dim_vehicle/` isn't exactly one file (warns if not 10 vehicles) |
| `collisions_dates_in_every_layer` | a crash date is missing in Silver or Gold |
| `collisions_one_file_per_date` | a date folder has 2+ files (its rows would be counted twice) |
| `collisions_silver_not_above_bronze` | Silver has more rows than Bronze for a date |
| `collisions_gold_matches_silver` | Gold rows differ from Silver rows for a date |
| `collisions_rows_dropped_in_silver` | (warning) Silver dropped more than 1% of Bronze rows |
| `dim_date_complete`, `dim_location_complete` | the table is missing or has the wrong number of rows |
| `columns_<layer>_<table>` | the newest file lacks a column the next step expects |

Results: PASS, WARN (worth a look, doesn't fail the run), FAIL, SKIP (nothing to check yet).

Run it by hand from the project root (virtualenv active, normal AWS login):

    pip install -r requirements.txt
    python validation/data_lake/validate_data_lake.py
    python validation/data_lake/validate_data_lake.py --pipeline telemetry --date 2026-10-26

Telemetry days are UTC dates (the fleet only drives 8:00-17:00 New York, so a
working day never crosses midnight UTC).

## Snowflake health & data quality

See the docstring at the top of `autopulse_pipeline_health.py`: Snowpipe load
errors (24 h), failed dynamic table refreshes (2 h), telemetry freshness in
Snowflake (< 60 min), and data-quality queries (duplicate keys, facts pointing at
missing dimension rows, impossible values) that must each return 0.

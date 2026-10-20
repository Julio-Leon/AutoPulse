# AutoPulse — Change Report: Cloud Readiness (Step 1)

**Date:** 2026-10-20  **Target demo:** Friday 2026-10-30
**Scope:** Make the existing pipelines runnable locally *or* against S3 with no code changes, fix known issues, and prepare the AWS setup.
**Git:** Nothing was committed, branched, or pushed. All changes are uncommitted edits in your working tree. Review them with `git status` and `git diff`.

---

## 1. Summary

| # | Change | Why |
|---|---|---|
| 1 | New `src/common/config.py`, which holds all paths, Kafka settings, and Spark setup in one place | The same code can run in WSL (local disk) or on EC2 (S3) by changing `.env` only |
| 2 | Collisions ingestion pulls a **date window**, pages through all results, and writes Bronze **partitioned by crash date** | It used to pull 1,000 rows and overwrite everything. Now runs are incremental, can be re-run safely, and work with Airflow |
| 3 | Collisions Silver keeps the **newest** copy of each `collision_id` | The city revises records, and windows can be re-ingested. `dropDuplicates` kept an arbitrary copy |
| 4 | Streaming trigger changed from **5 s to 1 minute** (configurable), with one file per micro-batch | Fewer, larger files on S3, which is better for Snowpipe and cost |
| 5 | Maintenance rule fixed: new `miles_until_service` column and `service_due` / `ok` status | The old rule (`odometer % 5000 == 0`) almost never triggered |
| 6 | Vehicle Silver: parse failures tracked with an explicit `parse_failed` column | The old code filtered on a `parsed` column that no longer existed after `parsed.*` was expanded |
| 7 | Tests read paths from config; new test for maintenance fields | Tests now work against local data or S3 |
| 8 | AWS setup guide plus IAM policies in `infra/aws/` | Lets you create the bucket and permissions now |
| 9 | README updated (architecture, configuration, how to run, status) | It still listed PostgreSQL, and the streaming item was unchecked |

**Unchanged:** the Kafka/docker-compose setup, the collision Gold aggregation logic, the simulator's movement and fuel model, and every existing data-quality rule.

---

## 2. New files

| File | Purpose |
|---|---|
| `src/common/config.py` | Central settings (see §4). Also `build_spark()`, `path_exists()`, `setup_logging()` |
| `src/__init__.py`, `src/common/__init__.py`, `src/batch/__init__.py`, `src/ingestion/__init__.py`, `src/streaming/__init__.py` | Empty files that make `src` an importable package (`from src.common import config`) |
| `.env.example` | Template for your `.env`. `.env` is already git-ignored |
| `pytest.ini` | Makes `src` importable from tests (`pythonpath = .`) |
| `infra/aws/README.md` | Step-by-step S3 bucket, lifecycle rule, and IAM setup |
| `infra/aws/iam/autopulse-pipeline-s3-policy.json` | Read/write on the bucket, for your dev user and the EC2 role |
| `infra/aws/iam/autopulse-snowflake-s3-read-policy.json` | Read-only on `silver/` and `gold/`, for Snowflake (used in the next step) |
| `docs/reports/2026-10-20-cloud-readiness-changes.md` | This report |

---

## 3. Modified files

### Pattern applied to every script
Each script now:
- adds the project root to `sys.path` when run as `python src/.../file.py`. Both that and `python -m src.batch.file` work.
- gets its paths from `config` instead of hardcoded `PROJECT_ROOT / "data" / ...`.
- creates Spark with `config.build_spark(...)` instead of its own `SparkSession.builder`. Same master, shuffle partitions, and WARN log level as before, plus the UTC session timezone.
- checks that inputs exist with `config.path_exists(spark, path)` instead of `Path.exists()`, which can't check S3. The existence check now runs **after** Spark starts.
- sets up logging with `config.setup_logging(...)`. Same log file names in `logs/`, still overwritten on each run, still also printed to stdout.

### `src/ingestion/nyc_collisions_api.py` (rewritten)
- **CLI:** `--start-date`, `--end-date`, `--days-back` (default 30), `--max-records`.
- **API:** SoQL `$where crash_date between ...`, ordered by `collision_id`, paging 50,000 rows at a time with `$offset`, 3 retries with backoff, optional `NYC_OPEN_DATA_APP_TOKEN` header.
- **Bronze schema:** every field is stored as a string, and nested fields such as `location` are stored as JSON text. The API omits empty fields, so inferred types could differ between runs. Bronze is supposed to stay raw anyway.
- **Write:** `partitionBy("crash_date_part")` with dynamic partition overwrite, so re-running a window only replaces those days.
- Removed the `__import__("pyspark.sql.functions"...)` workaround.

### `src/batch/transform_collisions.py`
- Reads Bronze with `mergeSchema=true`.
- Deduplication now uses a window ranked by `ingested_at DESC` (newest wins) instead of `dropDuplicates`.
- Removed the module-level `global spark`.
- Silver now also carries the `crash_date_part` column from Bronze. It's harmless, and Snowflake can use it later.

### `src/batch/transform_gold.py`
- Only paths and Spark setup changed. The three aggregations are identical.

### `src/streaming/vehicle_telemetry_stream.py`
- `build_spark(..., kafka=True)` adds the `spark-sql-kafka` connector that matches your installed PySpark version. You no longer need `--packages` on the command line. Passing it still works.
- Trigger is `STREAM_TRIGGER_INTERVAL` (default `1 minute`, previously `5 seconds`).
- `coalesce(1)` before writing, so each micro-batch produces one Parquet file.
- Checkpoint is now `config.checkpoint_path("vehicle_telemetry_bronze")`, the same local folder as before by default.

### `src/batch/transform_vehicle_telemetry.py`
- Adds `parse_failed` (the payload couldn't be parsed as JSON) while `parsed` still exists. Invalid and valid records are split on that flag. Unparseable rows get `validation_error = "unparseable_payload"` if no other reason applies.
- Silver is written as 4 files instead of 15.

### `src/batch/transform_vehicle_gold.py`
- **New column `miles_until_service`** = `interval − (odometer mod interval)`.
- **`maintenance_status`** is now `service_due` when `miles_until_service ≤ MAINTENANCE_DUE_WINDOW_MILES` (default 250), otherwise `ok`. The old values `check_service_interval` / `no_interval_alert` no longer exist.
- The interval and the due window come from `.env`.
- Written as a single file (10 rows).

### `src/streaming/vehicle_telemetry_producer.py`, `src/streaming/traffic_producer.py`
- The broker, topic, vehicle count, and publish interval come from config. The defaults match the old hardcoded values. No logic changes.

### `tests/test_data_quality.py`
- Uses config paths and `path_exists`. All existing tests are unchanged.
- Removed the duplicated imports and the second `PROJECT_ROOT` block.
- **New:** `test_vehicle_gold_maintenance_fields_are_consistent`. It is skipped if vehicle Gold hasn't been built.

### `README.md`
- Tech stack: removed PostgreSQL and added EC2 and Snowflake.
- Added the architecture diagram, the configuration table, the "Running the pipelines" commands, and an updated status checklist.

---

## 4. Configuration reference (`.env`)

| Variable | Default | Notes |
|---|---|---|
| `DATA_ROOT` | `<project>/data` | `s3://bucket` is converted to `s3a://bucket` automatically |
| `CHECKPOINT_ROOT` | `<project>/checkpoints` | Keep it local, even on EC2 |
| `AWS_REGION` | `us-east-1` | |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:29092` | |
| `VEHICLE_TELEMETRY_TOPIC` / `TRAFFIC_INCIDENTS_TOPIC` | `vehicle.telemetry.raw` / `traffic.incidents.raw` | |
| `STREAM_TRIGGER_INTERVAL` | `1 minute` | Use `10 seconds` for quick local testing |
| `SIMULATOR_NUMBER_OF_VEHICLES` | `10` | Must match the existing `fleet_state.json`, or the simulator stops with an error, as before |
| `SIMULATOR_PUBLISH_INTERVAL_SECONDS` | `5` | |
| `NYC_OPEN_DATA_APP_TOKEN` | — | Optional |
| `MAINTENANCE_INTERVAL_MILES` / `MAINTENANCE_DUE_WINDOW_MILES` | `5000` / `250` | |
| `SPARK_MASTER` | `local[*]` | |
| `HADOOP_AWS_VERSION` | auto | 3.3.4 for Spark 3.5, 3.4.1 for Spark 4.x. Override only if S3 access fails with a class or version error |

S3 credentials: locally, from `aws configure` or keys in `.env`. On EC2, from the instance IAM role, with no keys.

---

## 5. ⚠️ Before you run: one-time actions

1. **Delete the old local collisions Bronze folder:** `rm -rf data/bronze/collisions`. It holds unpartitioned files with an inferred `location` struct. Mixing those with the new partitioned, all-string layout would make the Silver read fail. Then re-ingest.
2. **Rebuild vehicle Silver and Gold.** They are regenerated from Bronze, so just re-run the two batch jobs.
3. `cp .env.example .env` (the defaults keep everything local).

---

## 6. How to verify (in WSL)

> I couldn't run Spark in my workspace because its package mirror was unreachable. Everything compiles, the config, API paging, and maintenance logic were tested in plain Python, and the IAM JSON is valid. Please run the steps below and send me any errors.

```bash
cd ~/<path>/AutoPulse
source .venv/bin/activate
git status                       # review the changed and new files
git diff                         # line-by-line review

cp .env.example .env
rm -rf data/bronze/collisions

# Collisions batch
python src/ingestion/nyc_collisions_api.py --days-back 14 --max-records 5000
python src/batch/transform_collisions.py
python src/batch/transform_gold.py

# Streaming (two terminals); use a short trigger for testing
docker compose up -d
python src/streaming/vehicle_telemetry_producer.py
STREAM_TRIGGER_INTERVAL="10 seconds" python src/streaming/vehicle_telemetry_stream.py
# after ~1 min, Ctrl+C both, then:
python src/batch/transform_vehicle_telemetry.py
python src/batch/transform_vehicle_gold.py

pytest -q
```

Expected: all tests pass. `ls data/bronze/collisions` shows `crash_date_part=YYYY-MM-DD` folders. The vehicle Gold output shows `miles_until_service` and `maintenance_status`.

---

## 7. Your AWS action items (≈20 min)

Follow `infra/aws/README.md`:
1. Create the bucket `autopulse-data-lake-<suffix>` in us-east-1 (private, SSE-S3).
2. Add the lifecycle rule for incomplete uploads.
3. Create the `AutoPulsePipelineS3Access` policy and the `autopulse-dev` IAM user, then run `aws configure` in WSL.
4. Create the `AutoPulseSnowflakeS3Read` policy (not attached yet).
5. Set `DATA_ROOT=s3://<bucket>` in `.env`, re-run the commands in §6, and check with `aws s3 ls s3://<bucket>/ --recursive | head`.

Send me the bucket name when it's done. Never share access keys.

---

## 8. Decisions to note / open items

- **Spark Gold vs. Snowflake:** Snowflake will load **Silver**, the row-level data the fact tables need. The Spark Gold summaries stay as-is for now. Once the Snowflake views exist, we can retire whichever copy the dashboard doesn't use.
- **Collision Silver is a full rebuild** each run (overwrite). Snowpipe will see new file names each time, so the Snowflake load will use `MERGE` on `collision_id` to avoid duplicates. That's handled in the next step.
- **`requirements.txt` is unpinned.** I recommend pinning `pyspark` to the version you have installed (`pip show pyspark`) before building the EC2 image, so the Kafka and hadoop-aws connector versions stay in sync.
- **Stale quarantine output:** if a run has no quarantined rows, the previous quarantine folder isn't cleared. This behavior already existed and is minor.

## 9. Next steps (proposed schedule)

| Day | Work |
|---|---|
| Tue 10/20 – Wed 10/21 | You: verify §6 and create the AWS resources. Me: Snowflake SQL (storage integration, stage, file formats, landing tables, Snowpipe) |
| Thu 10/22 – Fri 10/23 | Star schema: DIM_VEHICLE, DIM_TIME, DIM_LOCATION, DIM_BOROUGH, FACT_VEHICLE_TELEMETRY, FACT_COLLISION, plus MERGE tasks |
| Mon 10/26 | EC2 host: Docker Compose with Kafka, simulator, streaming, and Airflow DAGs |
| Tue 10/27 | Analytics views and the dashboard (Streamlit in Snowflake) |
| Wed 10/28 – Thu 10/29 | End-to-end test, cost check, demo rehearsal |
| Fri 10/30 | Demo |

---

## Addendum (2026-10-20, 12:35): collision default date window

**Problem:** without `--end-date`, the default window ended *today*. NYC publishes collision data with a lag of several months (the latest data is from June 2026), so a default run returned no records.

**Fix** in `src/ingestion/nyc_collisions_api.py`:
- New `latest_available_crash_date()` asks the API for `max(crash_date)`.
- Without `--end-date`, the window now ends at that date, and `--days-back` (default 30) counts back from it. The date found is written to the log.
- If the lookup fails, it falls back to today and logs a warning.
- Explicit `--start-date` / `--end-date` work exactly as before.

```bash
python src/ingestion/nyc_collisions_api.py --max-records 5000              # latest 30 days of available data
python src/ingestion/nyc_collisions_api.py --start-date 2026-05-01 --end-date 2026-06-30
```

---

## Addendum (2026-10-20, 12:45): missing boroughs are kept as `UNKNOWN`

**Decision:** keep collisions that have no borough. Many are highway or bridge crashes with valid coordinates, which matter for totals and hotspots.

**Changes**
- `src/batch/transform_collisions.py`: `borough` is trimmed and upper-cased in Silver. Null or blank becomes `"UNKNOWN"`, and a new `borough_known` (boolean) column records whether the original had a borough. The count of UNKNOWN rows is logged.
- `src/batch/transform_gold.py`: the borough summary also excludes `"UNKNOWN"`. Hourly trends and hotspots still include every collision.
- `tests/test_data_quality.py`: two new tests. `test_silver_boroughs_are_standardized` checks for no null or blank boroughs and that the flag matches the value. `test_gold_borough_summary_excludes_unknown` checks that UNKNOWN never appears in the borough summary.

**Snowflake (next step):** `DIM_BOROUGH` will include an `UNKNOWN` member, so every collision joins to a borough.

**Rerun:** `python src/batch/transform_collisions.py && python src/batch/transform_gold.py && pytest -q`

**Possible follow-up:** the NYC data has records with latitude/longitude of `0, 0`. These currently pass `coordinates_valid` because they're within world bounds, and they would show up off the coast of Africa on a map. Tightening the check to NYC's bounding box (about lat 40.49–40.92, lon −74.26 to −73.69) would fix this. Not changed yet.

# AutoPulse — Change Report: Snowflake Ingestion & Star Schema (Step 2)

**Date:** 2026-10-20  **Bucket:** `autopulse-julio-leon`  **Snowflake account:** `RHELJEO-OZ82635`
**Git:** nothing was committed. All changes are uncommitted edits in your working tree (`git status`, `git diff`).

> **Not yet run against Snowflake.** I can't connect to your account from here. The scripts were checked by careful review only. Run them in order (see `snowflake/README.md`) and send me any error with its script name and line number.

---

## 1. Summary

| # | Change | Why |
|---|---|---|
| 1 | New `snowflake/` folder: 8 ordered SQL scripts + README | Full Snowflake setup: S3 connection, Snowpipe, star schema, scheduled load, views, checks |
| 2 | Snowpipe auto-ingests `silver/collisions/` and `silver/vehicle_telemetry/` | New Silver files load automatically (decision from the ChatGPT plan) |
| 3 | Star schema in Snowflake: 4 dimensions, 2 facts | Facts and dimensions are modeled in Snowflake, as agreed |
| 4 | Streams + scheduled task + stored procedure | Loads only new rows every 5 min, dimensions before facts, in one transaction |
| 5 | Views that replace the Spark Gold outputs, plus 2 new ones | The Gold layer now lives in Snowflake |
| 6 | Data-quality and Spark-vs-Snowflake parity checks | Replaces the Gold pytest checks and proves the Spark Gold jobs can be retired |
| 7 | AWS guide steps 7–8 and a trust-policy template | Snowflake role and S3 → Snowpipe event notification |
| 8 | `src/common/config.py`: Spark writes standard Parquet timestamps | Avoids the legacy INT96 format so Snowflake reads timestamps reliably |

---

## 2. New files

| File | Run as | What it does |
|---|---|---|
| `snowflake/README.md` | — | Architecture, the **12-step setup order** (Snowflake and AWS interleaved), everyday commands, how to pause to save credits |
| `snowflake/01_account_setup.sql` | ACCOUNTADMIN | Role `AUTOPULSE_ENGINEER` (granted to you), warehouse `AUTOPULSE_WH` (X-Small, auto-suspend 60 s), create-database and execute-task grants |
| `snowflake/02_storage_integration.sql` | ACCOUNTADMIN | `AUTOPULSE_S3_INT` (read-only on `silver/` and `gold/`). **You paste your AWS role ARN** in it, and it shows the 2 values for the AWS trust policy |
| `snowflake/03_database_stages_raw.sql` | AUTOPULSE_ENGINEER | Database `AUTOPULSE`, schemas `RAW` + `ANALYTICS`, Parquet file format, stages, landing tables (VARIANT), streams, typed staging tables |
| `snowflake/04_snowpipe.sql` | AUTOPULSE_ENGINEER | Two auto-ingest pipes. Backfills existing files (`REFRESH`) and shows the SQS ARN for the S3 notification |
| `snowflake/05_star_schema.sql` | AUTOPULSE_ENGINEER | `REF_VEHICLE` (fictional fleet metadata), `DIM_TIME` (hourly, 2012–2027, filled), `DIM_BOROUGH` (5 boroughs + UNKNOWN), `DIM_LOCATION`, `DIM_VEHICLE`, `FACT_COLLISION`, `FACT_VEHICLE_TELEMETRY` |
| `snowflake/06_load_procedure_and_task.sql` | AUTOPULSE_ENGINEER | `SP_LOAD_STAR_SCHEMA` (stream → staging → dimensions → facts), runs it once, then creates and starts `TASK_LOAD_STAR_SCHEMA` |
| `snowflake/07_analytics_views.sql` | AUTOPULSE_ENGINEER | Six `VW_*` views (§4) |
| `snowflake/08_data_quality_checks.sql` | AUTOPULSE_ENGINEER | Integrity checks, freshness, parity with Spark Gold, raw-type troubleshooting |
| `infra/aws/iam/autopulse-snowflake-trust-policy.json` | — | Trust-policy template. Fill in the 2 values from script 02 |
| `docs/reports/2026-10-20-snowflake-star-schema.md` | — | This report |

## 3. Modified files

| File | Change |
|---|---|
| `src/common/config.py` | `build_spark()` adds `spark.sql.parquet.outputTimestampType = TIMESTAMP_MICROS`. **Re-run the Silver jobs** so files in S3 use it |
| `infra/aws/README.md` | New step 7 (Snowflake IAM role + trust policy) and step 8 (S3 event notification → Snowpipe SQS) |
| `README.md` | Link to `snowflake/README.md` |

Nothing else changed. The Spark Gold scripts are still in place (§6).

---

## 4. The model

### Star schema

| Table | Grain / key | Notes |
|---|---|---|
| `FACT_COLLISION` | 1 row per `collision_id` | Measures: collision_count (=1), persons/pedestrians/cyclists/motorists injured and killed. Plus street, zip, top-2 contributing factors and vehicle types. Updated in place only when a **newer** ingestion of the collision arrives |
| `FACT_VEHICLE_TELEMETRY` | 1 row per `event_id` | Speed, odometer, fuel, engine status, lat/lon, UTC and local timestamps, Kafka partition/offset. Insert-only |
| `DIM_TIME` | `time_key` = YYYYMMDDHH (local NYC time) | Date, hour, day name, weekend flag, ISO week, month, quarter, year. `-1` = unknown |
| `DIM_LOCATION` | grid cell, lat/lon rounded to 3 decimals (~110 m × 85 m) | Shared by **both** facts, which links vehicles to collision hotspots. `0` = missing or outside NYC |
| `DIM_BOROUGH` | borough | 0 = UNKNOWN (agreed earlier), 1–5 = boroughs |
| `DIM_VEHICLE` | `vehicle_id` | Type, make, model, year, depot, service interval (from fictional `REF_VEHICLE`), first/last seen |

### Views (the new Gold layer)

| View | Replaces / purpose |
|---|---|
| `VW_HOURLY_COLLISIONS` | Spark `gold/hourly_collisions` |
| `VW_BOROUGH_SUMMARY` | Spark `gold/borough_summary` (excludes UNKNOWN) |
| `VW_COLLISION_HOTSPOTS` | Spark `gold/geographic_hotspots`, plus `hotspot_rank` |
| `VW_FLEET_STATUS` | Spark `gold/vehicle_fleet_summary`. Same maintenance rule (per-vehicle interval, 250-mile window) plus `connection_status` (online if it reported within 5 min) and `fuel_status` |
| `VW_DAILY_COLLISIONS` | **New:** daily trend line |
| `VW_VEHICLE_HOTSPOT_EXPOSURE` | **New:** batch + stream integration. Share of each vehicle's readings inside the top-100 collision cells |

### Load logic (`SP_LOAD_STAR_SCHEMA`)
1. Clears staging, then reads each stream into typed staging. Keeps the newest row per `collision_id`, and one row per `event_id`.
2. Merges new boroughs, location cells, and vehicles.
3. Upserts collisions and inserts new telemetry events.
4. Everything runs in **one transaction**. On error it rolls back, and the streams keep their rows for the next try.

---

## 5. Decisions made (please review)

| Decision | Reason | Alternative |
|---|---|---|
| Landing tables store the whole row as VARIANT | Schema changes in Silver never break Snowpipe | Typed landing tables (more brittle) |
| Telemetry mapped to `DIM_TIME` in **NYC local time** | Collisions are published in local time, so hours line up. UTC is kept on the fact | Everything in UTC |
| Coordinates outside NYC's box (lat 40.49–40.92, lon −74.27 to −73.68) → unknown location | Removes the `0,0` points from maps (the issue flagged earlier). Done in Snowflake; Spark Silver is unchanged | Also flag them in Spark Silver |
| Hotspot = top 100 cells by collision count | A simple, explainable definition for the demo. It doesn't claim a vehicle was near a specific crash | Distance/time-window matching (more work) |
| Fictional `REF_VEHICLE` (Ford Transit, Ram ProMaster, Sprinter, Camry, box trucks, 3 depots) | Gives `DIM_VEHICLE` real attributes. The fleet is simulated | Leave attributes blank |
| Task every 5 min, only when streams have data | Near-real-time without wasting credits | Have Airflow call the procedure |
| `SP_LOAD_STAR_SCHEMA` is caller's-rights and sets session `TIMEZONE = 'UTC'` | Spark writes UTC. This guarantees timestamps parse correctly | — |

---

## 6. Retiring the Spark Gold jobs (agreed direction)

`transform_gold.py` and `transform_vehicle_gold.py` are **not deleted yet**. Once `08_data_quality_checks.sql` Part B returns no differences, I'll:
- delete both scripts and their Gold tests
- remove `gold/` from the Snowflake integration, the read policy, and the `S3_GOLD` stage
- update the READMEs

Expected difference: hotspot counts, because Spark still counts out-of-NYC points. Borough, hourly, and fleet numbers should match exactly.

---

## 7. What you need to do

1. **Re-run the pipelines to S3** (`DATA_ROOT=s3://autopulse-julio-leon` in `.env`). That covers both collision jobs, both vehicle jobs, and the two Gold jobs (for the parity check).
2. Follow **`snowflake/README.md` → Setup order** (12 steps, about 45 min). The AWS parts are:
   - create `AutoPulseSnowflakeRole` (step 7a)
   - paste the trust policy (step 7c)
   - create the S3 event notification (step 8)
3. Send me the output of the Part A checks in script 08, plus any errors.

## 8. Next steps

| When | Work |
|---|---|
| After your Snowflake run | Fix anything that errors; retire the Spark Gold jobs |
| Thu–Fri 10/22–10/23 | Airflow DAGs (collision ingestion + Silver jobs; vehicle Silver every few minutes) |
| Mon 10/26 | EC2 host: Docker Compose with Kafka, simulator, streaming, Airflow |
| Tue 10/27 | Dashboard: Streamlit in Snowflake on the `VW_*` views (fleet map, hotspots, KPIs) |
| Wed–Thu 10/28–10/29 | End-to-end test, cost check, rehearsal |

---

## Addendum (2026-10-20, 13:50): Spark S3 credentials

**Problem:** writing to S3 failed with `NoAuthWithAWSException: No AWS Credentials provided by ... EnvironmentVariableCredentialsProvider IAMInstanceCredentialsProvider`. Hadoop's default S3A credential chain checks environment variables and the EC2 role, but **not** `~/.aws/credentials` (the file `aws configure` writes). The earlier report said `aws configure` was enough. It wasn't.

**Fix** in `src/common/config.py`: Spark now sets `fs.s3a.aws.credentials.provider` to the AWS SDK's default chain: env vars → `~/.aws` profile → EC2 instance role. That's the same lookup the AWS CLI uses. The class is chosen to match the hadoop-aws version: SDK v2 `DefaultCredentialsProvider` for 3.4.x (your setup), SDK v1 `DefaultAWSCredentialsProviderChain` for 3.3.x.

**Result:** `aws configure` in WSL is all Spark needs locally. On EC2 the instance role works with no keys.

---

## Addendum (2026-10-20, 14:00): guaranteed Silver collision schema

**Problem:** `transform_gold.py` failed with `UNRESOLVED_COLUMN ... number_of_persons_killed`. The NYC API omits fields that have no value. In the 2026-05-12 → 2026-06-11 window, no record had `number_of_persons_killed`, so that column didn't exist in Bronze or Silver.

**Fix** in `src/batch/transform_collisions.py`: a new `EXPECTED_COLUMNS` list (22 columns used by Gold, the tests, and Snowflake). Any column missing from Bronze is added as null before type casting, and a warning lists which ones were added. Gold already treats null counts as 0. Snowflake's load already treated missing fields as 0, so nothing changed there.

**Rerun:** `python src/batch/transform_collisions.py && python src/batch/transform_gold.py`

---

## Addendum (2026-10-20, 15:15): no dedicated IAM user for local development

**Reason:** your AWS login isn't allowed to create IAM users.

**Changes**
- `infra/aws/README.md` step 4a: the `autopulse-dev` IAM user is gone. Local development uses **your own AWS login**: either an access-portal (SSO) profile via `aws configure sso` plus `AWS_PROFILE` in `.env`, or access keys on your own IAM user via `aws configure`. Both are picked up by Spark through the default credential chain (13:50 addendum). The `AutoPulsePipelineS3Access` policy is now only for the future EC2 role.
- `infra/aws/README.md` step 7: a note on what to send your AWS administrator if you also can't create IAM roles or policies. Snowflake needs that role to read a private bucket.
- `.env.example`: replaced the commented access-key lines with an optional `AWS_PROFILE`.

This supersedes step 3 of §7 in `2026-10-20-cloud-readiness-changes.md`.

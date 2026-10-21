# AutoPulse — Snowflake

Snowflake is AutoPulse's analytics warehouse. Spark writes cleaned **Silver**
Parquet to S3. **Snowpipe** loads each new file into landing tables, and a
scheduled **task** turns those rows into a **star schema** (facts and
dimensions). **Views** on top replace the old Spark Gold layer and feed the dashboard.

```
S3 silver/collisions/          ─┐                    ┌─ DIM_TIME, DIM_LOCATION
S3 silver/vehicle_telemetry/   ─┤ Snowpipe ─► RAW ─► │  DIM_BOROUGH, DIM_VEHICLE
                                │ (auto-ingest)  │   ├─ FACT_COLLISION
                                │            streams └─ FACT_VEHICLE_TELEMETRY
                                │                + task (every 5 min)     │
                                                                          ▼
                                                     VW_* views ─► dashboard
```

| Object | Purpose |
|---|---|
| `AUTOPULSE_ENGINEER` role, `AUTOPULSE_WH` warehouse (XS, 60 s auto-suspend) | Own and run everything |
| `AUTOPULSE_S3_INT` storage integration | Read-only trust between Snowflake and the bucket |
| `RAW.S3_SILVER` / `RAW.S3_GOLD` stages | S3 locations |
| `RAW.COLLISIONS`, `RAW.VEHICLE_TELEMETRY` | Landing tables: each Silver row stored as VARIANT |
| `RAW.PIPE_COLLISIONS`, `RAW.PIPE_VEHICLE_TELEMETRY` | Snowpipes (auto-ingest) |
| `RAW.*_STREAM` | Track landing rows not yet modeled |
| `ANALYTICS.SP_LOAD_STAR_SCHEMA` + `TASK_LOAD_STAR_SCHEMA` | Load dimensions, then facts, in one transaction |
| `ANALYTICS.DIM_*`, `ANALYTICS.FACT_*` | Star schema |
| `ANALYTICS.VW_*` | Reporting views |

## Setup order

Run each script in a Snowsight worksheet (**Projects → Worksheets → +**).
To run a whole script, select all and press **Ctrl+Shift+Enter**, or use
the ▶ dropdown → *Run All*.

| # | Where | Step |
|---|---|---|
| 1 | Snowflake | `01_account_setup.sql` (as ACCOUNTADMIN) |
| 2 | AWS | [`infra/aws/README.md`](../infra/aws/README.md) step **7a**: create `AutoPulseSnowflakeRole`, copy its ARN |
| 3 | Snowflake | Paste the ARN into `02_storage_integration.sql` and run it (as ACCOUNTADMIN). Copy the two values from `DESC INTEGRATION` |
| 4 | AWS | Step **7c**: update the role's trust policy with those values |
| 5 | Snowflake | `03_database_stages_raw.sql`. The `LIST @S3_SILVER` result must show your files |
| 6 | Snowflake | `04_snowpipe.sql`. Copy `notification_channel` from `SHOW PIPES` |
| 7 | AWS | Step **8**: S3 event notification → that SQS ARN |
| 8 | Snowflake | Wait about 1 min. The row-count query at the end of 04 should show rows |
| 9 | Snowflake | `05_star_schema.sql` |
| 10 | Snowflake | `06_load_procedure_and_task.sql` (runs the first load and starts the task) |
| 11 | Snowflake | `07_analytics_views.sql` |
| 12 | Snowflake | `08_data_quality_checks.sql`. All `failures` should be 0, and the parity queries should return no rows |

Every script is safe to re-run, **except** that re-creating the storage
integration (02) generates a new external ID. It uses `IF NOT EXISTS` to avoid that.

## Everyday use

```sql
USE ROLE AUTOPULSE_ENGINEER;
USE WAREHOUSE AUTOPULSE_WH;

-- Load now instead of waiting for the task
CALL AUTOPULSE.ANALYTICS.SP_LOAD_STAR_SCHEMA();

-- Did Snowpipe load anything recently?
SELECT * FROM TABLE(AUTOPULSE.INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'AUTOPULSE.RAW.VEHICLE_TELEMETRY',
  START_TIME => DATEADD('hour', -24, CURRENT_TIMESTAMP())));

-- Dashboard queries
SELECT * FROM AUTOPULSE.ANALYTICS.VW_FLEET_STATUS;
SELECT * FROM AUTOPULSE.ANALYTICS.VW_COLLISION_HOTSPOTS WHERE hotspot_rank <= 50;
```

> `SP_LOAD_STAR_SCHEMA` sets your session time zone to UTC, because Spark writes
> UTC timestamps. Displayed `TIMESTAMP_LTZ` values will then show in UTC.

## Pausing to save trial credits

```sql
ALTER TASK AUTOPULSE.ANALYTICS.TASK_LOAD_STAR_SCHEMA SUSPEND;
ALTER PIPE AUTOPULSE.RAW.PIPE_COLLISIONS SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE AUTOPULSE.RAW.PIPE_VEHICLE_TELEMETRY SET PIPE_EXECUTION_PAUSED = TRUE;
-- Resume: ... RESUME;  /  ... SET PIPE_EXECUTION_PAUSED = FALSE;
```

The warehouse suspends itself after 60 seconds idle. The task's `WHEN` check
is free, so the warehouse only starts when new data has arrived.

## Design notes

- **Landing as VARIANT:** a new or missing Silver column never breaks
  Snowpipe. Typed extraction happens once, in the load procedure.
- **Re-deliveries are expected:** Spark rewrites Silver each run, so the same
  collision or event can arrive again. `MERGE` on `collision_id` / `event_id`
  keeps facts unique. A collision is updated only when a newer ingestion of it arrives.
- **Time:** collisions are published in NYC local time, and telemetry arrives in UTC.
  Both are mapped to `DIM_TIME` in **local time**, and UTC is kept on the telemetry fact.
- **Location:** `DIM_LOCATION` is a grid of about 110 m × 85 m cells, shared by both facts.
  Coordinates outside NYC's bounding box (including `0,0`) map to key `0` (unknown).
- **Hotspot exposure** (`VW_VEHICLE_HOTSPOT_EXPOSURE`) is the share of a vehicle's
  readings inside the 100 cells with the most collisions. It does not mean a vehicle
  was near a specific collision when it happened.
- **Vehicle metadata** (`REF_VEHICLE`) is fictional, because the fleet is simulated.

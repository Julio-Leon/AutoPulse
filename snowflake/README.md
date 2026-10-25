# AutoPulse in Snowflake

Snowflake is the serving layer: it loads the Gold tables from S3 automatically
and gives the dashboard ready-made views.

```
S3 gold/<table>/  --Snowpipe-->  LANDING.<table>  --dynamic table-->  CORE.<table>  -->  ANALYTICS.VW_*
 (Glue writes)     (~1 min)      raw, append-only   (dedup, auto)      star schema        dashboard
```

| Schema | What's in it | Kept up to date by |
|---|---|---|
| `LANDING` | Raw copies of the Gold files, plus `SOURCE_FILE` / `LOADED_AT` | 5 Snowpipes (one per Gold table) |
| `CORE` | `DIM_DATE`, `DIM_LOCATION`, `DIM_VEHICLE`, `FACT_COLLISION`, `FACT_VEHICLE_TELEMETRY`: newest version of each row | Dynamic tables (refresh on their own) |
| `ANALYTICS` | `VW_COLLISION_HOTSPOTS`, `VW_FLEET_CURRENT_POSITION`, `VW_VEHICLE_RECENT_PATH`, `VW_VEHICLE_DAILY_SUMMARY`, `VW_VEHICLE_HOTSPOT_EXPOSURE` | Views (always current) |

A nightly task (`LANDING.DELETE_OLD_TELEMETRY`) deletes telemetry older than
30 days, matching the S3 lifecycle rules.

## Setup, in order

Run each `.sql` file in a Snowsight worksheet (paste it, then **Run All**).

1. **`01_warehouse_database_schemas.sql`**: warehouse `AUTOPULSE_WH`, database `AUTOPULSE`, the 3 schemas.
2. **`02_storage_integration.sql`** (as ACCOUNTADMIN, once). The last result
   shows `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`: copy both.
3. **AWS: create the role `AutoPulseSnowflakeRole`.**
   1. In `policies/snowflake_s3_access/trust_policy.json`, replace
      `REPLACE_WITH_STORAGE_AWS_IAM_USER_ARN` and `REPLACE_WITH_STORAGE_AWS_EXTERNAL_ID`
      with the two values.
   2. IAM -> Roles -> Create role -> **Custom trust policy** -> paste the file -> Next
      -> no managed policies -> name `AutoPulseSnowflakeRole` -> Create.
   3. Open the role -> Add permissions -> Create inline policy -> JSON ->
      paste `policies/snowflake_s3_access/permissions_policy.json`
      (read-only on `gold/`) -> name `AutoPulseSnowflakeGoldRead`.
4. **`03_stage_and_file_format.sql`**: the final `LIST @GOLD_STAGE` tests the
   connection. An error means step 3 isn't right; an empty list is fine.
5. **`04_landing_tables.sql`**: the 5 landing tables.
6. **`05_snowpipes.sql`**: the 5 pipes. In the last result (`SHOW PIPES`),
   copy the **`notification_channel`** value (an `arn:aws:sqs:...` queue; the
   same for all pipes).
7. **AWS: S3 event notification** (tells Snowflake when Gold files land).
   Put the queue ARN in `infrastructure/s3_bucket/snowpipe_event_notification.json`
   (replace `REPLACE_WITH_SNOWFLAKE_NOTIFICATION_CHANNEL`), then from the project root:

       aws s3api put-bucket-notification-configuration --bucket autopulse-julio-leon --notification-configuration file://infrastructure/s3_bucket/snowpipe_event_notification.json

   (This replaces the bucket's event notifications; the bucket has no others.)
8. **`06_initial_load.sql`**: loads the Gold files that already exist.
9. **`07_core_dynamic_tables.sql`**: the star schema.
10. **`08_retention_task.sql`**: the nightly 30-day cleanup.
11. **`09_analytics_views.sql`**: the dashboard views.

## Checking that it works

```sql
-- Rows per landing table
SELECT 'FACT_VEHICLE_TELEMETRY', COUNT(*) FROM AUTOPULSE.LANDING.FACT_VEHICLE_TELEMETRY;
-- A pipe's state (pending files, last error)
SELECT SYSTEM$PIPE_STATUS('AUTOPULSE.LANDING.FACT_VEHICLE_TELEMETRY_PIPE');
-- Files a pipe loaded in the last hour, and any errors
SELECT * FROM TABLE(AUTOPULSE.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'AUTOPULSE.LANDING.FACT_VEHICLE_TELEMETRY',
    START_TIME => DATEADD(hour, -1, CURRENT_TIMESTAMP())));
-- Dynamic table refreshes
SHOW DYNAMIC TABLES IN SCHEMA AUTOPULSE.CORE;
-- The dashboard
SELECT * FROM AUTOPULSE.ANALYTICS.VW_FLEET_CURRENT_POSITION;
SELECT * FROM AUTOPULSE.ANALYTICS.VW_COLLISION_HOTSPOTS ORDER BY HOTSPOT_RANK LIMIT 20;
```

## Cost

- **Snowpipe**: serverless, billed per file loaded (tiny files, tiny cost).
- **AUTOPULSE_WH** (X-Small, 1 credit/hour while running, suspends after 60 s idle):
  dynamic table refreshes and dashboard queries. A refresh with no new data
  is skipped and uses no compute, so nights and weekends cost nothing.

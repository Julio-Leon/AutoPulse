-- =====================================================================
-- AutoPulse - Snowflake step 6: load the Gold files that already exist
-- Run AFTER the S3 event notification is set up (snowflake/README.md).
-- =====================================================================
--
-- S3 events only announce files written from now on. REFRESH makes each pipe
-- look at its folder and load the files it hasn't loaded yet (files written in
-- the last 7 days). Safe to run again: already-loaded files are skipped.
--
-- Also use it later if a Gold job ran while the notification was missing.

USE ROLE SYSADMIN;
USE DATABASE AUTOPULSE;
USE SCHEMA LANDING;

ALTER PIPE DIM_DATE_PIPE REFRESH;
ALTER PIPE DIM_LOCATION_PIPE REFRESH;
ALTER PIPE DIM_VEHICLE_PIPE REFRESH;
ALTER PIPE FACT_COLLISION_PIPE REFRESH;
ALTER PIPE FACT_VEHICLE_TELEMETRY_PIPE REFRESH;

-- Wait about a minute, then check how many rows arrived in each table.
SELECT 'DIM_DATE' AS landing_table, COUNT(*) AS row_count FROM DIM_DATE
UNION ALL SELECT 'DIM_LOCATION', COUNT(*) FROM DIM_LOCATION
UNION ALL SELECT 'DIM_VEHICLE', COUNT(*) FROM DIM_VEHICLE
UNION ALL SELECT 'FACT_COLLISION', COUNT(*) FROM FACT_COLLISION
UNION ALL SELECT 'FACT_VEHICLE_TELEMETRY', COUNT(*) FROM FACT_VEHICLE_TELEMETRY;

-- A pipe's state, e.g. its pending file count and last error:
-- SELECT SYSTEM$PIPE_STATUS('AUTOPULSE.LANDING.FACT_VEHICLE_TELEMETRY_PIPE');

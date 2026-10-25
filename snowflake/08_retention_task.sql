-- =====================================================================
-- AutoPulse - Snowflake step 8: keep only the last 30 days of telemetry
-- =====================================================================
--
-- S3 deletes telemetry files after 30 days (lifecycle rules), but rows already
-- loaded into Snowflake stay until deleted here. This TASK runs every night at
-- 03:30 New York time and deletes from the landing tables:
--   - telemetry events older than 30 days
--   - old copies of dim_vehicle rows (a new copy arrives with every Gold run;
--     only the newest per vehicle is used, keep 7 days of history)
-- The CORE dynamic tables follow automatically on their next refresh.
--
-- Collisions are kept (they're needed for the 1-year hotspot history).

USE ROLE SYSADMIN;
USE DATABASE AUTOPULSE;
USE SCHEMA LANDING;

CREATE OR REPLACE TASK DELETE_OLD_TELEMETRY
    WAREHOUSE = AUTOPULSE_WH
    SCHEDULE = 'USING CRON 30 3 * * * America/New_York'
    COMMENT = 'Nightly: delete telemetry older than 30 days'
AS
EXECUTE IMMEDIATE $$
BEGIN
    DELETE FROM AUTOPULSE.LANDING.FACT_VEHICLE_TELEMETRY
    WHERE EVENT_TIMESTAMP < DATEADD(day, -30, SYSDATE());   -- SYSDATE() = now, UTC

    -- Only older copies: a vehicle's newest row is always kept, even if it
    -- hasn't been seen for a while.
    DELETE FROM AUTOPULSE.LANDING.DIM_VEHICLE AS old_copy
    USING (SELECT VEHICLE_ID, MAX(UPDATED_AT) AS NEWEST_UPDATE
           FROM AUTOPULSE.LANDING.DIM_VEHICLE GROUP BY VEHICLE_ID) AS newest
    WHERE old_copy.VEHICLE_ID = newest.VEHICLE_ID
      AND old_copy.UPDATED_AT < newest.NEWEST_UPDATE
      AND old_copy.LOADED_AT < DATEADD(day, -7, CURRENT_TIMESTAMP());
END;
$$;

-- Tasks are created paused; this switches it on.
ALTER TASK DELETE_OLD_TELEMETRY RESUME;

SHOW TASKS IN SCHEMA AUTOPULSE.LANDING;

-- To run it right away (e.g. to test): EXECUTE TASK DELETE_OLD_TELEMETRY;

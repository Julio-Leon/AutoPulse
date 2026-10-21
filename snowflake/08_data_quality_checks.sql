-- =====================================================================
-- AutoPulse | 08 Data-quality and parity checks
-- Run as: AUTOPULSE_ENGINEER, any time after a load.
--
-- Part A: every check returns ONE row; `failures` must be 0.
-- Part B: compares Snowflake views with the Spark Gold output in S3.
--         Run it after both were built from the same Silver data. Once it
--         matches, the Spark Gold jobs can be retired.
-- =====================================================================

USE ROLE AUTOPULSE_ENGINEER;
USE WAREHOUSE AUTOPULSE_WH;
USE SCHEMA AUTOPULSE.ANALYTICS;
ALTER SESSION SET TIMEZONE = 'UTC';

-- ---------------------------------------------------------------------
-- Part A: integrity checks
-- ---------------------------------------------------------------------
SELECT 'fact_collision: not empty' AS check_name,
       IFF(COUNT(*) = 0, 1, 0) AS failures
FROM FACT_COLLISION
UNION ALL
SELECT 'fact_collision: duplicate collision_id',
       COUNT(*) - COUNT(DISTINCT collision_id)
FROM FACT_COLLISION
UNION ALL
SELECT 'fact_collision: orphan time_key',
       COUNT_IF(t.time_key IS NULL)
FROM FACT_COLLISION f LEFT JOIN DIM_TIME t ON t.time_key = f.time_key
UNION ALL
SELECT 'fact_collision: orphan location_key',
       COUNT_IF(l.location_key IS NULL)
FROM FACT_COLLISION f LEFT JOIN DIM_LOCATION l ON l.location_key = f.location_key
UNION ALL
SELECT 'fact_collision: orphan borough_key',
       COUNT_IF(b.borough_key IS NULL)
FROM FACT_COLLISION f LEFT JOIN DIM_BOROUGH b ON b.borough_key = f.borough_key
UNION ALL
SELECT 'fact_collision: negative injury/death counts',
       COUNT_IF(LEAST(persons_injured, persons_killed, pedestrians_injured, pedestrians_killed,
                      cyclists_injured, cyclists_killed, motorists_injured, motorists_killed) < 0)
FROM FACT_COLLISION
UNION ALL
SELECT 'fact_vehicle_telemetry: not empty',
       IFF(COUNT(*) = 0, 1, 0)
FROM FACT_VEHICLE_TELEMETRY
UNION ALL
SELECT 'fact_vehicle_telemetry: duplicate event_id',
       COUNT(*) - COUNT(DISTINCT event_id)
FROM FACT_VEHICLE_TELEMETRY
UNION ALL
SELECT 'fact_vehicle_telemetry: orphan vehicle_key',
       COUNT_IF(v.vehicle_key IS NULL)
FROM FACT_VEHICLE_TELEMETRY f LEFT JOIN DIM_VEHICLE v ON v.vehicle_key = f.vehicle_key
UNION ALL
SELECT 'fact_vehicle_telemetry: missing event timestamp',
       COUNT_IF(event_ts_utc IS NULL OR time_key = -1)
FROM FACT_VEHICLE_TELEMETRY
UNION ALL
SELECT 'fact_vehicle_telemetry: out-of-range values',
       COUNT_IF(speed_mph < 0 OR odometer_miles < 0
                OR fuel_level_pct < 0 OR fuel_level_pct > 100
                OR engine_status NOT IN ('running', 'idling', 'off'))
FROM FACT_VEHICLE_TELEMETRY
UNION ALL
SELECT 'dim_vehicle: vehicles missing reference data',
       COUNT_IF(make = 'UNKNOWN')
FROM DIM_VEHICLE
UNION ALL
SELECT 'vw_borough_summary: contains UNKNOWN',
       COUNT_IF(borough = 'UNKNOWN')
FROM VW_BOROUGH_SUMMARY
UNION ALL
SELECT 'vw_fleet_status: maintenance fields inconsistent',
       COUNT_IF(miles_until_service < 0
                OR (miles_until_service <= 250 AND maintenance_status <> 'service_due')
                OR (miles_until_service > 250 AND maintenance_status <> 'ok'))
FROM VW_FLEET_STATUS
UNION ALL
-- Informational, not a failure: collisions without usable coordinates.
SELECT 'info: collisions with no mapped location (not a failure)',
       COUNT_IF(location_key = 0)
FROM FACT_COLLISION
ORDER BY failures DESC, check_name;

-- Pipeline freshness
SELECT 'collisions' AS dataset, MAX(loaded_at) AS last_loaded_at, COUNT(*) AS rows_in_fact
FROM FACT_COLLISION
UNION ALL
SELECT 'vehicle_telemetry', MAX(loaded_at), COUNT(*)
FROM FACT_VEHICLE_TELEMETRY;

-- ---------------------------------------------------------------------
-- Part B: parity with the Spark Gold output (temporary)
-- Rows returned = differences. Expected: no rows for boroughs and hours.
-- Hotspots can differ slightly: Snowflake drops coordinates outside NYC
-- (e.g. 0,0), which the Spark job still counts.
-- ---------------------------------------------------------------------

-- Boroughs
SELECT COALESCE(g.borough, s.borough) AS borough,
       g.collision_count AS spark_gold, s.collision_count AS snowflake
FROM (
  SELECT $1:borough::STRING AS borough, $1:collision_count::NUMBER AS collision_count
  FROM @AUTOPULSE.RAW.S3_GOLD/borough_summary/ (PATTERN => '.*[.]parquet')
) g
FULL OUTER JOIN VW_BOROUGH_SUMMARY s ON s.borough = g.borough
WHERE g.collision_count IS DISTINCT FROM s.collision_count;

-- Hours
SELECT COALESCE(g.crash_hour, s.crash_hour) AS crash_hour,
       g.collision_count AS spark_gold, s.collision_count AS snowflake
FROM (
  SELECT $1:crash_hour::NUMBER AS crash_hour, $1:collision_count::NUMBER AS collision_count
  FROM @AUTOPULSE.RAW.S3_GOLD/hourly_collisions/ (PATTERN => '.*[.]parquet')
) g
FULL OUTER JOIN VW_HOURLY_COLLISIONS s ON s.crash_hour = g.crash_hour
WHERE g.collision_count IS DISTINCT FROM s.collision_count
ORDER BY 1;

-- Fleet (maintenance and latest odometer per vehicle)
SELECT COALESCE(g.vehicle_id, s.vehicle_id) AS vehicle_id,
       g.latest_odometer_miles AS spark_odometer, s.latest_odometer_miles AS snowflake_odometer,
       g.maintenance_status    AS spark_status,   s.maintenance_status    AS snowflake_status
FROM (
  SELECT $1:vehicle_id::STRING             AS vehicle_id,
         $1:latest_odometer_miles::FLOAT   AS latest_odometer_miles,
         $1:maintenance_status::STRING     AS maintenance_status
  FROM @AUTOPULSE.RAW.S3_GOLD/vehicle_fleet_summary/ (PATTERN => '.*[.]parquet')
) g
FULL OUTER JOIN VW_FLEET_STATUS s ON s.vehicle_id = g.vehicle_id
WHERE ABS(COALESCE(g.latest_odometer_miles, -1) - COALESCE(s.latest_odometer_miles, -1)) > 0.01
   OR g.maintenance_status IS DISTINCT FROM s.maintenance_status;

-- ---------------------------------------------------------------------
-- Troubleshooting: how Snowflake sees the raw Silver fields
-- (useful if timestamps or numbers come through as NULL)
-- ---------------------------------------------------------------------
SELECT record:event_timestamp            AS event_timestamp_raw,
       TYPEOF(record:event_timestamp)    AS event_timestamp_type,
       record:ingested_at                AS ingested_at_raw,
       TYPEOF(record:ingested_at)        AS ingested_at_type
FROM AUTOPULSE.RAW.VEHICLE_TELEMETRY
LIMIT 5;

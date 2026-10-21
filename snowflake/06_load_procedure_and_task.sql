-- =====================================================================
-- AutoPulse | 06 Star-schema load procedure and scheduled task
-- Run as: AUTOPULSE_ENGINEER
--
-- Flow (one transaction; on any error everything rolls back and the
-- streams keep their rows for the next run):
--   1. Streams -> typed, de-duplicated staging tables (STG_*)
--   2. Dimensions: DIM_BOROUGH, DIM_LOCATION, DIM_VEHICLE
--   3. Facts:      FACT_COLLISION (upsert), FACT_VEHICLE_TELEMETRY (insert-only)
--
-- Re-loads are safe: Spark rewrites Silver on every run, so Snowpipe may
-- deliver the same collision or event again; MERGE on the natural keys
-- (collision_id, event_id) prevents duplicates.
--
-- Timestamps: Spark writes UTC. The procedure sets the session time zone
-- to UTC so they are read correctly, then derives NYC local time for
-- DIM_TIME (collision times are published in local time).
-- =====================================================================

USE ROLE AUTOPULSE_ENGINEER;
USE WAREHOUSE AUTOPULSE_WH;
USE SCHEMA AUTOPULSE.ANALYTICS;

CREATE OR REPLACE PROCEDURE SP_LOAD_STAR_SCHEMA()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
COMMENT = 'Loads new RAW rows (via streams) into the AutoPulse star schema'
AS
$$
DECLARE
  collisions_staged   INTEGER DEFAULT 0;
  telemetry_staged    INTEGER DEFAULT 0;
  collisions_merged   INTEGER DEFAULT 0;
  telemetry_inserted  INTEGER DEFAULT 0;
BEGIN
  -- Caller's-rights procedure, so this applies to the calling session.
  ALTER SESSION SET TIMEZONE = 'UTC';

  BEGIN TRANSACTION;

  DELETE FROM AUTOPULSE.RAW.STG_COLLISIONS;
  DELETE FROM AUTOPULSE.RAW.STG_VEHICLE_TELEMETRY;

  -- -------------------------------------------------------------------
  -- 1a. Collisions: stream -> staging (newest version per collision_id)
  -- -------------------------------------------------------------------
  INSERT INTO AUTOPULSE.RAW.STG_COLLISIONS (
    collision_id, crash_date, crash_ts, time_key, borough, borough_known,
    latitude, longitude, in_nyc_bounds, latitude_cell, longitude_cell,
    zip_code, on_street_name, cross_street_name, off_street_name,
    contributing_factor_1, contributing_factor_2, vehicle_type_1, vehicle_type_2,
    persons_injured, persons_killed, pedestrians_injured, pedestrians_killed,
    cyclists_injured, cyclists_killed, motorists_injured, motorists_killed,
    ingested_at, source_file
  )
  SELECT
    c.collision_id,
    c.crash_date,
    c.crash_ts,
    IFF(c.crash_ts IS NULL, -1, TO_NUMBER(TO_CHAR(c.crash_ts, 'YYYYMMDDHH24'))),
    c.borough,
    c.borough <> 'UNKNOWN',
    c.latitude,
    c.longitude,
    c.in_nyc_bounds,
    IFF(c.in_nyc_bounds, ROUND(c.latitude, 3)::NUMBER(9,3), NULL),
    IFF(c.in_nyc_bounds, ROUND(c.longitude, 3)::NUMBER(9,3), NULL),
    c.zip_code, c.on_street_name, c.cross_street_name, c.off_street_name,
    c.contributing_factor_1, c.contributing_factor_2, c.vehicle_type_1, c.vehicle_type_2,
    c.persons_injured, c.persons_killed, c.pedestrians_injured, c.pedestrians_killed,
    c.cyclists_injured, c.cyclists_killed, c.motorists_injured, c.motorists_killed,
    c.ingested_at,
    c.source_file
  FROM (
    SELECT
      TRY_TO_NUMBER(r.record:collision_id::STRING)                       AS collision_id,
      TRY_TO_DATE(COALESCE(r.record:crash_date_part::STRING,
                           LEFT(r.record:crash_date::STRING, 10)))       AS crash_date,
      -- crash_time is "H:mm" (e.g. "9:05"); pad to "09:05".
      TRY_TO_TIMESTAMP_NTZ(
        COALESCE(r.record:crash_date_part::STRING, LEFT(r.record:crash_date::STRING, 10))
          || ' ' || LPAD(TRIM(r.record:crash_time::STRING), 5, '0'),
        'YYYY-MM-DD HH24:MI')                                            AS crash_ts,
      COALESCE(NULLIF(UPPER(TRIM(r.record:borough::STRING)), ''), 'UNKNOWN') AS borough,
      TRY_TO_DOUBLE(r.record:latitude::STRING)                           AS latitude,
      TRY_TO_DOUBLE(r.record:longitude::STRING)                          AS longitude,
      -- NYC bounding box; excludes the 0,0 and other placeholder coordinates.
      COALESCE(
        (TRY_TO_DOUBLE(r.record:latitude::STRING)  BETWEEN 40.49 AND 40.92)
        AND (TRY_TO_DOUBLE(r.record:longitude::STRING) BETWEEN -74.27 AND -73.68),
        FALSE)                                                           AS in_nyc_bounds,
      NULLIF(TRIM(r.record:zip_code::STRING), '')                        AS zip_code,
      NULLIF(TRIM(r.record:on_street_name::STRING), '')                  AS on_street_name,
      NULLIF(TRIM(r.record:cross_street_name::STRING), '')               AS cross_street_name,
      NULLIF(TRIM(r.record:off_street_name::STRING), '')                 AS off_street_name,
      NULLIF(TRIM(r.record:contributing_factor_vehicle_1::STRING), '')   AS contributing_factor_1,
      NULLIF(TRIM(r.record:contributing_factor_vehicle_2::STRING), '')   AS contributing_factor_2,
      NULLIF(TRIM(r.record:vehicle_type_code1::STRING), '')              AS vehicle_type_1,
      NULLIF(TRIM(r.record:vehicle_type_code2::STRING), '')              AS vehicle_type_2,
      COALESCE(TRY_TO_NUMBER(r.record:number_of_persons_injured::STRING), 0)     AS persons_injured,
      COALESCE(TRY_TO_NUMBER(r.record:number_of_persons_killed::STRING), 0)      AS persons_killed,
      COALESCE(TRY_TO_NUMBER(r.record:number_of_pedestrians_injured::STRING), 0) AS pedestrians_injured,
      COALESCE(TRY_TO_NUMBER(r.record:number_of_pedestrians_killed::STRING), 0)  AS pedestrians_killed,
      COALESCE(TRY_TO_NUMBER(r.record:number_of_cyclist_injured::STRING), 0)     AS cyclists_injured,
      COALESCE(TRY_TO_NUMBER(r.record:number_of_cyclist_killed::STRING), 0)      AS cyclists_killed,
      COALESCE(TRY_TO_NUMBER(r.record:number_of_motorist_injured::STRING), 0)    AS motorists_injured,
      COALESCE(TRY_TO_NUMBER(r.record:number_of_motorist_killed::STRING), 0)     AS motorists_killed,
      CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(r.record:ingested_at::STRING))::TIMESTAMP_NTZ AS ingested_at,
      r.source_file,
      r.loaded_at
    FROM AUTOPULSE.RAW.COLLISIONS_STREAM r
  ) c
  WHERE c.collision_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.collision_id
    ORDER BY c.ingested_at DESC NULLS LAST, c.loaded_at DESC
  ) = 1;

  collisions_staged := SQLROWCOUNT;

  -- -------------------------------------------------------------------
  -- 1b. Telemetry: stream -> staging (one row per event_id)
  -- -------------------------------------------------------------------
  INSERT INTO AUTOPULSE.RAW.STG_VEHICLE_TELEMETRY (
    event_id, vehicle_id, event_ts_utc, event_ts_local, time_key,
    latitude, longitude, in_nyc_bounds, latitude_cell, longitude_cell,
    speed_mph, odometer_miles, fuel_level_pct, engine_status, is_simulated,
    kafka_partition, kafka_offset, ingested_at, source_file
  )
  SELECT
    t.event_id,
    t.vehicle_id,
    t.event_ts_utc,
    CONVERT_TIMEZONE('UTC', 'America/New_York', t.event_ts_utc),
    IFF(t.event_ts_utc IS NULL, -1,
        TO_NUMBER(TO_CHAR(CONVERT_TIMEZONE('UTC', 'America/New_York', t.event_ts_utc), 'YYYYMMDDHH24'))),
    t.latitude,
    t.longitude,
    t.in_nyc_bounds,
    IFF(t.in_nyc_bounds, ROUND(t.latitude, 3)::NUMBER(9,3), NULL),
    IFF(t.in_nyc_bounds, ROUND(t.longitude, 3)::NUMBER(9,3), NULL),
    t.speed_mph, t.odometer_miles, t.fuel_level_pct, t.engine_status, t.is_simulated,
    t.kafka_partition, t.kafka_offset, t.ingested_at, t.source_file
  FROM (
    SELECT
      TRIM(r.record:event_id::STRING)                                    AS event_id,
      UPPER(TRIM(r.record:vehicle_id::STRING))                           AS vehicle_id,
      CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(r.record:event_timestamp::STRING))::TIMESTAMP_NTZ AS event_ts_utc,
      TRY_TO_DOUBLE(r.record:latitude::STRING)                           AS latitude,
      TRY_TO_DOUBLE(r.record:longitude::STRING)                          AS longitude,
      COALESCE(
        (TRY_TO_DOUBLE(r.record:latitude::STRING)  BETWEEN 40.49 AND 40.92)
        AND (TRY_TO_DOUBLE(r.record:longitude::STRING) BETWEEN -74.27 AND -73.68),
        FALSE)                                                           AS in_nyc_bounds,
      TRY_TO_DOUBLE(r.record:speed_mph::STRING)                          AS speed_mph,
      TRY_TO_DOUBLE(r.record:odometer_miles::STRING)                     AS odometer_miles,
      TRY_TO_DOUBLE(r.record:fuel_level_pct::STRING)                     AS fuel_level_pct,
      LOWER(TRIM(r.record:engine_status::STRING))                        AS engine_status,
      TRY_TO_BOOLEAN(r.record:is_simulated::STRING)                      AS is_simulated,
      TRY_TO_NUMBER(r.record:kafka_partition::STRING)                    AS kafka_partition,
      TRY_TO_NUMBER(r.record:kafka_offset::STRING)                       AS kafka_offset,
      CONVERT_TIMEZONE('UTC', TRY_TO_TIMESTAMP_TZ(r.record:ingested_at::STRING))::TIMESTAMP_NTZ AS ingested_at,
      r.source_file,
      r.loaded_at
    FROM AUTOPULSE.RAW.VEHICLE_TELEMETRY_STREAM r
  ) t
  WHERE t.event_id IS NOT NULL AND t.event_id <> ''
    AND t.vehicle_id IS NOT NULL AND t.vehicle_id <> ''
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY t.event_id
    ORDER BY t.kafka_partition, t.kafka_offset, t.loaded_at
  ) = 1;

  telemetry_staged := SQLROWCOUNT;

  -- -------------------------------------------------------------------
  -- 2. Dimensions (before facts, so every fact row finds its keys)
  -- -------------------------------------------------------------------
  MERGE INTO AUTOPULSE.ANALYTICS.DIM_BOROUGH d
  USING (SELECT DISTINCT borough FROM AUTOPULSE.RAW.STG_COLLISIONS) s
  ON d.borough_name = s.borough
  WHEN NOT MATCHED THEN INSERT (borough_name, is_known)
    VALUES (s.borough, s.borough <> 'UNKNOWN');

  MERGE INTO AUTOPULSE.ANALYTICS.DIM_LOCATION d
  USING (
    SELECT latitude_cell, longitude_cell FROM AUTOPULSE.RAW.STG_COLLISIONS WHERE in_nyc_bounds
    UNION
    SELECT latitude_cell, longitude_cell FROM AUTOPULSE.RAW.STG_VEHICLE_TELEMETRY WHERE in_nyc_bounds
  ) s
  ON d.latitude_cell = s.latitude_cell AND d.longitude_cell = s.longitude_cell
  WHEN NOT MATCHED THEN INSERT (latitude_cell, longitude_cell, is_known)
    VALUES (s.latitude_cell, s.longitude_cell, TRUE);

  MERGE INTO AUTOPULSE.ANALYTICS.DIM_VEHICLE d
  USING (
    SELECT
      s.vehicle_id,
      MIN(s.event_ts_utc)                        AS first_seen,
      MAX(s.event_ts_utc)                        AS last_seen,
      BOOLOR_AGG(COALESCE(s.is_simulated, FALSE)) AS is_simulated,
      ANY_VALUE(r.vehicle_type)                  AS vehicle_type,
      ANY_VALUE(r.make)                          AS make,
      ANY_VALUE(r.model)                         AS model,
      ANY_VALUE(r.model_year)                    AS model_year,
      ANY_VALUE(r.home_depot)                    AS home_depot,
      ANY_VALUE(r.service_interval_miles)        AS service_interval_miles
    FROM AUTOPULSE.RAW.STG_VEHICLE_TELEMETRY s
    LEFT JOIN AUTOPULSE.ANALYTICS.REF_VEHICLE r
      ON r.vehicle_id = s.vehicle_id
    GROUP BY s.vehicle_id
  ) src
  ON d.vehicle_id = src.vehicle_id
  WHEN MATCHED THEN UPDATE SET
    d.vehicle_type           = COALESCE(src.vehicle_type, d.vehicle_type),
    d.make                   = COALESCE(src.make, d.make),
    d.model                  = COALESCE(src.model, d.model),
    d.model_year             = COALESCE(src.model_year, d.model_year),
    d.home_depot             = COALESCE(src.home_depot, d.home_depot),
    d.service_interval_miles = COALESCE(src.service_interval_miles, d.service_interval_miles),
    d.first_seen_at_utc      = LEAST(COALESCE(d.first_seen_at_utc, src.first_seen), COALESCE(src.first_seen, d.first_seen_at_utc)),
    d.last_seen_at_utc       = GREATEST(COALESCE(d.last_seen_at_utc, src.last_seen), COALESCE(src.last_seen, d.last_seen_at_utc))
  WHEN NOT MATCHED THEN INSERT (
    vehicle_id, vehicle_type, make, model, model_year, home_depot,
    service_interval_miles, is_simulated, first_seen_at_utc, last_seen_at_utc
  ) VALUES (
    src.vehicle_id,
    COALESCE(src.vehicle_type, 'UNKNOWN'),
    COALESCE(src.make, 'UNKNOWN'),
    COALESCE(src.model, 'UNKNOWN'),
    src.model_year,
    COALESCE(src.home_depot, 'UNKNOWN'),
    COALESCE(src.service_interval_miles, 5000),
    src.is_simulated,
    src.first_seen,
    src.last_seen
  );

  -- -------------------------------------------------------------------
  -- 3a. FACT_COLLISION: insert new collisions; update revised ones
  -- -------------------------------------------------------------------
  MERGE INTO AUTOPULSE.ANALYTICS.FACT_COLLISION f
  USING (
    SELECT
      s.*,
      COALESCE(t.time_key, -1)     AS dim_time_key,
      COALESCE(l.location_key, 0)  AS dim_location_key,
      COALESCE(b.borough_key, 0)   AS dim_borough_key
    FROM AUTOPULSE.RAW.STG_COLLISIONS s
    LEFT JOIN AUTOPULSE.ANALYTICS.DIM_TIME t
      ON t.time_key = s.time_key
    LEFT JOIN AUTOPULSE.ANALYTICS.DIM_LOCATION l
      ON s.in_nyc_bounds
     AND l.latitude_cell = s.latitude_cell
     AND l.longitude_cell = s.longitude_cell
    LEFT JOIN AUTOPULSE.ANALYTICS.DIM_BOROUGH b
      ON b.borough_name = s.borough
  ) src
  ON f.collision_id = src.collision_id
  -- Only rewrite a collision when a newer ingestion of it arrives.
  WHEN MATCHED AND (f.source_ingested_at IS NULL OR src.ingested_at > f.source_ingested_at) THEN UPDATE SET
    f.time_key              = src.dim_time_key,
    f.location_key          = src.dim_location_key,
    f.borough_key           = src.dim_borough_key,
    f.crash_ts              = src.crash_ts,
    f.crash_date            = src.crash_date,
    f.latitude              = src.latitude,
    f.longitude             = src.longitude,
    f.zip_code              = src.zip_code,
    f.on_street_name        = src.on_street_name,
    f.cross_street_name     = src.cross_street_name,
    f.off_street_name       = src.off_street_name,
    f.contributing_factor_1 = src.contributing_factor_1,
    f.contributing_factor_2 = src.contributing_factor_2,
    f.vehicle_type_1        = src.vehicle_type_1,
    f.vehicle_type_2        = src.vehicle_type_2,
    f.persons_injured       = src.persons_injured,
    f.persons_killed        = src.persons_killed,
    f.pedestrians_injured   = src.pedestrians_injured,
    f.pedestrians_killed    = src.pedestrians_killed,
    f.cyclists_injured      = src.cyclists_injured,
    f.cyclists_killed       = src.cyclists_killed,
    f.motorists_injured     = src.motorists_injured,
    f.motorists_killed      = src.motorists_killed,
    f.source_ingested_at    = src.ingested_at,
    f.updated_at            = CURRENT_TIMESTAMP()
  WHEN NOT MATCHED THEN INSERT (
    collision_id, time_key, location_key, borough_key, crash_ts, crash_date,
    latitude, longitude, zip_code, on_street_name, cross_street_name, off_street_name,
    contributing_factor_1, contributing_factor_2, vehicle_type_1, vehicle_type_2,
    collision_count, persons_injured, persons_killed, pedestrians_injured, pedestrians_killed,
    cyclists_injured, cyclists_killed, motorists_injured, motorists_killed,
    source_ingested_at, loaded_at, updated_at
  ) VALUES (
    src.collision_id, src.dim_time_key, src.dim_location_key, src.dim_borough_key, src.crash_ts, src.crash_date,
    src.latitude, src.longitude, src.zip_code, src.on_street_name, src.cross_street_name, src.off_street_name,
    src.contributing_factor_1, src.contributing_factor_2, src.vehicle_type_1, src.vehicle_type_2,
    1, src.persons_injured, src.persons_killed, src.pedestrians_injured, src.pedestrians_killed,
    src.cyclists_injured, src.cyclists_killed, src.motorists_injured, src.motorists_killed,
    src.ingested_at, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );

  collisions_merged := SQLROWCOUNT;

  -- -------------------------------------------------------------------
  -- 3b. FACT_VEHICLE_TELEMETRY: insert events not seen before
  -- -------------------------------------------------------------------
  MERGE INTO AUTOPULSE.ANALYTICS.FACT_VEHICLE_TELEMETRY f
  USING (
    SELECT
      s.*,
      v.vehicle_key,
      COALESCE(t.time_key, -1)     AS dim_time_key,
      COALESCE(l.location_key, 0)  AS dim_location_key
    FROM AUTOPULSE.RAW.STG_VEHICLE_TELEMETRY s
    JOIN AUTOPULSE.ANALYTICS.DIM_VEHICLE v
      ON v.vehicle_id = s.vehicle_id
    LEFT JOIN AUTOPULSE.ANALYTICS.DIM_TIME t
      ON t.time_key = s.time_key
    LEFT JOIN AUTOPULSE.ANALYTICS.DIM_LOCATION l
      ON s.in_nyc_bounds
     AND l.latitude_cell = s.latitude_cell
     AND l.longitude_cell = s.longitude_cell
  ) src
  ON f.event_id = src.event_id
  WHEN NOT MATCHED THEN INSERT (
    event_id, vehicle_key, time_key, location_key, event_ts_utc, event_ts_local,
    latitude, longitude, speed_mph, odometer_miles, fuel_level_pct, engine_status,
    is_simulated, kafka_partition, kafka_offset, loaded_at
  ) VALUES (
    src.event_id, src.vehicle_key, src.dim_time_key, src.dim_location_key, src.event_ts_utc, src.event_ts_local,
    src.latitude, src.longitude, src.speed_mph, src.odometer_miles, src.fuel_level_pct, src.engine_status,
    src.is_simulated, src.kafka_partition, src.kafka_offset, CURRENT_TIMESTAMP()
  );

  telemetry_inserted := SQLROWCOUNT;

  COMMIT;

  RETURN 'collisions staged=' || collisions_staged
      || ', collisions inserted/updated=' || collisions_merged
      || ', telemetry staged=' || telemetry_staged
      || ', telemetry inserted=' || telemetry_inserted;

EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    RAISE;
END;
$$;

-- ---------------------------------------------------------------------
-- First load: run it by hand once and check the returned counts.
-- ---------------------------------------------------------------------
CALL SP_LOAD_STAR_SCHEMA();

-- ---------------------------------------------------------------------
-- Scheduled task: every 5 minutes, but only when Snowpipe has delivered
-- new rows (the WHEN check costs nothing; the warehouse starts only if
-- there is work).
-- ---------------------------------------------------------------------
CREATE OR REPLACE TASK TASK_LOAD_STAR_SCHEMA
  WAREHOUSE = AUTOPULSE_WH
  SCHEDULE = '5 MINUTE'
  TIMEZONE = 'UTC'
  COMMENT = 'Loads new Snowpipe rows into the AutoPulse star schema'
  WHEN SYSTEM$STREAM_HAS_DATA('AUTOPULSE.RAW.COLLISIONS_STREAM')
    OR SYSTEM$STREAM_HAS_DATA('AUTOPULSE.RAW.VEHICLE_TELEMETRY_STREAM')
AS
  CALL AUTOPULSE.ANALYTICS.SP_LOAD_STAR_SCHEMA();

ALTER TASK TASK_LOAD_STAR_SCHEMA RESUME;

-- Task history (most recent first):
SELECT name, state, scheduled_time, completed_time, return_value, error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TASK_LOAD_STAR_SCHEMA'))
ORDER BY scheduled_time DESC
LIMIT 20;

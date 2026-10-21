-- =====================================================================
-- AutoPulse | 07 Analytics views (the Gold layer, in Snowflake)
-- Run as: AUTOPULSE_ENGINEER
--
-- These replace the Spark Gold jobs (transform_gold.py and
-- transform_vehicle_gold.py) and are what the dashboard will query.
-- Safe to re-run.
-- =====================================================================

USE ROLE AUTOPULSE_ENGINEER;
USE WAREHOUSE AUTOPULSE_WH;
USE SCHEMA AUTOPULSE.ANALYTICS;

-- ---------------------------------------------------------------------
-- Collisions by hour of day   (replaces gold/hourly_collisions)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_HOURLY_COLLISIONS
  COMMENT = 'Collisions, injuries and deaths by hour of day (local time)'
AS
SELECT
  t.hour_of_day            AS crash_hour,
  SUM(f.collision_count)   AS collision_count,
  SUM(f.persons_injured)   AS persons_injured,
  SUM(f.persons_killed)    AS persons_killed
FROM FACT_COLLISION f
JOIN DIM_TIME t ON t.time_key = f.time_key
WHERE f.time_key <> -1
GROUP BY t.hour_of_day;

-- ---------------------------------------------------------------------
-- Collisions by borough   (replaces gold/borough_summary)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_BOROUGH_SUMMARY
  COMMENT = 'Collisions by borough; excludes collisions with no borough (UNKNOWN)'
AS
SELECT
  b.borough_name           AS borough,
  SUM(f.collision_count)   AS collision_count,
  SUM(f.persons_injured)   AS persons_injured,
  SUM(f.persons_killed)    AS persons_killed,
  SUM(f.pedestrians_injured + f.cyclists_injured) AS vulnerable_road_users_injured
FROM FACT_COLLISION f
JOIN DIM_BOROUGH b ON b.borough_key = f.borough_key
WHERE b.is_known
GROUP BY b.borough_name;

-- ---------------------------------------------------------------------
-- Collision hotspots by grid cell   (replaces gold/geographic_hotspots)
-- Only coordinates inside NYC are mapped (location_key 0 = unknown).
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_COLLISION_HOTSPOTS
  COMMENT = 'Collision counts per ~110 m x 85 m grid cell, ranked'
AS
SELECT
  l.location_key,
  l.latitude_cell,
  l.longitude_cell,
  SUM(f.collision_count)   AS collision_count,
  SUM(f.persons_injured)   AS persons_injured,
  SUM(f.persons_killed)    AS persons_killed,
  RANK() OVER (ORDER BY SUM(f.collision_count) DESC) AS hotspot_rank
FROM FACT_COLLISION f
JOIN DIM_LOCATION l ON l.location_key = f.location_key
WHERE l.is_known
GROUP BY l.location_key, l.latitude_cell, l.longitude_cell;

-- ---------------------------------------------------------------------
-- Collisions by date (trend line for the dashboard)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_DAILY_COLLISIONS
  COMMENT = 'Collisions per day with weekday/weekend flag'
AS
SELECT
  t.calendar_date,
  t.day_name,
  t.is_weekend,
  SUM(f.collision_count)   AS collision_count,
  SUM(f.persons_injured)   AS persons_injured,
  SUM(f.persons_killed)    AS persons_killed
FROM FACT_COLLISION f
JOIN DIM_TIME t ON t.time_key = f.time_key
WHERE f.time_key <> -1
GROUP BY t.calendar_date, t.day_name, t.is_weekend;

-- ---------------------------------------------------------------------
-- Fleet status: one row per vehicle with its latest reading
-- (replaces gold/vehicle_fleet_summary)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_FLEET_STATUS
  COMMENT = 'Latest position and condition of each vehicle, plus maintenance outlook'
AS
WITH latest AS (
  SELECT
    f.*,
    ROW_NUMBER() OVER (
      PARTITION BY f.vehicle_key
      ORDER BY f.event_ts_utc DESC, f.kafka_partition DESC, f.kafka_offset DESC
    ) AS rn
  FROM FACT_VEHICLE_TELEMETRY f
),
stats AS (
  SELECT
    vehicle_key,
    COUNT(*)                                   AS telemetry_readings,
    MIN(event_ts_utc)                          AS first_seen_utc,
    MAX(event_ts_utc)                          AS last_seen_utc,
    ROUND(AVG(speed_mph), 2)                   AS average_speed_mph,
    ROUND(MAX(speed_mph), 2)                   AS maximum_speed_mph,
    ROUND(MAX(odometer_miles) - MIN(odometer_miles), 2) AS observed_distance_miles
  FROM FACT_VEHICLE_TELEMETRY
  GROUP BY vehicle_key
)
SELECT
  v.vehicle_id,
  v.vehicle_type,
  v.make,
  v.model,
  v.model_year,
  v.home_depot,
  s.telemetry_readings,
  s.first_seen_utc,
  s.last_seen_utc,
  DATEDIFF(
    'minute', s.last_seen_utc,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())::TIMESTAMP_NTZ
  )                                            AS minutes_since_last_report,
  -- Telemetry reaches Snowflake in batches (Glue every 15 min + Snowpipe +
  -- 5-min task), so allow 30 minutes before calling a vehicle offline.
  IFF(minutes_since_last_report <= 30, 'online', 'offline') AS connection_status,
  s.average_speed_mph,
  s.maximum_speed_mph,
  s.observed_distance_miles,
  l.latitude                                   AS latest_latitude,
  l.longitude                                  AS latest_longitude,
  l.speed_mph                                  AS latest_speed_mph,
  l.fuel_level_pct                             AS latest_fuel_level_pct,
  l.engine_status                              AS latest_engine_status,
  l.odometer_miles                             AS latest_odometer_miles,
  -- Same rule as the Spark job: miles to the next multiple of the
  -- service interval; "service_due" within 250 miles of it.
  ROUND(v.service_interval_miles - MOD(l.odometer_miles, v.service_interval_miles), 2) AS miles_until_service,
  IFF(miles_until_service <= 250, 'service_due', 'ok') AS maintenance_status,
  IFF(l.fuel_level_pct < 15, 'low_fuel', 'ok')  AS fuel_status
FROM latest l
JOIN stats s       ON s.vehicle_key = l.vehicle_key
JOIN DIM_VEHICLE v ON v.vehicle_key = l.vehicle_key
WHERE l.rn = 1;

-- ---------------------------------------------------------------------
-- Fleet exposure to collision hotspots (batch-stream integration)
--
-- Definition: a telemetry reading is "in a hotspot" when it falls in one
-- of the 100 grid cells (~110 m x 85 m) with the most collisions.
-- This measures where vehicles spend time relative to historically
-- dangerous locations. It does NOT mean a vehicle was near a specific
-- collision when it happened.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW VW_VEHICLE_HOTSPOT_EXPOSURE
  COMMENT = 'Share of each vehicle''s readings inside the top-100 collision hotspot cells'
AS
WITH hotspots AS (
  SELECT location_key, hotspot_rank
  FROM VW_COLLISION_HOTSPOTS
  WHERE hotspot_rank <= 100
)
SELECT
  v.vehicle_id,
  v.vehicle_type,
  v.home_depot,
  COUNT(*)                                          AS telemetry_readings,
  COUNT_IF(h.location_key IS NOT NULL)              AS readings_in_hotspots,
  ROUND(100 * readings_in_hotspots / NULLIF(telemetry_readings, 0), 1) AS pct_time_in_hotspots,
  COUNT(DISTINCT h.location_key)                    AS distinct_hotspots_visited,
  MAX(IFF(h.location_key IS NOT NULL, f.event_ts_utc, NULL)) AS last_hotspot_visit_utc
FROM FACT_VEHICLE_TELEMETRY f
JOIN DIM_VEHICLE v ON v.vehicle_key = f.vehicle_key
LEFT JOIN hotspots h ON h.location_key = f.location_key
GROUP BY v.vehicle_id, v.vehicle_type, v.home_depot;

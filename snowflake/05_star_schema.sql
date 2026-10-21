-- =====================================================================
-- AutoPulse | 05 Star schema (dimensions and facts)
-- Run as: AUTOPULSE_ENGINEER
--
--   DIM_TIME ──┬── FACT_VEHICLE_TELEMETRY ── DIM_VEHICLE
--              │          │
--   DIM_LOCATION ─────────┤
--              │          │
--              └── FACT_COLLISION ── DIM_BOROUGH
--
-- Grain
--   FACT_VEHICLE_TELEMETRY  one row per telemetry event (event_id)
--   FACT_COLLISION          one row per collision (collision_id)
--   DIM_TIME                one row per hour (time_key = YYYYMMDDHH, local NYC time)
--   DIM_LOCATION            one ~110 m x 85 m grid cell (lat/lon rounded to 3 decimals)
--   DIM_BOROUGH             one row per borough, plus UNKNOWN
--   DIM_VEHICLE             one row per fleet vehicle
--
-- Unknown members: DIM_TIME -1, DIM_LOCATION 0, DIM_BOROUGH 0 (UNKNOWN).
-- Primary/foreign keys are declared for documentation and BI tools;
-- Snowflake does not enforce them (the load procedure keeps them valid).
--
-- Safe to re-run.
-- =====================================================================

USE ROLE AUTOPULSE_ENGINEER;
USE WAREHOUSE AUTOPULSE_WH;
USE SCHEMA AUTOPULSE.ANALYTICS;

-- ---------------------------------------------------------------------
-- Reference data: fictional metadata for the simulated fleet
-- ---------------------------------------------------------------------

CREATE OR REPLACE TABLE REF_VEHICLE (
  vehicle_id             VARCHAR PRIMARY KEY,
  vehicle_type           VARCHAR,
  make                   VARCHAR,
  model                  VARCHAR,
  model_year             NUMBER(4),
  home_depot             VARCHAR,
  service_interval_miles NUMBER
)
COMMENT = 'Fictional reference data for the 10 simulated vehicles';

INSERT INTO REF_VEHICLE VALUES
  ('VEH-001', 'Cargo Van', 'Ford',          'Transit 250',     2023, 'Midtown Depot',          5000),
  ('VEH-002', 'Cargo Van', 'Ford',          'Transit 250',     2023, 'Midtown Depot',          5000),
  ('VEH-003', 'Cargo Van', 'Ram',           'ProMaster 2500',  2022, 'Midtown Depot',          5000),
  ('VEH-004', 'Cargo Van', 'Ram',           'ProMaster 2500',  2022, 'Long Island City Depot', 5000),
  ('VEH-005', 'Cargo Van', 'Mercedes-Benz', 'Sprinter 2500',   2024, 'Long Island City Depot', 5000),
  ('VEH-006', 'Cargo Van', 'Mercedes-Benz', 'Sprinter 2500',   2024, 'Long Island City Depot', 5000),
  ('VEH-007', 'Sedan',     'Toyota',        'Camry',           2023, 'Midtown Depot',          5000),
  ('VEH-008', 'Sedan',     'Toyota',        'Camry',           2023, 'Midtown Depot',          5000),
  ('VEH-009', 'Box Truck', 'Chevrolet',     'Express 3500',    2021, 'Red Hook Depot',         5000),
  ('VEH-010', 'Box Truck', 'Isuzu',         'NPR-HD',          2022, 'Red Hook Depot',         5000);

-- ---------------------------------------------------------------------
-- DIM_TIME (hourly, 2012-07-01 .. 2027-12-31; collisions data starts 2012-07)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS DIM_TIME (
  time_key      NUMBER        NOT NULL PRIMARY KEY  COMMENT 'YYYYMMDDHH, or -1 = unknown',
  hour_start    TIMESTAMP_NTZ,
  calendar_date DATE,
  hour_of_day   NUMBER(2),
  day_of_week   NUMBER(1)     COMMENT '1 = Monday ... 7 = Sunday',
  day_name      VARCHAR(3),
  is_weekend    BOOLEAN,
  week_of_year  NUMBER(2),
  month_number  NUMBER(2),
  month_name    VARCHAR(3),
  quarter       NUMBER(1),
  year          NUMBER(4)
)
COMMENT = 'Hour-grain time dimension, local NYC time';

MERGE INTO DIM_TIME d
USING (
  WITH hours AS (
    SELECT DATEADD(
             HOUR,
             ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1,
             '2012-07-01 00:00:00'::TIMESTAMP_NTZ
           ) AS hour_start
    FROM TABLE(GENERATOR(ROWCOUNT => 140000))
  )
  SELECT
    TO_NUMBER(TO_CHAR(hour_start, 'YYYYMMDDHH24')) AS time_key,
    hour_start,
    hour_start::DATE                               AS calendar_date,
    HOUR(hour_start)                               AS hour_of_day,
    DAYOFWEEKISO(hour_start)                       AS day_of_week,
    DAYNAME(hour_start)                            AS day_name,
    DAYOFWEEKISO(hour_start) >= 6                  AS is_weekend,
    WEEKISO(hour_start)                            AS week_of_year,
    MONTH(hour_start)                              AS month_number,
    MONTHNAME(hour_start)                          AS month_name,
    QUARTER(hour_start)                            AS quarter,
    YEAR(hour_start)                               AS year
  FROM hours
  WHERE hour_start < '2028-01-01 00:00:00'::TIMESTAMP_NTZ
  UNION ALL
  SELECT -1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
) s
ON d.time_key = s.time_key
WHEN NOT MATCHED THEN INSERT VALUES (
  s.time_key, s.hour_start, s.calendar_date, s.hour_of_day, s.day_of_week,
  s.day_name, s.is_weekend, s.week_of_year, s.month_number, s.month_name,
  s.quarter, s.year
);

-- ---------------------------------------------------------------------
-- DIM_BOROUGH
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS DIM_BOROUGH (
  -- Keys 0-5 are seeded below; any unexpected borough value gets 100+.
  borough_key   NUMBER AUTOINCREMENT START 100 INCREMENT 1 PRIMARY KEY,
  borough_name  VARCHAR NOT NULL UNIQUE,
  is_known      BOOLEAN
)
COMMENT = 'NYC boroughs; key 0 = UNKNOWN (collision had no borough)';

MERGE INTO DIM_BOROUGH d
USING (
  SELECT column1 AS borough_key, column2 AS borough_name, column3 AS is_known
  FROM VALUES
    (0, 'UNKNOWN',       FALSE),
    (1, 'BRONX',         TRUE),
    (2, 'BROOKLYN',      TRUE),
    (3, 'MANHATTAN',     TRUE),
    (4, 'QUEENS',        TRUE),
    (5, 'STATEN ISLAND', TRUE)
) s
ON d.borough_name = s.borough_name
WHEN NOT MATCHED THEN INSERT (borough_key, borough_name, is_known)
  VALUES (s.borough_key, s.borough_name, s.is_known);

-- ---------------------------------------------------------------------
-- DIM_LOCATION (grid cells; shared by collisions and vehicle telemetry,
-- which is what lets us relate vehicle positions to collision hotspots)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS DIM_LOCATION (
  location_key    NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
  latitude_cell   NUMBER(9,3),
  longitude_cell  NUMBER(9,3),
  is_known        BOOLEAN,
  UNIQUE (latitude_cell, longitude_cell)
)
COMMENT = 'Lat/lon rounded to 3 decimals (~110 m x 85 m in NYC); key 0 = missing or outside NYC';

MERGE INTO DIM_LOCATION d
USING (SELECT 0 AS location_key) s
ON d.location_key = s.location_key
WHEN NOT MATCHED THEN INSERT (location_key, latitude_cell, longitude_cell, is_known)
  VALUES (0, NULL, NULL, FALSE);

-- ---------------------------------------------------------------------
-- DIM_VEHICLE (type 1: attributes overwritten from REF_VEHICLE)
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS DIM_VEHICLE (
  vehicle_key             NUMBER AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
  vehicle_id              VARCHAR NOT NULL UNIQUE,
  vehicle_type            VARCHAR,
  make                    VARCHAR,
  model                   VARCHAR,
  model_year              NUMBER(4),
  home_depot              VARCHAR,
  service_interval_miles  NUMBER,
  is_simulated            BOOLEAN,
  first_seen_at_utc       TIMESTAMP_NTZ,
  last_seen_at_utc        TIMESTAMP_NTZ
)
COMMENT = 'Fleet vehicles; attributes from REF_VEHICLE, UNKNOWN if not listed there';

-- ---------------------------------------------------------------------
-- FACT_COLLISION
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FACT_COLLISION (
  collision_id          NUMBER NOT NULL PRIMARY KEY  COMMENT 'NYC collision ID (degenerate dimension)',
  time_key              NUMBER NOT NULL REFERENCES DIM_TIME (time_key),
  location_key          NUMBER NOT NULL REFERENCES DIM_LOCATION (location_key),
  borough_key           NUMBER NOT NULL REFERENCES DIM_BOROUGH (borough_key),
  crash_ts              TIMESTAMP_NTZ  COMMENT 'Local NYC time',
  crash_date            DATE,
  latitude              FLOAT,
  longitude             FLOAT,
  zip_code              VARCHAR,
  on_street_name        VARCHAR,
  cross_street_name     VARCHAR,
  off_street_name       VARCHAR,
  contributing_factor_1 VARCHAR,
  contributing_factor_2 VARCHAR,
  vehicle_type_1        VARCHAR,
  vehicle_type_2        VARCHAR,
  collision_count       NUMBER(1) DEFAULT 1,
  persons_injured       NUMBER,
  persons_killed        NUMBER,
  pedestrians_injured   NUMBER,
  pedestrians_killed    NUMBER,
  cyclists_injured      NUMBER,
  cyclists_killed       NUMBER,
  motorists_injured     NUMBER,
  motorists_killed      NUMBER,
  source_ingested_at    TIMESTAMP_NTZ  COMMENT 'When Spark ingested this version',
  loaded_at             TIMESTAMP_LTZ,
  updated_at            TIMESTAMP_LTZ
)
COMMENT = 'One row per NYC collision; updated in place when the city revises a record';

-- ---------------------------------------------------------------------
-- FACT_VEHICLE_TELEMETRY
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS FACT_VEHICLE_TELEMETRY (
  event_id              VARCHAR NOT NULL PRIMARY KEY  COMMENT 'Simulator event ID (degenerate dimension)',
  vehicle_key           NUMBER NOT NULL REFERENCES DIM_VEHICLE (vehicle_key),
  time_key              NUMBER NOT NULL REFERENCES DIM_TIME (time_key),
  location_key          NUMBER NOT NULL REFERENCES DIM_LOCATION (location_key),
  event_ts_utc          TIMESTAMP_NTZ,
  event_ts_local        TIMESTAMP_NTZ  COMMENT 'America/New_York',
  latitude              FLOAT,
  longitude             FLOAT,
  speed_mph             FLOAT,
  odometer_miles        FLOAT,
  fuel_level_pct        FLOAT,
  engine_status         VARCHAR,
  is_simulated          BOOLEAN,
  kafka_partition       NUMBER,
  kafka_offset          NUMBER,
  loaded_at             TIMESTAMP_LTZ
)
COMMENT = 'One row per vehicle telemetry event (insert-only)';

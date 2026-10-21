-- =====================================================================
-- AutoPulse | 03 Database, S3 stages, landing (RAW) tables, streams
-- Run as: AUTOPULSE_ENGINEER
--
-- Layout
--   AUTOPULSE.RAW        landing tables (one VARIANT row per Silver record),
--                        streams that track new rows, typed staging tables
--   AUTOPULSE.ANALYTICS  star schema (facts, dimensions) and views
--
-- Safe to re-run (IF NOT EXISTS everywhere; re-creating the RAW tables
-- would break their streams).
-- =====================================================================

USE ROLE AUTOPULSE_ENGINEER;
USE WAREHOUSE AUTOPULSE_WH;

CREATE DATABASE IF NOT EXISTS AUTOPULSE
  COMMENT = 'AutoPulse fleet and collision analytics';

CREATE SCHEMA IF NOT EXISTS AUTOPULSE.RAW
  COMMENT = 'Landing zone loaded by Snowpipe from S3 Silver';
CREATE SCHEMA IF NOT EXISTS AUTOPULSE.ANALYTICS
  COMMENT = 'Star schema and reporting views';

USE SCHEMA AUTOPULSE.RAW;

-- ---------------------------------------------------------------------
-- File format and stages
-- ---------------------------------------------------------------------

-- USE_LOGICAL_TYPE: read Parquet timestamps/decimals as real types.
CREATE FILE FORMAT IF NOT EXISTS FF_PARQUET
  TYPE = PARQUET
  USE_LOGICAL_TYPE = TRUE
  COMMENT = 'Spark-written Parquet';

CREATE STAGE IF NOT EXISTS S3_SILVER
  URL = 's3://autopulse-julio-leon/silver/'
  STORAGE_INTEGRATION = AUTOPULSE_S3_INT
  FILE_FORMAT = FF_PARQUET
  COMMENT = 'S3 Silver layer (source for Snowpipe)';

-- Used only by the parity checks in 08 while the Spark Gold jobs still exist.
CREATE STAGE IF NOT EXISTS S3_GOLD
  URL = 's3://autopulse-julio-leon/gold/'
  STORAGE_INTEGRATION = AUTOPULSE_S3_INT
  FILE_FORMAT = FF_PARQUET
  COMMENT = 'S3 Gold layer (Spark output, for parity checks only)';

-- Connectivity check: should list Parquet files under collisions/ and
-- vehicle_telemetry/. An "access denied" error means the AWS trust policy
-- (infra/aws/README.md step 7c) is not set up yet.
LIST @S3_SILVER PATTERN = '.*[.]parquet';

-- ---------------------------------------------------------------------
-- Landing tables: the whole Silver row is kept as VARIANT, so a new or
-- missing column in Silver never breaks loading. Typed extraction happens
-- in the load procedure (06).
-- ---------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS COLLISIONS (
  record           VARIANT        COMMENT 'One Silver collision row',
  source_file      VARCHAR        COMMENT 'S3 file the row came from',
  source_file_row  NUMBER,
  loaded_at        TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Snowpipe landing table for s3://.../silver/collisions/';

CREATE TABLE IF NOT EXISTS VEHICLE_TELEMETRY (
  record           VARIANT        COMMENT 'One Silver telemetry row',
  source_file      VARCHAR,
  source_file_row  NUMBER,
  loaded_at        TIMESTAMP_LTZ  DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Snowpipe landing table for s3://.../silver/vehicle_telemetry/';

-- Streams remember which landing rows the star-schema load has already
-- processed. Created before the pipes so no rows are missed.
CREATE STREAM IF NOT EXISTS COLLISIONS_STREAM
  ON TABLE COLLISIONS
  APPEND_ONLY = TRUE;

CREATE STREAM IF NOT EXISTS VEHICLE_TELEMETRY_STREAM
  ON TABLE VEHICLE_TELEMETRY
  APPEND_ONLY = TRUE;

-- ---------------------------------------------------------------------
-- Typed staging tables: each load run fills these from the streams,
-- de-duplicated, then merges them into the star schema.
-- ---------------------------------------------------------------------

CREATE TRANSIENT TABLE IF NOT EXISTS STG_COLLISIONS (
  collision_id          NUMBER,
  crash_date            DATE,
  crash_ts              TIMESTAMP_NTZ  COMMENT 'Local NYC time, as published',
  time_key              NUMBER         COMMENT 'YYYYMMDDHH of crash_ts',
  borough               VARCHAR,
  borough_known         BOOLEAN,
  latitude              FLOAT,
  longitude             FLOAT,
  in_nyc_bounds         BOOLEAN        COMMENT 'Coordinates inside the NYC bounding box',
  latitude_cell         NUMBER(9,3),
  longitude_cell        NUMBER(9,3),
  zip_code              VARCHAR,
  on_street_name        VARCHAR,
  cross_street_name     VARCHAR,
  off_street_name       VARCHAR,
  contributing_factor_1 VARCHAR,
  contributing_factor_2 VARCHAR,
  vehicle_type_1        VARCHAR,
  vehicle_type_2        VARCHAR,
  persons_injured       NUMBER,
  persons_killed        NUMBER,
  pedestrians_injured   NUMBER,
  pedestrians_killed    NUMBER,
  cyclists_injured      NUMBER,
  cyclists_killed       NUMBER,
  motorists_injured     NUMBER,
  motorists_killed      NUMBER,
  ingested_at           TIMESTAMP_NTZ,
  source_file           VARCHAR
);

CREATE TRANSIENT TABLE IF NOT EXISTS STG_VEHICLE_TELEMETRY (
  event_id              VARCHAR,
  vehicle_id            VARCHAR,
  event_ts_utc          TIMESTAMP_NTZ,
  event_ts_local        TIMESTAMP_NTZ  COMMENT 'America/New_York, to match collision times',
  time_key              NUMBER,
  latitude              FLOAT,
  longitude             FLOAT,
  in_nyc_bounds         BOOLEAN,
  latitude_cell         NUMBER(9,3),
  longitude_cell        NUMBER(9,3),
  speed_mph             FLOAT,
  odometer_miles        FLOAT,
  fuel_level_pct        FLOAT,
  engine_status         VARCHAR,
  is_simulated          BOOLEAN,
  kafka_partition       NUMBER,
  kafka_offset          NUMBER,
  ingested_at           TIMESTAMP_NTZ,
  source_file           VARCHAR
);

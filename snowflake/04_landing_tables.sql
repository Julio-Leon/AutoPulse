-- =====================================================================
-- AutoPulse - Snowflake step 4: landing tables (one per Gold table)
-- =====================================================================
--
-- Each table has the same columns as its Gold Parquet files, plus two that
-- Snowpipe fills in for every row:
--   SOURCE_FILE   the S3 file the row came from
--   LOADED_AT     when Snowpipe loaded it
-- These are what the CORE dynamic tables use to keep only the newest version
-- of each row (e.g. when a collisions date is re-ingested and its Gold file
-- replaced, the new file's rows win).
--
-- Timestamps: *_LOCAL columns are New York wall-clock time; the others are UTC.

USE ROLE SYSADMIN;
USE DATABASE AUTOPULSE;
USE SCHEMA LANDING;

-- gold/dim_date/: one row per calendar day ---------------------------------
CREATE TABLE IF NOT EXISTS DIM_DATE (
    DATE_KEY        NUMBER(8,0),        -- 20261022
    FULL_DATE       DATE,
    YEAR            NUMBER(4,0),
    QUARTER         NUMBER(1,0),
    MONTH           NUMBER(2,0),
    MONTH_NAME      VARCHAR,
    DAY_OF_MONTH    NUMBER(2,0),
    DAY_OF_WEEK     NUMBER(1,0),        -- 1 = Monday ... 7 = Sunday
    DAY_NAME        VARCHAR,
    IS_WEEKEND      BOOLEAN,
    WEEK_OF_YEAR    NUMBER(2,0),
    SOURCE_FILE     VARCHAR,
    LOADED_AT       TIMESTAMP_LTZ
);

-- gold/dim_location/: one row per ~500 m grid cell --------------------------
CREATE TABLE IF NOT EXISTS DIM_LOCATION (
    CELL_ID             VARCHAR,        -- R059-C048
    CELL_ROW            NUMBER(3,0),
    CELL_COLUMN         NUMBER(3,0),
    SOUTH_LATITUDE      FLOAT,
    NORTH_LATITUDE      FLOAT,
    WEST_LONGITUDE      FLOAT,
    EAST_LONGITUDE      FLOAT,
    CENTER_LATITUDE     FLOAT,
    CENTER_LONGITUDE    FLOAT,
    SOURCE_FILE         VARCHAR,
    LOADED_AT           TIMESTAMP_LTZ
);

-- gold/dim_vehicle/: one row per vehicle (file replaced every Gold run) -----
CREATE TABLE IF NOT EXISTS DIM_VEHICLE (
    VEHICLE_ID      VARCHAR,
    DRIVER_NAME     VARCHAR,
    MAKE            VARCHAR,
    MODEL           VARCHAR,
    MODEL_YEAR      NUMBER(4,0),
    FIRST_SEEN_AT   TIMESTAMP_NTZ,      -- UTC
    LAST_SEEN_AT    TIMESTAMP_NTZ,      -- UTC
    UPDATED_AT      TIMESTAMP_NTZ,      -- UTC
    SOURCE_FILE     VARCHAR,
    LOADED_AT       TIMESTAMP_LTZ
);

-- gold/fact_collision/: one row per collision ------------------------------
CREATE TABLE IF NOT EXISTS FACT_COLLISION (
    COLLISION_ID                    NUMBER(19,0),
    DATE_KEY                        NUMBER(8,0),
    CRASH_TIMESTAMP_LOCAL           TIMESTAMP_NTZ,
    CRASH_HOUR                      NUMBER(2,0),
    CELL_ID                         VARCHAR,
    BOROUGH                         VARCHAR,
    ZIP_CODE                        VARCHAR,
    LATITUDE                        FLOAT,
    LONGITUDE                       FLOAT,
    COORDINATES_VALID               BOOLEAN,
    ON_STREET_NAME                  VARCHAR,
    CROSS_STREET_NAME               VARCHAR,
    NUMBER_OF_PERSONS_INJURED       NUMBER(5,0),
    NUMBER_OF_PERSONS_KILLED        NUMBER(5,0),
    NUMBER_OF_PEDESTRIANS_INJURED   NUMBER(5,0),
    NUMBER_OF_PEDESTRIANS_KILLED    NUMBER(5,0),
    NUMBER_OF_CYCLIST_INJURED       NUMBER(5,0),
    NUMBER_OF_CYCLIST_KILLED        NUMBER(5,0),
    NUMBER_OF_MOTORIST_INJURED      NUMBER(5,0),
    NUMBER_OF_MOTORIST_KILLED       NUMBER(5,0),
    CONTRIBUTING_FACTOR             VARCHAR,
    VEHICLE_TYPE                    VARCHAR,
    SOURCE_FILE                     VARCHAR,
    LOADED_AT                       TIMESTAMP_LTZ
);

-- gold/fact_vehicle_telemetry/: one row per telemetry event ----------------
CREATE TABLE IF NOT EXISTS FACT_VEHICLE_TELEMETRY (
    EVENT_ID                VARCHAR,
    VEHICLE_ID              VARCHAR,
    DATE_KEY                NUMBER(8,0),
    EVENT_TIMESTAMP         TIMESTAMP_NTZ,      -- UTC
    EVENT_TIMESTAMP_LOCAL   TIMESTAMP_NTZ,      -- New York
    EVENT_HOUR_LOCAL        NUMBER(2,0),
    CELL_ID                 VARCHAR,
    LATITUDE                FLOAT,
    LONGITUDE               FLOAT,
    SPEED_MPH               FLOAT,
    IS_SPEEDING             BOOLEAN,
    ODOMETER_MILES          FLOAT,
    FUEL_LEVEL_PCT          FLOAT,
    ENGINE_STATUS           VARCHAR,
    SOURCE_FILE             VARCHAR,
    LOADED_AT               TIMESTAMP_LTZ
);

SHOW TABLES IN SCHEMA AUTOPULSE.LANDING;

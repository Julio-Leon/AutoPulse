-- =====================================================================
-- AutoPulse - Snowflake step 7: the star schema (CORE), as dynamic tables
-- =====================================================================
--
-- A DYNAMIC TABLE is a table defined by a query: Snowflake keeps its content
-- equal to the query's result, refreshing it automatically. TARGET_LAG says
-- how out of date it may get ("refresh at least every 5 minutes"). When the
-- landing table hasn't changed, a refresh does nothing and costs nothing, so
-- overnight and on weekends these use no compute.
--
-- Every table here is "the landing rows, newest version of each key only":
--     QUALIFY ROW_NUMBER() OVER (PARTITION BY <key> ORDER BY LOADED_AT DESC ...) = 1
-- e.g. a collision loaded twice (its date was re-ingested) appears once, with
-- the newer values.
--
--                       DIM_DATE (date_key)
--                           |
--   DIM_VEHICLE --- FACT_VEHICLE_TELEMETRY        FACT_COLLISION
--   (vehicle_id)            |  cell_id                  |  cell_id
--                           +------ DIM_LOCATION -------+

USE ROLE SYSADMIN;
USE DATABASE AUTOPULSE;
USE SCHEMA CORE;
USE WAREHOUSE AUTOPULSE_WH;

-- Dimensions ---------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE DIM_DATE
    TARGET_LAG = '1 day'
    WAREHOUSE = AUTOPULSE_WH
    COMMENT = 'One row per calendar day'
AS
SELECT DATE_KEY, FULL_DATE, YEAR, QUARTER, MONTH, MONTH_NAME,
       DAY_OF_MONTH, DAY_OF_WEEK, DAY_NAME, IS_WEEKEND, WEEK_OF_YEAR
FROM AUTOPULSE.LANDING.DIM_DATE
QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_KEY ORDER BY LOADED_AT DESC, SOURCE_FILE DESC) = 1;

CREATE OR REPLACE DYNAMIC TABLE DIM_LOCATION
    TARGET_LAG = '1 day'
    WAREHOUSE = AUTOPULSE_WH
    COMMENT = 'One row per ~500 m grid cell of New York City'
AS
SELECT CELL_ID, CELL_ROW, CELL_COLUMN,
       SOUTH_LATITUDE, NORTH_LATITUDE, WEST_LONGITUDE, EAST_LONGITUDE,
       CENTER_LATITUDE, CENTER_LONGITUDE
FROM AUTOPULSE.LANDING.DIM_LOCATION
QUALIFY ROW_NUMBER() OVER (PARTITION BY CELL_ID ORDER BY LOADED_AT DESC, SOURCE_FILE DESC) = 1;

CREATE OR REPLACE DYNAMIC TABLE DIM_VEHICLE
    TARGET_LAG = '10 minutes'
    WAREHOUSE = AUTOPULSE_WH
    COMMENT = 'One row per vehicle: driver and vehicle details'
AS
SELECT VEHICLE_ID, DRIVER_NAME, MAKE, MODEL, MODEL_YEAR,
       FIRST_SEEN_AT, LAST_SEEN_AT, UPDATED_AT
FROM AUTOPULSE.LANDING.DIM_VEHICLE
QUALIFY ROW_NUMBER() OVER (PARTITION BY VEHICLE_ID ORDER BY UPDATED_AT DESC, LOADED_AT DESC) = 1;

-- Facts --------------------------------------------------------------------

CREATE OR REPLACE DYNAMIC TABLE FACT_COLLISION
    TARGET_LAG = '1 hour'
    WAREHOUSE = AUTOPULSE_WH
    COMMENT = 'One row per NYC collision (latest version from the city)'
AS
SELECT COLLISION_ID, DATE_KEY, CRASH_TIMESTAMP_LOCAL, CRASH_HOUR, CELL_ID,
       BOROUGH, ZIP_CODE, LATITUDE, LONGITUDE, COORDINATES_VALID,
       ON_STREET_NAME, CROSS_STREET_NAME,
       NUMBER_OF_PERSONS_INJURED, NUMBER_OF_PERSONS_KILLED,
       NUMBER_OF_PEDESTRIANS_INJURED, NUMBER_OF_PEDESTRIANS_KILLED,
       NUMBER_OF_CYCLIST_INJURED, NUMBER_OF_CYCLIST_KILLED,
       NUMBER_OF_MOTORIST_INJURED, NUMBER_OF_MOTORIST_KILLED,
       CONTRIBUTING_FACTOR, VEHICLE_TYPE
FROM AUTOPULSE.LANDING.FACT_COLLISION
QUALIFY ROW_NUMBER() OVER (PARTITION BY COLLISION_ID ORDER BY LOADED_AT DESC, SOURCE_FILE DESC) = 1;

CREATE OR REPLACE DYNAMIC TABLE FACT_VEHICLE_TELEMETRY
    TARGET_LAG = '5 minutes'
    WAREHOUSE = AUTOPULSE_WH
    COMMENT = 'One row per vehicle telemetry event (last 30 days)'
AS
SELECT EVENT_ID, VEHICLE_ID, DATE_KEY, EVENT_TIMESTAMP, EVENT_TIMESTAMP_LOCAL,
       EVENT_HOUR_LOCAL, CELL_ID, LATITUDE, LONGITUDE, SPEED_MPH, IS_SPEEDING,
       ODOMETER_MILES, FUEL_LEVEL_PCT, ENGINE_STATUS
FROM AUTOPULSE.LANDING.FACT_VEHICLE_TELEMETRY
QUALIFY ROW_NUMBER() OVER (PARTITION BY EVENT_ID ORDER BY LOADED_AT DESC, SOURCE_FILE DESC) = 1;

SHOW DYNAMIC TABLES IN SCHEMA AUTOPULSE.CORE;

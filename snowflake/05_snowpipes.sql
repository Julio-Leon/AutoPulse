-- =====================================================================
-- AutoPulse - Snowflake step 5: one Snowpipe per Gold table
-- =====================================================================
--
-- A Snowpipe loads each new file the moment it lands in S3:
--   Glue writes a file in gold/<table>/  ->  S3 sends an event to Snowflake's
--   queue  ->  the pipe runs its COPY INTO for that one file (~1 minute).
-- AUTO_INGEST = TRUE is what makes it listen for those S3 events. The events
-- are switched on in AWS afterwards (snowflake/README.md, "S3 event
-- notification"), using the queue ARN shown by SHOW PIPES at the end.
--
-- Each COPY:
--   MATCH_BY_COLUMN_NAME   fills columns by name (Parquet column -> table column)
--   INCLUDE_METADATA       fills SOURCE_FILE and LOADED_AT
--   PATTERN                only .parquet files (skips any marker files)
-- Snowpipe remembers which files it has loaded and never loads the same file
-- twice. A *replaced* file has a new name, so it IS loaded: that's why the
-- CORE layer removes duplicates.
--
-- Snowpipe's compute is serverless (billed per file, not through AUTOPULSE_WH).

USE ROLE SYSADMIN;
USE DATABASE AUTOPULSE;
USE SCHEMA LANDING;

CREATE PIPE IF NOT EXISTS DIM_DATE_PIPE AUTO_INGEST = TRUE
    COMMENT = 'gold/dim_date/ -> LANDING.DIM_DATE'
AS
COPY INTO DIM_DATE
FROM @GOLD_STAGE/dim_date/
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (SOURCE_FILE = METADATA$FILENAME, LOADED_AT = METADATA$START_SCAN_TIME)
PATTERN = '.*[.]parquet';

CREATE PIPE IF NOT EXISTS DIM_LOCATION_PIPE AUTO_INGEST = TRUE
    COMMENT = 'gold/dim_location/ -> LANDING.DIM_LOCATION'
AS
COPY INTO DIM_LOCATION
FROM @GOLD_STAGE/dim_location/
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (SOURCE_FILE = METADATA$FILENAME, LOADED_AT = METADATA$START_SCAN_TIME)
PATTERN = '.*[.]parquet';

CREATE PIPE IF NOT EXISTS DIM_VEHICLE_PIPE AUTO_INGEST = TRUE
    COMMENT = 'gold/dim_vehicle/ -> LANDING.DIM_VEHICLE'
AS
COPY INTO DIM_VEHICLE
FROM @GOLD_STAGE/dim_vehicle/
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (SOURCE_FILE = METADATA$FILENAME, LOADED_AT = METADATA$START_SCAN_TIME)
PATTERN = '.*[.]parquet';

CREATE PIPE IF NOT EXISTS FACT_COLLISION_PIPE AUTO_INGEST = TRUE
    COMMENT = 'gold/fact_collision/ -> LANDING.FACT_COLLISION'
AS
COPY INTO FACT_COLLISION
FROM @GOLD_STAGE/fact_collision/
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (SOURCE_FILE = METADATA$FILENAME, LOADED_AT = METADATA$START_SCAN_TIME)
PATTERN = '.*[.]parquet';

CREATE PIPE IF NOT EXISTS FACT_VEHICLE_TELEMETRY_PIPE AUTO_INGEST = TRUE
    COMMENT = 'gold/fact_vehicle_telemetry/ -> LANDING.FACT_VEHICLE_TELEMETRY'
AS
COPY INTO FACT_VEHICLE_TELEMETRY
FROM @GOLD_STAGE/fact_vehicle_telemetry/
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FORMAT')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
INCLUDE_METADATA = (SOURCE_FILE = METADATA$FILENAME, LOADED_AT = METADATA$START_SCAN_TIME)
PATTERN = '.*[.]parquet';

-- The notification_channel column: the SQS queue ARN for the S3 event
-- notification. It is the same for all five pipes; copy it once.
SHOW PIPES IN SCHEMA AUTOPULSE.LANDING;

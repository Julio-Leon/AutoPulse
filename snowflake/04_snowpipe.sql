-- =====================================================================
-- AutoPulse | 04 Snowpipe (auto-ingest from S3 Silver)
-- Run as: AUTOPULSE_ENGINEER
--
-- Each pipe copies every new Parquet file that Spark writes under
-- silver/<dataset>/ into the matching RAW landing table.
--
-- AFTER running: copy the `notification_channel` value (an SQS ARN) from
-- SHOW PIPES and create the S3 event notification (infra/aws/README.md
-- step 8). Both pipes share the same SQS queue, so one notification is enough.
-- =====================================================================

USE ROLE AUTOPULSE_ENGINEER;
USE WAREHOUSE AUTOPULSE_WH;
USE SCHEMA AUTOPULSE.RAW;

CREATE PIPE IF NOT EXISTS PIPE_COLLISIONS
  AUTO_INGEST = TRUE
  COMMENT = 'S3 silver/collisions/ -> RAW.COLLISIONS'
AS
COPY INTO COLLISIONS (record, source_file, source_file_row, loaded_at)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    CURRENT_TIMESTAMP()
  FROM @S3_SILVER/collisions/
)
PATTERN = '.*[.]parquet';

CREATE PIPE IF NOT EXISTS PIPE_VEHICLE_TELEMETRY
  AUTO_INGEST = TRUE
  COMMENT = 'S3 silver/vehicle_telemetry/ -> RAW.VEHICLE_TELEMETRY'
AS
COPY INTO VEHICLE_TELEMETRY (record, source_file, source_file_row, loaded_at)
FROM (
  SELECT
    $1,
    METADATA$FILENAME,
    METADATA$FILE_ROW_NUMBER,
    CURRENT_TIMESTAMP()
  FROM @S3_SILVER/vehicle_telemetry/
)
PATTERN = '.*[.]parquet';

-- The SQS ARN for the S3 event notification is in `notification_channel`.
SHOW PIPES IN SCHEMA AUTOPULSE.RAW;

-- Files already in S3 before the pipes existed are not auto-ingested.
-- REFRESH queues files staged in the last 7 days.
ALTER PIPE PIPE_COLLISIONS REFRESH;
ALTER PIPE PIPE_VEHICLE_TELEMETRY REFRESH;

-- Check progress (executionState should be RUNNING; pendingFileCount -> 0):
SELECT SYSTEM$PIPE_STATUS('AUTOPULSE.RAW.PIPE_COLLISIONS');
SELECT SYSTEM$PIPE_STATUS('AUTOPULSE.RAW.PIPE_VEHICLE_TELEMETRY');

-- After a minute or two, confirm rows arrived:
SELECT 'collisions' AS landing_table, COUNT(*) AS rows_loaded FROM COLLISIONS
UNION ALL
SELECT 'vehicle_telemetry', COUNT(*) FROM VEHICLE_TELEMETRY;

-- =====================================================================
-- AutoPulse - Snowflake step 3: the stage (Snowflake's view of gold/)
-- Run AFTER the IAM role AutoPulseSnowflakeRole exists in AWS.
-- =====================================================================
--
-- PARQUET_FORMAT   how to read the files. USE_LOGICAL_TYPE = TRUE makes
--                  Snowflake read Parquet dates and timestamps as dates and
--                  timestamps (not as raw numbers).
-- GOLD_STAGE       s3://autopulse-julio-leon/gold/ as seen by Snowflake,
--                  through the storage integration from step 2.

USE ROLE SYSADMIN;
USE DATABASE AUTOPULSE;
USE SCHEMA LANDING;

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT
    TYPE = PARQUET
    USE_LOGICAL_TYPE = TRUE
    COMMENT = 'Parquet files written by the Gold Glue jobs';

CREATE STAGE IF NOT EXISTS GOLD_STAGE
    URL = 's3://autopulse-julio-leon/gold/'
    STORAGE_INTEGRATION = AUTOPULSE_S3_INTEGRATION
    FILE_FORMAT = PARQUET_FORMAT
    COMMENT = 'The Gold layer in S3';

-- Test the connection: lists the Gold files (an error here means the IAM
-- role or its trust policy is not right yet; an empty result just means no
-- Gold files exist yet).
LIST @GOLD_STAGE;

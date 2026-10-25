-- =====================================================================
-- AutoPulse - Snowflake step 2: connect Snowflake to the S3 bucket
-- Run as ACCOUNTADMIN. Run ONCE (see the warning below).
-- =====================================================================
--
-- A STORAGE INTEGRATION lets Snowflake read the bucket without any AWS keys:
-- Snowflake has its own AWS identity, and we allow that identity to use the
-- IAM role AutoPulseSnowflakeRole (read-only on gold/).
--
-- The handshake:
--   1. This script creates the integration, pointing at the role's ARN.
--   2. DESC INTEGRATION (last line) shows two values Snowflake generated:
--        STORAGE_AWS_IAM_USER_ARN   Snowflake's AWS identity
--        STORAGE_AWS_EXTERNAL_ID    a shared secret
--   3. Put them in policies/snowflake_s3_access/trust_policy.json and create
--      the IAM role with it (see snowflake/README.md).
--
-- WARNING: do not use CREATE OR REPLACE on the integration later. Replacing it
-- generates a NEW external ID and breaks the role's trust policy.

USE ROLE ACCOUNTADMIN;

CREATE STORAGE INTEGRATION IF NOT EXISTS AUTOPULSE_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::866934333672:role/AutoPulseSnowflakeRole'
    STORAGE_ALLOWED_LOCATIONS = ('s3://autopulse-julio-leon/gold/')
    COMMENT = 'AutoPulse: read-only access to the Gold layer';

-- SYSADMIN owns the AutoPulse objects: let it use the integration and run tasks.
GRANT USAGE ON INTEGRATION AUTOPULSE_S3_INTEGRATION TO ROLE SYSADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

-- All timestamps in the pipeline are UTC; make Snowflake use UTC too, so
-- loading and comparing them never shifts them by the account's default time zone.
ALTER ACCOUNT SET TIMEZONE = 'UTC';

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from the result.
DESC INTEGRATION AUTOPULSE_S3_INTEGRATION;

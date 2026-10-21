-- =====================================================================
-- AutoPulse | 02 Storage integration (Snowflake <-> S3 trust)
-- Run as: ACCOUNTADMIN
--
-- BEFORE running: create the AWS role AutoPulseSnowflakeRole
-- (infra/aws/README.md, step 7a) and paste its ARN below.
-- AFTER running: copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- from the DESC output into the role's trust policy (step 7c).
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- NOTE: IF NOT EXISTS keeps the external ID stable when re-run.
-- If you ran this with a wrong ARN, fix it without changing the external ID:
--   ALTER STORAGE INTEGRATION AUTOPULSE_S3_INT
--     SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/AutoPulseSnowflakeRole';
CREATE STORAGE INTEGRATION IF NOT EXISTS AUTOPULSE_S3_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<AWS_ACCOUNT_ID>:role/AutoPulseSnowflakeRole'   -- <-- replace
  STORAGE_ALLOWED_LOCATIONS = (
    's3://autopulse-julio-leon/silver/',
    's3://autopulse-julio-leon/gold/'
  )
  COMMENT = 'Read-only access to the AutoPulse data lake (silver, gold)';

GRANT USAGE ON INTEGRATION AUTOPULSE_S3_INT TO ROLE AUTOPULSE_ENGINEER;

-- Copy these two values into infra/aws/iam/autopulse-snowflake-trust-policy.json:
--   STORAGE_AWS_IAM_USER_ARN  -> "Principal": {"AWS": ...}
--   STORAGE_AWS_EXTERNAL_ID   -> "sts:ExternalId"
DESC INTEGRATION AUTOPULSE_S3_INT;

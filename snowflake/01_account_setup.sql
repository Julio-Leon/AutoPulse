-- =====================================================================
-- AutoPulse | 01 Account setup
-- Run as: ACCOUNTADMIN   (Snowsight > Projects > Worksheets)
-- Creates the project role and a small, auto-suspending warehouse.
-- Safe to re-run.
-- =====================================================================

USE ROLE ACCOUNTADMIN;

-- Project role that will own every AutoPulse object.
CREATE ROLE IF NOT EXISTS AUTOPULSE_ENGINEER
  COMMENT = 'Owns AutoPulse database objects, pipes and tasks';

GRANT ROLE AUTOPULSE_ENGINEER TO ROLE SYSADMIN;

-- Give the role to the user running this script.
SET current_user_name = CURRENT_USER();
GRANT ROLE AUTOPULSE_ENGINEER TO USER IDENTIFIER($current_user_name);

-- Extra-small warehouse; suspends after 60 s idle to save trial credits.
CREATE WAREHOUSE IF NOT EXISTS AUTOPULSE_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'AutoPulse loads, tasks and dashboard queries';

GRANT USAGE, OPERATE, MONITOR ON WAREHOUSE AUTOPULSE_WH TO ROLE AUTOPULSE_ENGINEER;

-- Let the project role create its own database and run scheduled tasks.
GRANT CREATE DATABASE ON ACCOUNT TO ROLE AUTOPULSE_ENGINEER;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE AUTOPULSE_ENGINEER;

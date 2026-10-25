-- =====================================================================
-- AutoPulse - Snowflake step 1: warehouse, database and schemas
-- Run in a Snowsight worksheet. Safe to run again (IF NOT EXISTS).
-- =====================================================================
--
-- AUTOPULSE_WH          the compute that loads and refreshes data.
--                       X-Small is the smallest size; it switches itself off
--                       after 60 seconds without work, so it only costs credits
--                       while something is actually running.
-- AUTOPULSE             the database, with three schemas (layers):
--   LANDING             raw copies of the Gold files, loaded by Snowpipe.
--                       Append-only: a file loaded twice means rows twice.
--   CORE                the star schema (facts + dimensions) as DYNAMIC TABLES:
--                       the LANDING rows with duplicates removed, kept up to
--                       date by Snowflake automatically.
--   ANALYTICS           views for the dashboard (fleet map, hotspots, vehicles).

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS AUTOPULSE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'AutoPulse: Snowpipe-adjacent work, dynamic table refreshes, dashboard queries';

CREATE DATABASE IF NOT EXISTS AUTOPULSE
    COMMENT = 'AutoPulse fleet management: vehicle telemetry + NYC collisions';

CREATE SCHEMA IF NOT EXISTS AUTOPULSE.LANDING
    COMMENT = 'Raw Gold files from S3, loaded by Snowpipe (append-only)';
CREATE SCHEMA IF NOT EXISTS AUTOPULSE.CORE
    COMMENT = 'Star schema: deduplicated facts and dimensions (dynamic tables)';
CREATE SCHEMA IF NOT EXISTS AUTOPULSE.ANALYTICS
    COMMENT = 'Dashboard views: fleet map, collision hotspots, vehicle summaries';

SHOW SCHEMAS IN DATABASE AUTOPULSE;

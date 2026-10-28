-- =====================================================================
-- AutoPulse - Snowflake step 10: a read-only user for Airflow's health checks
-- Run as ACCOUNTADMIN, AFTER setup_airflow_host.sh has printed the public key.
-- =====================================================================
--
-- AUTOPULSE_MONITOR   a role that can only READ: the LANDING tables, the CORE
--                     dynamic tables, the ANALYTICS views, and the status of the
--                     pipes and dynamic tables. It can't change any data.
-- AUTOPULSE_AIRFLOW   a SERVICE user (a program, not a person: no password, no
--                     web login) that signs in with a key pair. The private key
--                     never leaves the Airflow host; Snowflake only gets the
--                     public half, pasted below.
--
-- The Airflow DAG autopulse_pipeline_health uses it to check Snowpipe loads,
-- dynamic table refreshes, telemetry freshness and data quality.

USE ROLE ACCOUNTADMIN;

-- The role ----------------------------------------------------------------
CREATE ROLE IF NOT EXISTS AUTOPULSE_MONITOR
    COMMENT = 'Read-only monitoring of the AutoPulse pipeline (used by Airflow)';

GRANT USAGE ON WAREHOUSE AUTOPULSE_WH TO ROLE AUTOPULSE_MONITOR;
GRANT USAGE ON DATABASE AUTOPULSE TO ROLE AUTOPULSE_MONITOR;
GRANT USAGE ON ALL SCHEMAS IN DATABASE AUTOPULSE TO ROLE AUTOPULSE_MONITOR;

GRANT SELECT ON ALL TABLES IN SCHEMA AUTOPULSE.LANDING TO ROLE AUTOPULSE_MONITOR;
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA AUTOPULSE.CORE TO ROLE AUTOPULSE_MONITOR;
GRANT SELECT ON ALL VIEWS IN SCHEMA AUTOPULSE.ANALYTICS TO ROLE AUTOPULSE_MONITOR;

-- Status of the pipes (load history) and dynamic tables (refresh history).
GRANT MONITOR ON PIPE AUTOPULSE.LANDING.DIM_DATE_PIPE TO ROLE AUTOPULSE_MONITOR;
GRANT MONITOR ON PIPE AUTOPULSE.LANDING.DIM_LOCATION_PIPE TO ROLE AUTOPULSE_MONITOR;
GRANT MONITOR ON PIPE AUTOPULSE.LANDING.DIM_VEHICLE_PIPE TO ROLE AUTOPULSE_MONITOR;
GRANT MONITOR ON PIPE AUTOPULSE.LANDING.FACT_COLLISION_PIPE TO ROLE AUTOPULSE_MONITOR;
GRANT MONITOR ON PIPE AUTOPULSE.LANDING.FACT_VEHICLE_TELEMETRY_PIPE TO ROLE AUTOPULSE_MONITOR;

GRANT MONITOR ON DYNAMIC TABLE AUTOPULSE.CORE.DIM_DATE TO ROLE AUTOPULSE_MONITOR;
GRANT MONITOR ON DYNAMIC TABLE AUTOPULSE.CORE.DIM_LOCATION TO ROLE AUTOPULSE_MONITOR;
GRANT MONITOR ON DYNAMIC TABLE AUTOPULSE.CORE.DIM_VEHICLE TO ROLE AUTOPULSE_MONITOR;
GRANT MONITOR ON DYNAMIC TABLE AUTOPULSE.CORE.FACT_COLLISION TO ROLE AUTOPULSE_MONITOR;
GRANT MONITOR ON DYNAMIC TABLE AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY TO ROLE AUTOPULSE_MONITOR;

-- The user ----------------------------------------------------------------
-- Replace the value below with the key printed at the end of setup_airflow_host.sh
-- (one long line of letters, without the -----BEGIN/END----- lines).
CREATE USER IF NOT EXISTS AUTOPULSE_AIRFLOW
    TYPE = SERVICE
    DEFAULT_ROLE = AUTOPULSE_MONITOR
    DEFAULT_WAREHOUSE = AUTOPULSE_WH
    COMMENT = 'Airflow (EC2 autopulse-airflow-host): pipeline health checks';

ALTER USER AUTOPULSE_AIRFLOW SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5ogeRcrXpK3/sZPXBP7WIHxqzPfYuLg0esIA9ujigVKwdI1IqE8TjIrom5zDbY4LOF2hL27zOtqrX1vuqipfzPdzmBFL74+mHQ6umnzCuaBxf3/KZgUl8DKeN1czD8oa+EYfJPjOdX3yaxJtw/wzcL9ombpHW2NacW93XByc6V81nnFpkEpAsdrEkIuUCrr6EKypA7jNdX20QQDS4dW9imnwrnv49k/2r4xUM6nK53u54XfrDIxKIisEewRp/Yb8zSSeAJl62VxoxfhKWsek4MJ6M+GxUan9FE7T+REvxRx3aJkwE3apEJlYvh4CC/GTEIVCU4OHvFtL+AdCo+6ciwIDAQAB';

GRANT ROLE AUTOPULSE_MONITOR TO USER AUTOPULSE_AIRFLOW;

-- Check: RSA_PUBLIC_KEY_FP should show a fingerprint (SHA256:...).
DESC USER AUTOPULSE_AIRFLOW;

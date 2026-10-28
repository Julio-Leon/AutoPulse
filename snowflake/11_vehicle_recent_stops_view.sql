-- =====================================================================
-- AutoPulse - Snowflake step 11: each vehicle's recent stops (ANALYTICS)
-- =====================================================================
--
--   VW_VEHICLE_RECENT_STOPS   every delivery stop of each vehicle's latest
--                             driving day ("today"; after 5 PM and on weekends:
--                             the last day it drove), numbered newest first
--                             (STOPS_AGO 1 = most recent). The dashboard shows
--                             either the last 5 or all of today's.
--   VW_VEHICLE_RECENT_PATH    each vehicle's positions on that same day. The
--                             dashboard draws the part that covers the stops it
--                             shows (at least the last 2 hours), so every stop
--                             marker sits on its route.
--
-- What counts as a stop: the simulator turns the engine OFF when a vehicle
-- parks at its destination (a delivery, 2-8 minutes) and back on when it
-- leaves. Red lights are not stops: the engine stays on ("idling") there.
-- So a stop = an unbroken series of events with ENGINE_STATUS = 'off'.
--
-- How the SQL finds them ("gaps and islands"):
--   1. For every event, look at the vehicle's previous event (LAG).
--   2. An 'off' event starts a NEW stop when the previous event was not
--      'off', or came more than 30 minutes earlier (e.g. overnight).
--   3. A running count of those starts numbers each stop, so all events of
--      one stop share a number; GROUP BY that number = one row per stop.
--
-- Only the last 4 days are searched (enough to reach Friday from Monday
-- morning), which keeps the view fast.
--
-- Run as SYSADMIN, after 09_analytics_views.sql. Safe to run again.
-- (The route view used to be in 09; it now lives here, after the stops view it uses.)

USE ROLE SYSADMIN;
USE DATABASE AUTOPULSE;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE VIEW VW_VEHICLE_RECENT_STOPS
    COMMENT = 'Each vehicle''s delivery stops (engine off) on its latest driving day, newest first'
AS
WITH events AS (
    SELECT
        VEHICLE_ID, EVENT_TIMESTAMP, EVENT_TIMESTAMP_LOCAL, LATITUDE, LONGITUDE, CELL_ID,
        ENGINE_STATUS = 'off'                                                    AS IS_PARKED,
        LAG(ENGINE_STATUS = 'off') OVER (PARTITION BY VEHICLE_ID ORDER BY EVENT_TIMESTAMP) AS WAS_PARKED,
        LAG(EVENT_TIMESTAMP)       OVER (PARTITION BY VEHICLE_ID ORDER BY EVENT_TIMESTAMP) AS PREVIOUS_EVENT,
        MAX(EVENT_TIMESTAMP)       OVER (PARTITION BY VEHICLE_ID)                         AS VEHICLE_LAST_EVENT,
        MAX(EVENT_TIMESTAMP_LOCAL) OVER (PARTITION BY VEHICLE_ID)                         AS VEHICLE_LAST_EVENT_LOCAL
    FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY
    WHERE EVENT_TIMESTAMP >= DATEADD(day, -4, SYSDATE())
),
parked_events AS (
    SELECT
        *,
        -- 1 when this event starts a new stop, summed up = the stop's number
        SUM(IFF(NOT COALESCE(WAS_PARKED, FALSE)
                OR DATEDIFF(minute, PREVIOUS_EVENT, EVENT_TIMESTAMP) > 30, 1, 0))
            OVER (PARTITION BY VEHICLE_ID ORDER BY EVENT_TIMESTAMP
                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)              AS STOP_NUMBER
    FROM events
    WHERE IS_PARKED
),
stops AS (
    SELECT
        VEHICLE_ID,
        STOP_NUMBER,
        MIN(EVENT_TIMESTAMP_LOCAL)                                     AS ARRIVED_LOCAL,
        MAX(EVENT_TIMESTAMP_LOCAL)                                     AS LAST_PARKED_LOCAL,
        -- still parked: the stop's last event is the vehicle's newest event
        MAX(EVENT_TIMESTAMP) = MAX(VEHICLE_LAST_EVENT)                 AS IS_PARKED_NOW,
        -- each event stands for 5 seconds
        ROUND((DATEDIFF(second, MIN(EVENT_TIMESTAMP), MAX(EVENT_TIMESTAMP)) + 5) / 60, 1) AS MINUTES_PARKED,
        MIN_BY(LATITUDE, EVENT_TIMESTAMP)                              AS LATITUDE,
        MIN_BY(LONGITUDE, EVENT_TIMESTAMP)                             AS LONGITUDE,
        MIN_BY(CELL_ID, EVENT_TIMESTAMP)                               AS CELL_ID,
        MAX(VEHICLE_LAST_EVENT_LOCAL)                                  AS VEHICLE_LAST_EVENT_LOCAL
    FROM parked_events
    GROUP BY VEHICLE_ID, STOP_NUMBER
)
SELECT
    s.VEHICLE_ID,
    v.DRIVER_NAME,
    ROW_NUMBER() OVER (PARTITION BY s.VEHICLE_ID ORDER BY s.ARRIVED_LOCAL DESC) AS STOPS_AGO,   -- 1 = most recent
    s.ARRIVED_LOCAL,
    IFF(s.IS_PARKED_NOW, NULL, s.LAST_PARKED_LOCAL)                AS DEPARTED_LOCAL,          -- empty while parked
    s.IS_PARKED_NOW,
    s.MINUTES_PARKED,
    s.LATITUDE, s.LONGITUDE, s.CELL_ID,
    h.HOTSPOT_RANK                                                  -- empty if no collisions in that cell
FROM stops s
LEFT JOIN AUTOPULSE.CORE.DIM_VEHICLE v ON v.VEHICLE_ID = s.VEHICLE_ID
LEFT JOIN VW_COLLISION_HOTSPOTS h ON h.CELL_ID = s.CELL_ID
-- only stops made on the vehicle's latest driving day (New York date)
WHERE TO_DATE(s.ARRIVED_LOCAL) = TO_DATE(s.VEHICLE_LAST_EVENT_LOCAL);

-- ---------------------------------------------------------------------
-- Each vehicle's route on its latest driving day (replaces the 2-hour version from file 09)
-- ---------------------------------------------------------------------
-- All positions since the start of the day of the vehicle's latest event, so
-- yesterday's driving is never drawn. The dashboard cuts it down to the part
-- that covers the stops it shows.
CREATE OR REPLACE VIEW VW_VEHICLE_RECENT_PATH
    COMMENT = 'Each vehicle''s positions on its latest driving day, in order'
AS
WITH with_latest AS (
    SELECT t.*, MAX(t.EVENT_TIMESTAMP_LOCAL) OVER (PARTITION BY t.VEHICLE_ID) AS VEHICLE_LAST_EVENT_LOCAL
    FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY t
    WHERE t.EVENT_TIMESTAMP >= DATEADD(day, -4, SYSDATE())
)
SELECT
    VEHICLE_ID,
    ROW_NUMBER() OVER (PARTITION BY VEHICLE_ID ORDER BY EVENT_TIMESTAMP) AS POINT_ORDER,
    EVENT_TIMESTAMP_LOCAL, LATITUDE, LONGITUDE, SPEED_MPH, IS_SPEEDING, CELL_ID
FROM with_latest
WHERE EVENT_TIMESTAMP_LOCAL >= DATE_TRUNC('day', VEHICLE_LAST_EVENT_LOCAL);

-- Airflow's read-only monitoring role can read all dashboard views.
GRANT SELECT ON VIEW VW_VEHICLE_RECENT_STOPS TO ROLE AUTOPULSE_MONITOR;
GRANT SELECT ON VIEW VW_VEHICLE_RECENT_PATH TO ROLE AUTOPULSE_MONITOR;

-- Check: each vehicle's stops on its latest driving day, STOPS_AGO 1 = the latest stop.
SELECT VEHICLE_ID, DRIVER_NAME, STOPS_AGO, ARRIVED_LOCAL, DEPARTED_LOCAL, MINUTES_PARKED, HOTSPOT_RANK
FROM VW_VEHICLE_RECENT_STOPS
ORDER BY VEHICLE_ID, STOPS_AGO;

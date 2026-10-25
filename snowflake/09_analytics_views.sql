-- =====================================================================
-- AutoPulse - Snowflake step 9: dashboard views (ANALYTICS)
-- =====================================================================
--
-- The dashboard reads only these views:
--
--   VW_COLLISION_HOTSPOTS        map layer 1: grid cells ranked by crash severity
--   VW_FLEET_CURRENT_POSITION    map layer 2: where each vehicle is right now
--   VW_VEHICLE_RECENT_PATH       map layer 3: each vehicle's last 2 hours of driving
--   VW_VEHICLE_DAILY_SUMMARY     per vehicle per day: miles, speeding, hotspot time
--   VW_VEHICLE_HOTSPOT_EXPOSURE  per vehicle per hotspot: how often it drives there
--
-- Views are queries, not copies: they always show the current CORE data.

USE ROLE SYSADMIN;
USE DATABASE AUTOPULSE;
USE SCHEMA ANALYTICS;

-- ---------------------------------------------------------------------
-- Collision hotspots
-- ---------------------------------------------------------------------
-- The last 12 months of collisions in the data (counted back from the latest
-- crash date, because the city's feed is currently frozen at June 2026), per
-- ~500 m cell.
--   SEVERITY_SCORE = collisions + 3 x people injured + 10 x people killed
--   HOTSPOT_RANK   1 = most severe cell
--   IS_HOTSPOT     the 100 most severe cells (~1% of the city's cells)
CREATE OR REPLACE VIEW VW_COLLISION_HOTSPOTS
    COMMENT = 'Grid cells ranked by collision severity over the last 12 months of data'
AS
WITH last_crash AS (
    SELECT MAX(d.FULL_DATE) AS LAST_CRASH_DATE
    FROM AUTOPULSE.CORE.FACT_COLLISION c
    JOIN AUTOPULSE.CORE.DIM_DATE d ON d.DATE_KEY = c.DATE_KEY
),
recent_collisions AS (
    SELECT c.*
    FROM AUTOPULSE.CORE.FACT_COLLISION c
    JOIN AUTOPULSE.CORE.DIM_DATE d ON d.DATE_KEY = c.DATE_KEY
    CROSS JOIN last_crash
    WHERE c.CELL_ID IS NOT NULL
      AND d.FULL_DATE > DATEADD(year, -1, last_crash.LAST_CRASH_DATE)
),
per_cell AS (
    SELECT
        CELL_ID,
        COUNT(*)                                                AS COLLISIONS,
        -- ZEROIFNULL: the city leaves some counts empty; count those as 0.
        SUM(ZEROIFNULL(NUMBER_OF_PERSONS_INJURED))              AS PERSONS_INJURED,
        SUM(ZEROIFNULL(NUMBER_OF_PERSONS_KILLED))               AS PERSONS_KILLED,
        SUM(ZEROIFNULL(NUMBER_OF_PEDESTRIANS_INJURED) + ZEROIFNULL(NUMBER_OF_PEDESTRIANS_KILLED)) AS PEDESTRIANS_HURT,
        SUM(ZEROIFNULL(NUMBER_OF_CYCLIST_INJURED) + ZEROIFNULL(NUMBER_OF_CYCLIST_KILLED))         AS CYCLISTS_HURT,
        MODE(NULLIF(BOROUGH, 'UNKNOWN'))                        AS BOROUGH,
        MODE(NULLIF(CONTRIBUTING_FACTOR, 'Unspecified'))        AS TOP_CONTRIBUTING_FACTOR,
        MODE(CRASH_HOUR)                                        AS MOST_COMMON_CRASH_HOUR,
        COUNT(*) + 3 * SUM(ZEROIFNULL(NUMBER_OF_PERSONS_INJURED))
                 + 10 * SUM(ZEROIFNULL(NUMBER_OF_PERSONS_KILLED))  AS SEVERITY_SCORE
    FROM recent_collisions
    GROUP BY CELL_ID
)
SELECT
    p.CELL_ID,
    RANK() OVER (ORDER BY p.SEVERITY_SCORE DESC)          AS HOTSPOT_RANK,
    RANK() OVER (ORDER BY p.SEVERITY_SCORE DESC) <= 100   AS IS_HOTSPOT,
    p.SEVERITY_SCORE,
    p.COLLISIONS, p.PERSONS_INJURED, p.PERSONS_KILLED, p.PEDESTRIANS_HURT, p.CYCLISTS_HURT,
    p.BOROUGH, p.TOP_CONTRIBUTING_FACTOR, p.MOST_COMMON_CRASH_HOUR,
    l.CENTER_LATITUDE, l.CENTER_LONGITUDE,
    l.SOUTH_LATITUDE, l.NORTH_LATITUDE, l.WEST_LONGITUDE, l.EAST_LONGITUDE
FROM per_cell p
JOIN AUTOPULSE.CORE.DIM_LOCATION l ON l.CELL_ID = p.CELL_ID;

-- ---------------------------------------------------------------------
-- Fleet: current position of every vehicle
-- ---------------------------------------------------------------------
-- The newest event per vehicle, with its driver and vehicle details.
--   MINUTES_SINCE_LAST_SEEN  0 while driving; grows after 5 PM and on weekends
--   IS_ONLINE                seen in the last 30 minutes (allows for the
--                            stream -> Glue -> Snowpipe delay)
--   IN_HOTSPOT               the vehicle is in one of the 100 hotspot cells now
CREATE OR REPLACE VIEW VW_FLEET_CURRENT_POSITION
    COMMENT = 'Latest position and status of every vehicle'
AS
WITH latest_event AS (
    SELECT *
    FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY VEHICLE_ID ORDER BY EVENT_TIMESTAMP DESC) = 1
)
SELECT
    e.VEHICLE_ID,
    v.DRIVER_NAME, v.MAKE, v.MODEL, v.MODEL_YEAR,
    e.EVENT_TIMESTAMP_LOCAL                                     AS LAST_SEEN_LOCAL,
    DATEDIFF(minute, e.EVENT_TIMESTAMP, SYSDATE())              AS MINUTES_SINCE_LAST_SEEN,
    DATEDIFF(minute, e.EVENT_TIMESTAMP, SYSDATE()) <= 30        AS IS_ONLINE,
    e.LATITUDE, e.LONGITUDE,
    e.SPEED_MPH, e.IS_SPEEDING, e.ENGINE_STATUS,
    e.FUEL_LEVEL_PCT, e.ODOMETER_MILES,
    e.CELL_ID,
    COALESCE(h.IS_HOTSPOT, FALSE)                               AS IN_HOTSPOT,
    h.HOTSPOT_RANK, h.TOP_CONTRIBUTING_FACTOR                   AS HOTSPOT_TOP_FACTOR
FROM latest_event e
LEFT JOIN AUTOPULSE.CORE.DIM_VEHICLE v ON v.VEHICLE_ID = e.VEHICLE_ID
LEFT JOIN VW_COLLISION_HOTSPOTS h ON h.CELL_ID = e.CELL_ID;

-- ---------------------------------------------------------------------
-- Fleet: each vehicle's recent path (for drawing its route on the map)
-- ---------------------------------------------------------------------
-- The 2 hours of driving up to each vehicle's latest event (so after 5 PM
-- the map still shows the day's last route), oldest point first.
CREATE OR REPLACE VIEW VW_VEHICLE_RECENT_PATH
    COMMENT = 'Each vehicle''s last 2 hours of positions, in order'
AS
WITH with_latest AS (
    SELECT t.*, MAX(t.EVENT_TIMESTAMP) OVER (PARTITION BY t.VEHICLE_ID) AS VEHICLE_LAST_EVENT
    FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY t
)
SELECT
    VEHICLE_ID,
    ROW_NUMBER() OVER (PARTITION BY VEHICLE_ID ORDER BY EVENT_TIMESTAMP) AS POINT_ORDER,
    EVENT_TIMESTAMP_LOCAL, LATITUDE, LONGITUDE, SPEED_MPH, IS_SPEEDING, CELL_ID
FROM with_latest
WHERE EVENT_TIMESTAMP >= DATEADD(hour, -2, VEHICLE_LAST_EVENT);

-- ---------------------------------------------------------------------
-- Vehicles: daily summary
-- ---------------------------------------------------------------------
-- One row per vehicle per (New York) day. Each event covers 5 seconds of
-- driving, so minutes = events x 5 / 60.
CREATE OR REPLACE VIEW VW_VEHICLE_DAILY_SUMMARY
    COMMENT = 'Per vehicle per day: distance, speed, speeding and time in hotspots'
AS
WITH hotspot_cells AS (
    SELECT CELL_ID FROM VW_COLLISION_HOTSPOTS WHERE IS_HOTSPOT
)
SELECT
    t.VEHICLE_ID,
    v.DRIVER_NAME, v.MAKE, v.MODEL, v.MODEL_YEAR,
    d.FULL_DATE                                                     AS ACTIVITY_DATE,
    d.DAY_NAME,
    MIN(t.EVENT_TIMESTAMP_LOCAL)                                    AS FIRST_EVENT_LOCAL,
    MAX(t.EVENT_TIMESTAMP_LOCAL)                                    AS LAST_EVENT_LOCAL,
    COUNT(*)                                                        AS EVENTS,
    ROUND(COUNT(*) * 5 / 60, 1)                                     AS MINUTES_ACTIVE,
    ROUND(MAX(t.ODOMETER_MILES) - MIN(t.ODOMETER_MILES), 2)         AS MILES_DRIVEN,
    ROUND(AVG(t.SPEED_MPH), 1)                                      AS AVG_SPEED_MPH,
    ROUND(MAX(t.SPEED_MPH), 1)                                      AS MAX_SPEED_MPH,
    COUNT_IF(t.IS_SPEEDING)                                         AS SPEEDING_EVENTS,
    ROUND(100 * COUNT_IF(t.IS_SPEEDING) / COUNT(*), 1)              AS PCT_TIME_SPEEDING,
    ROUND(COUNT_IF(h.CELL_ID IS NOT NULL) * 5 / 60, 1)              AS MINUTES_IN_HOTSPOTS,
    ROUND(100 * COUNT_IF(h.CELL_ID IS NOT NULL) / COUNT(*), 1)      AS PCT_TIME_IN_HOTSPOTS,
    ROUND(MIN(t.FUEL_LEVEL_PCT), 1)                                 AS LOWEST_FUEL_PCT
FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY t
JOIN AUTOPULSE.CORE.DIM_DATE d ON d.DATE_KEY = t.DATE_KEY
LEFT JOIN AUTOPULSE.CORE.DIM_VEHICLE v ON v.VEHICLE_ID = t.VEHICLE_ID
LEFT JOIN hotspot_cells h ON h.CELL_ID = t.CELL_ID
GROUP BY t.VEHICLE_ID, v.DRIVER_NAME, v.MAKE, v.MODEL, v.MODEL_YEAR, d.FULL_DATE, d.DAY_NAME;

-- ---------------------------------------------------------------------
-- Vehicles: exposure to hotspots
-- ---------------------------------------------------------------------
-- For each vehicle and each hotspot cell it has driven through (last 30 days
-- of telemetry): how long it spent there and when it was last there.
CREATE OR REPLACE VIEW VW_VEHICLE_HOTSPOT_EXPOSURE
    COMMENT = 'Per vehicle per hotspot cell: time spent there and last visit'
AS
SELECT
    t.VEHICLE_ID,
    v.DRIVER_NAME,
    h.CELL_ID, h.HOTSPOT_RANK, h.SEVERITY_SCORE, h.BOROUGH, h.TOP_CONTRIBUTING_FACTOR,
    h.CENTER_LATITUDE, h.CENTER_LONGITUDE,
    COUNT(*)                                        AS EVENTS_IN_CELL,
    ROUND(COUNT(*) * 5 / 60, 1)                     AS MINUTES_IN_CELL,
    COUNT_IF(t.IS_SPEEDING)                         AS SPEEDING_EVENTS_IN_CELL,
    MAX(t.EVENT_TIMESTAMP_LOCAL)                    AS LAST_VISIT_LOCAL
FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY t
JOIN VW_COLLISION_HOTSPOTS h ON h.CELL_ID = t.CELL_ID AND h.IS_HOTSPOT
LEFT JOIN AUTOPULSE.CORE.DIM_VEHICLE v ON v.VEHICLE_ID = t.VEHICLE_ID
GROUP BY t.VEHICLE_ID, v.DRIVER_NAME, h.CELL_ID, h.HOTSPOT_RANK, h.SEVERITY_SCORE, h.BOROUGH,
         h.TOP_CONTRIBUTING_FACTOR, h.CENTER_LATITUDE, h.CENTER_LONGITUDE;

SHOW VIEWS IN SCHEMA AUTOPULSE.ANALYTICS;

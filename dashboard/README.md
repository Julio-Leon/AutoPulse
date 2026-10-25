# AutoPulse dashboard (Streamlit in Snowflake)

`streamlit_app.py` is a one-page fleet management dashboard that runs **inside
Snowflake**: no server to host, no passwords to store. It reads only the views
in `AUTOPULSE.ANALYTICS` (see `snowflake/09_analytics_views.sql`).

| Section | Shows | View(s) |
|---|---|---|
| KPIs | vehicles online, in a hotspot now, speeding now, fleet miles on the latest day | `VW_FLEET_CURRENT_POSITION`, `VW_VEHICLE_DAILY_SUMMARY` |
| Map | hotspot cells (red squares), vehicles (dots), routes of the last 2 hours (lines); hover for details | `VW_COLLISION_HOTSPOTS`, `VW_FLEET_CURRENT_POSITION`, `VW_VEHICLE_RECENT_PATH` |
| Fleet tab | every vehicle: driver, make/model/year, status, speed, fuel, in hotspot | `VW_FLEET_CURRENT_POSITION` |
| Hotspots tab | the 100 most severe cells and their main crash causes | `VW_COLLISION_HOTSPOTS` |
| Vehicle details tab | one vehicle: daily miles, % speeding, % time in hotspots, hotspots it drives through | `VW_VEHICLE_DAILY_SUMMARY`, `VW_VEHICLE_HOTSPOT_EXPOSURE` |

Map colors: vehicle **green** = online, **red** = online and inside a hotspot,
**grey** = offline (not seen for 30+ minutes, e.g. after 5 PM). Data is cached
for 2 minutes; the sidebar's **Refresh now** reloads it.

## Create the app in Snowsight (Workspace)

1. In a Snowsight Workspace, create a Streamlit app named `AUTOPULSE_FLEET_DASHBOARD`
   and replace the example `streamlit_app.py` with this one. (`pydeck`, for the
   map, comes with Streamlit; no extra package needed.)
2. **Run** shows the preview on the right.
3. **Settings -> Targets -> streamlit_app** (the deploy target):
   - App title `AUTOPULSE_FLEET_DASHBOARD`
   - Owner role of deployed app `SYSADMIN` (untick "matches the preview execution role")
   - App location `AUTOPULSE` / `ANALYTICS`
   - Compute pool `SYSTEM_COMPUTE_POOL_CPU`, query warehouse `AUTOPULSE_WH`
   -> **Save settings**.
4. **Deploy -> streamlit_app -> Deploy**. The app then opens from
   **Projects -> Streamlit**.

After changing the code, click **Deploy** again: the deployed app keeps the old
version until you do.

Until the Gold jobs have run, the app shows "No data yet" messages and an empty
map of New York; it fills in by itself once data arrives.

## Sharing

The deployed app is owned by SYSADMIN. Anyone with a Snowflake
login and a role granted `USAGE` on the app (plus the database, schema and
warehouse) can open it from **Projects -> Streamlit**.

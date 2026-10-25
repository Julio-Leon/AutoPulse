"""
AutoPulse - fleet management dashboard (Streamlit in Snowflake).

One page, built on the views in AUTOPULSE.ANALYTICS:

  Top        KPIs: vehicles online, in a hotspot now, speeding now, fleet miles today
  Map        collision hotspots (red squares, darker = more severe), every vehicle's
             position (green = online, red = online and inside a hotspot, grey =
             offline) and its last 2 hours of driving (lines)
  Tabs       Fleet      every vehicle: driver, vehicle, status, speed, fuel
             Hotspots   the most dangerous ~500 m cells and their main crash causes
             Vehicle    one vehicle in detail: daily miles, speeding, time in hotspots

The data refreshes every 2 minutes (or with the "Refresh now" button). Until the
Gold jobs have produced data, each section says what is missing instead of failing.

Runs inside Snowflake (Snowsight -> Projects -> Streamlit, or a Workspace).
All queries use full names (AUTOPULSE.ANALYTICS.<view>), so the app works
whatever database/schema it is created in. The map uses pydeck, which comes
with Streamlit (see dashboard/README.md).
"""

from decimal import Decimal

import pandas as pd
import pydeck as pdk
import streamlit as st
from snowflake.snowpark.context import get_active_session

# --------------------------------------------------
# Settings
# --------------------------------------------------

VIEWS = "AUTOPULSE.ANALYTICS"
REFRESH_SECONDS = 120                     # how long query results are reused
NYC_CENTER = (40.73, -73.95)              # map center when there are no vehicles yet

GREEN = [0, 160, 80]                      # online
RED = [220, 30, 30]                       # online and inside a hotspot
GREY = [140, 140, 140]                    # offline (not seen for 30+ minutes)

st.set_page_config(page_title="AutoPulse Fleet", layout="wide")


def snowflake_session():
    """
    The Snowflake session the app runs in. Classic Streamlit in Snowflake
    provides it directly; apps created from a Snowsight Workspace use
    st.connection("snowflake") instead. Works in both.
    """
    try:
        return get_active_session()
    except Exception:
        return st.connection("snowflake").session()


session = snowflake_session()


# --------------------------------------------------
# Data
# --------------------------------------------------

@st.cache_data(ttl=REFRESH_SECONDS, show_spinner=False)
def run_query(sql):
    """Run a query and return a pandas DataFrame, with Snowflake decimals turned into floats."""
    df = session.sql(sql).to_pandas()
    for column in df.select_dtypes(include="object").columns:      # decimals arrive as "object"
        if df[column].map(lambda value: isinstance(value, Decimal)).any():
            df[column] = df[column].astype(float)
    return df


def load_data():
    return {
        "fleet": run_query(f"SELECT * FROM {VIEWS}.VW_FLEET_CURRENT_POSITION ORDER BY VEHICLE_ID"),
        "hotspots": run_query(f"SELECT * FROM {VIEWS}.VW_COLLISION_HOTSPOTS "
                              f"WHERE HOTSPOT_RANK <= 500 ORDER BY HOTSPOT_RANK"),
        "paths": run_query(f"SELECT * FROM {VIEWS}.VW_VEHICLE_RECENT_PATH ORDER BY VEHICLE_ID, POINT_ORDER"),
        "daily": run_query(f"SELECT * FROM {VIEWS}.VW_VEHICLE_DAILY_SUMMARY "
                           f"ORDER BY ACTIVITY_DATE DESC, VEHICLE_ID"),
        "exposure": run_query(f"SELECT * FROM {VIEWS}.VW_VEHICLE_HOTSPOT_EXPOSURE "
                              f"ORDER BY VEHICLE_ID, MINUTES_IN_CELL DESC"),
    }


def vehicle_color(row):
    if not row["IS_ONLINE"]:
        return GREY
    return RED if row["IN_HOTSPOT"] else GREEN


def or_default(value, default=""):
    """The value, or a default when it is empty (None / NaN)."""
    return default if pd.isna(value) else value


def as_text(timestamp, fmt="%a %b %d, %H:%M"):
    return "" if pd.isna(timestamp) else pd.Timestamp(timestamp).strftime(fmt)


# --------------------------------------------------
# Map layers
# --------------------------------------------------
# Every layer gets a "tooltip" column with ready-made HTML, so one tooltip
# setting works for all layers.

def hotspot_layer(hotspots):
    cells = hotspots.copy()
    worst = max(cells["HOTSPOT_RANK"].max(), 1)
    intensity = 1 - (cells["HOTSPOT_RANK"] - 1) / worst            # 1 = most severe
    cells["fill"] = [[255, int(200 * (1 - i)), 0, int(60 + 140 * i)] for i in intensity]
    cells["polygon"] = cells.apply(lambda c: [
        [c["WEST_LONGITUDE"], c["SOUTH_LATITUDE"]], [c["EAST_LONGITUDE"], c["SOUTH_LATITUDE"]],
        [c["EAST_LONGITUDE"], c["NORTH_LATITUDE"]], [c["WEST_LONGITUDE"], c["NORTH_LATITUDE"]],
    ], axis=1)
    cells["tooltip"] = cells.apply(lambda c: (
        f"<b>Hotspot #{int(c['HOTSPOT_RANK'])}</b> ({or_default(c['BOROUGH'], 'borough unknown')})<br/>"
        f"{int(c['COLLISIONS'])} collisions, {int(c['PERSONS_INJURED'])} injured, "
        f"{int(c['PERSONS_KILLED'])} killed (last 12 months)<br/>"
        f"Main cause: {or_default(c['TOP_CONTRIBUTING_FACTOR'], 'unspecified')}"
        + (f"<br/>Most crashes around {int(c['MOST_COMMON_CRASH_HOUR']):02d}:00"
           if pd.notna(c["MOST_COMMON_CRASH_HOUR"]) else "")
    ), axis=1)
    return pdk.Layer(
        "PolygonLayer",
        data=cells[["polygon", "fill", "tooltip"]],
        get_polygon="polygon",
        get_fill_color="fill",
        get_line_color=[150, 0, 0, 120],
        line_width_min_pixels=1,
        pickable=True,
    )


def path_layer(paths, fleet):
    routes = (paths.groupby("VEHICLE_ID")
                   .apply(lambda p: p[["LONGITUDE", "LATITUDE"]].values.tolist())
                   .reset_index(name="path"))
    routes = routes.merge(fleet[["VEHICLE_ID", "DRIVER_NAME", "color"]], on="VEHICLE_ID", how="left")
    routes["color"] = routes["color"].apply(lambda c: c if isinstance(c, list) else GREY)
    routes["tooltip"] = routes.apply(
        lambda r: f"<b>{r['VEHICLE_ID']}</b> - last 2 hours of driving<br/>{or_default(r['DRIVER_NAME'])}", axis=1)
    return pdk.Layer(
        "PathLayer",
        data=routes[["path", "color", "tooltip"]],
        get_path="path",
        get_color="color",
        width_min_pixels=2,
        pickable=True,
    )


def vehicle_layers(fleet):
    vehicles = fleet.copy()
    vehicles["tooltip"] = vehicles.apply(lambda v: (
        f"<b>{v['VEHICLE_ID']}</b> - {or_default(v['DRIVER_NAME'], 'driver unknown')}<br/>"
        f"{or_default(v['MAKE'])} {or_default(v['MODEL'])} "
        f"{int(v['MODEL_YEAR']) if pd.notna(v['MODEL_YEAR']) else ''}<br/>"
        f"{'Online' if v['IS_ONLINE'] else 'Offline'} - last seen {as_text(v['LAST_SEEN_LOCAL'])}<br/>"
        f"{v['SPEED_MPH']:.0f} mph{' (speeding)' if v['IS_SPEEDING'] else ''}, "
        f"fuel {v['FUEL_LEVEL_PCT']:.0f}%"
        + (f"<br/><b>In hotspot #{int(v['HOTSPOT_RANK'])}</b>" if v["IN_HOTSPOT"] else "")
    ), axis=1)
    data = vehicles[["LONGITUDE", "LATITUDE", "VEHICLE_ID", "color", "tooltip"]]
    dots = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position=["LONGITUDE", "LATITUDE"],
        get_fill_color="color",
        get_line_color=[255, 255, 255],
        line_width_min_pixels=2,
        stroked=True,
        get_radius=90,
        radius_min_pixels=7,
        pickable=True,
    )
    labels = pdk.Layer(
        "TextLayer",
        data=data,
        get_position=["LONGITUDE", "LATITUDE"],
        get_text="VEHICLE_ID",
        get_size=13,
        get_color=[30, 30, 30],
        get_pixel_offset=[0, -18],
    )
    return [dots, labels]


def fleet_map(fleet, hotspots, paths, hotspots_on_map, show_hotspots, show_paths):
    layers = []
    if show_hotspots and not hotspots.empty:
        layers.append(hotspot_layer(hotspots[hotspots["HOTSPOT_RANK"] <= hotspots_on_map]))
    if show_paths and not paths.empty:
        layers.append(path_layer(paths, fleet))
    if not fleet.empty:
        layers.extend(vehicle_layers(fleet))

    if fleet.empty:
        latitude, longitude, zoom = NYC_CENTER[0], NYC_CENTER[1], 10
    else:
        latitude, longitude, zoom = fleet["LATITUDE"].mean(), fleet["LONGITUDE"].mean(), 12

    return pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(latitude=latitude, longitude=longitude, zoom=zoom),
        map_provider="carto",
        map_style="light",
        tooltip={"html": "{tooltip}", "style": {"fontSize": "12px"}},
    )


# --------------------------------------------------
# Page
# --------------------------------------------------

st.title("AutoPulse - Fleet & Collision Hotspots")

with st.sidebar:
    st.header("Map")
    show_hotspots = st.checkbox("Collision hotspots", value=True)
    hotspots_on_map = st.slider("Hotspot cells to show", 10, 500, 100, step=10,
                                help="The most severe ~500 m cells over the last 12 months of collisions.")
    show_paths = st.checkbox("Routes (last 2 hours)", value=True)
    st.divider()
    if st.button("Refresh now"):
        st.cache_data.clear()
        (getattr(st, "rerun", None) or st.experimental_rerun)()
    st.caption(f"Data is refreshed every {REFRESH_SECONDS // 60} minutes. "
               "Vehicles drive Mon-Fri, 8:00-17:00 New York time.")

with st.spinner("Loading data from Snowflake..."):
    data = load_data()

fleet, hotspots, paths, daily, exposure = (
    data["fleet"], data["hotspots"], data["paths"], data["daily"], data["exposure"])
if not fleet.empty:
    fleet["color"] = fleet.apply(vehicle_color, axis=1)

# KPIs -----------------------------------------------------------------------
latest_day = daily["ACTIVITY_DATE"].max() if not daily.empty else None
miles_latest_day = daily.loc[daily["ACTIVITY_DATE"] == latest_day, "MILES_DRIVEN"].sum() if latest_day else 0
online = fleet[fleet["IS_ONLINE"]] if not fleet.empty else fleet

kpi = st.columns(4)
kpi[0].metric("Vehicles online", f"{len(online)} / {len(fleet)}")
kpi[1].metric("In a hotspot now", int(online["IN_HOTSPOT"].sum()) if not online.empty else 0)
kpi[2].metric("Speeding now", int(online["IS_SPEEDING"].sum()) if not online.empty else 0)
kpi[3].metric(f"Fleet miles {latest_day:%a %b %d}" if latest_day else "Fleet miles today",
              f"{miles_latest_day:,.1f}")

# Map ------------------------------------------------------------------------
if fleet.empty and hotspots.empty:
    st.info("No data yet: the map fills in once the Gold jobs have run and Snowpipe has "
            "loaded their files (vehicles from autopulse-telemetry-gold, hotspots from "
            "autopulse-collisions-gold).")
st.pydeck_chart(fleet_map(fleet, hotspots, paths, hotspots_on_map, show_hotspots, show_paths),
                width="stretch")
st.caption("Squares: collision hotspots (darker = more severe). Dots: vehicles - "
           "green online, red online inside a hotspot, grey offline. Lines: last 2 hours of driving.")

# Tabs -----------------------------------------------------------------------
fleet_tab, hotspots_tab, vehicle_tab = st.tabs(["Fleet", "Hotspots", "Vehicle details"])

with fleet_tab:
    if fleet.empty:
        st.info("No vehicle data yet (autopulse-telemetry-gold has not produced data).")
    else:
        table = pd.DataFrame({
            "Vehicle": fleet["VEHICLE_ID"],
            "Driver": fleet["DRIVER_NAME"],
            "Make / model": fleet["MAKE"] + " " + fleet["MODEL"] + " " + fleet["MODEL_YEAR"].astype("Int64").astype(str),
            "Status": fleet["IS_ONLINE"].map({True: "Online", False: "Offline"}),
            "Last seen": fleet["LAST_SEEN_LOCAL"].apply(as_text),
            "Speed (mph)": fleet["SPEED_MPH"].round(0),
            "Speeding": fleet["IS_SPEEDING"],
            "Fuel %": fleet["FUEL_LEVEL_PCT"].round(0),
            "Odometer (mi)": fleet["ODOMETER_MILES"].round(0),
            "In hotspot": fleet["IN_HOTSPOT"],
        })
        st.dataframe(table, hide_index=True, width="stretch")

with hotspots_tab:
    if hotspots.empty:
        st.info("No collision data yet (autopulse-collisions-gold has not produced data).")
    else:
        top = hotspots[hotspots["IS_HOTSPOT"]]
        left, right = st.columns([3, 2])
        with left:
            st.subheader("Most dangerous cells (last 12 months)")
            st.dataframe(pd.DataFrame({
                "Rank": top["HOTSPOT_RANK"].astype(int),
                "Borough": top["BOROUGH"],
                "Collisions": top["COLLISIONS"].astype(int),
                "Injured": top["PERSONS_INJURED"].astype(int),
                "Killed": top["PERSONS_KILLED"].astype(int),
                "Pedestrians hurt": top["PEDESTRIANS_HURT"].astype(int),
                "Cyclists hurt": top["CYCLISTS_HURT"].astype(int),
                "Main cause": top["TOP_CONTRIBUTING_FACTOR"],
                "Peak hour": top["MOST_COMMON_CRASH_HOUR"].astype("Int64"),
                "Severity score": top["SEVERITY_SCORE"].astype(int),
            }), hide_index=True, width="stretch", height=420)
        with right:
            st.subheader("Main causes in the hotspots")
            causes = top["TOP_CONTRIBUTING_FACTOR"].fillna("Unspecified").value_counts().head(10)
            st.bar_chart(causes)
            st.caption("Severity score = collisions + 3 x people injured + 10 x people killed. "
                       "Hotspots = the 100 highest-scoring cells.")

with vehicle_tab:
    if fleet.empty:
        st.info("No vehicle data yet.")
    else:
        vehicle_id = st.selectbox("Vehicle", fleet["VEHICLE_ID"].tolist(),
                                  format_func=lambda v: f"{v} - {fleet.set_index('VEHICLE_ID').at[v, 'DRIVER_NAME']}")
        vehicle = fleet.set_index("VEHICLE_ID").loc[vehicle_id]

        info = st.columns(4)
        info[0].metric("Driver", vehicle["DRIVER_NAME"])
        info[1].metric("Vehicle", f"{vehicle['MAKE']} {vehicle['MODEL']}",
                       f"{int(vehicle['MODEL_YEAR'])}" if pd.notna(vehicle["MODEL_YEAR"]) else None,
                       delta_color="off")
        info[2].metric("Status", "Online" if vehicle["IS_ONLINE"] else "Offline",
                       f"last seen {as_text(vehicle['LAST_SEEN_LOCAL'])}", delta_color="off")
        info[3].metric("Fuel", f"{vehicle['FUEL_LEVEL_PCT']:.0f}%")

        days = daily[daily["VEHICLE_ID"] == vehicle_id].sort_values("ACTIVITY_DATE")
        if not days.empty:
            st.subheader("Day by day (last 30 days)")
            chart = st.columns(2)
            chart[0].caption("Miles driven")
            chart[0].bar_chart(days.set_index("ACTIVITY_DATE")["MILES_DRIVEN"])
            chart[1].caption("% of driving time speeding / in hotspots")
            chart[1].line_chart(days.set_index("ACTIVITY_DATE")[["PCT_TIME_SPEEDING", "PCT_TIME_IN_HOTSPOTS"]])
            st.dataframe(pd.DataFrame({
                "Date": days["ACTIVITY_DATE"],
                "Day": days["DAY_NAME"],
                "Miles": days["MILES_DRIVEN"],
                "Minutes active": days["MINUTES_ACTIVE"],
                "Avg mph": days["AVG_SPEED_MPH"],
                "Max mph": days["MAX_SPEED_MPH"],
                "% speeding": days["PCT_TIME_SPEEDING"],
                "Minutes in hotspots": days["MINUTES_IN_HOTSPOTS"],
                "Lowest fuel %": days["LOWEST_FUEL_PCT"],
            }).sort_values("Date", ascending=False), hide_index=True, width="stretch")

        visits = exposure[exposure["VEHICLE_ID"] == vehicle_id]
        st.subheader("Hotspots this vehicle drives through")
        if visits.empty:
            st.caption("None in the last 30 days.")
        else:
            st.dataframe(pd.DataFrame({
                "Hotspot rank": visits["HOTSPOT_RANK"].astype(int),
                "Borough": visits["BOROUGH"],
                "Main cause there": visits["TOP_CONTRIBUTING_FACTOR"],
                "Minutes spent": visits["MINUTES_IN_CELL"],
                "Speeding events there": visits["SPEEDING_EVENTS_IN_CELL"].astype(int),
                "Last visit": visits["LAST_VISIT_LOCAL"].apply(as_text),
            }), hide_index=True, width="stretch")

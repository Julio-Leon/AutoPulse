"""
AutoPulse - fleet management dashboard (Streamlit in Snowflake).

One page, built on the views in AUTOPULSE.ANALYTICS:

  Top        KPIs: vehicles online, in a hotspot now, speeding now, fleet miles today
  Map        collision hotspots (red squares, darker = more severe) and, in each
             driver's own color: the vehicle's position (faded when offline), its
             route (line) and its stops: the last 5 or all of today's
             (numbered markers, 1 = most recent)
  Tabs       Fleet      every vehicle: driver, vehicle, status, speed, fuel
             Stops      every vehicle's stops (last 5, or today's)
             Hotspots   the most dangerous ~500 m cells and their main crash causes
             Vehicle    one vehicle in detail: daily miles, speeding, time in hotspots

Refreshing: every minute the page checks (with one tiny query) whether new
telemetry has reached Snowflake, and reloads itself as soon as it has. It also
reloads every 15 minutes regardless, so "online/offline" stays correct when no
new data arrives (evenings, weekends). "Refresh now" reloads immediately. Until the
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
RELOAD_EVERY_MINUTES = 15                 # reload the page at least this often
CHECK_FOR_NEW_DATA_EVERY = "60s"          # how often to look for new telemetry
NEW_YORK = "America/New_York"
NYC_CENTER = (40.73, -73.95)              # map center when there are no vehicles yet

# One color per vehicle (dot, route and stops), in VEHICLE_ID order. No reds,
# oranges or yellows: those are the hotspot squares' colors.
DRIVER_COLORS = [
    [31, 119, 180],     # blue
    [44, 160, 44],      # green
    [148, 103, 189],    # purple
    [140, 86, 75],      # brown
    [227, 119, 194],    # pink
    [23, 190, 207],     # cyan
    [0, 0, 128],        # navy
    [128, 128, 0],      # olive
    [112, 128, 144],    # slate grey
    [0, 0, 0],          # black
]
OFFLINE_OPACITY = 90                      # 0-255: offline vehicles are drawn faded
LAST_STOPS = 5                            # "Last 5 stops" option
LAST_STOPS_OPTION = f"Last {LAST_STOPS}"
TODAY_OPTION = "Today's"
MIN_ROUTE_HOURS = 2                       # a route always shows at least this much driving

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

@st.cache_data(ttl=RELOAD_EVERY_MINUTES * 60, show_spinner=False)
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
        "stops": recent_stops(),
    }


def recent_stops():
    """
    Each vehicle's stops on its latest driving day (snowflake/11_vehicle_recent_stops_view.sql).
    If that view hasn't been created yet, the rest of the page still works.
    """
    try:
        return run_query(f"SELECT * FROM {VIEWS}.VW_VEHICLE_RECENT_STOPS ORDER BY VEHICLE_ID, STOPS_AGO")
    except Exception:
        return pd.DataFrame()


def latest_telemetry():
    """
    Newest event in the telemetry table the dashboard reads. It changes only when
    the pipeline delivers new data, so it tells the page when to reload.
    (MAX of a column is usually answered from Snowflake's table metadata, so this
    check costs next to nothing.)
    """
    row = session.sql("SELECT MAX(EVENT_TIMESTAMP) FROM AUTOPULSE.CORE.FACT_VEHICLE_TELEMETRY").collect()
    return str(row[0][0]) if row else "none"


def reload_page():
    """Forget the cached query results and draw the whole page again."""
    st.cache_data.clear()
    st.session_state.pop("loaded_at", None)
    st.rerun()


@st.fragment(run_every=CHECK_FOR_NEW_DATA_EVERY)
def auto_refresh():
    """
    Runs on its own every minute (a fragment re-runs only itself, not the page).
    Reloads the page when new telemetry has arrived, or when the data on screen is
    RELOAD_EVERY_MINUTES old.
    """
    now = pd.Timestamp.now(tz=NEW_YORK)
    newest = latest_telemetry()
    # First run of this browser tab: remember what is on screen.
    st.session_state.setdefault("loaded_at", now)
    st.session_state.setdefault("loaded_telemetry", newest)

    too_old = now - st.session_state["loaded_at"] >= pd.Timedelta(minutes=RELOAD_EVERY_MINUTES)
    if newest != st.session_state["loaded_telemetry"] or too_old:
        st.session_state["loaded_telemetry"] = newest
        st.cache_data.clear()
        st.session_state["loaded_at"] = now
        st.rerun(scope="app")

    st.caption(f"Updated {st.session_state['loaded_at']:%H:%M}. The page reloads by itself when "
               f"new telemetry arrives, and at least every {RELOAD_EVERY_MINUTES} minutes. "
               "Vehicles drive Mon-Fri, 8:00-17:00 New York time.")


def driver_colors(*tables):
    """{vehicle_id: [r, g, b]}: each vehicle gets its own color, always the same one."""
    ids = sorted(set().union(*(set(t["VEHICLE_ID"]) for t in tables if not t.empty)))
    return {vehicle_id: DRIVER_COLORS[i % len(DRIVER_COLORS)] for i, vehicle_id in enumerate(ids)}


def css(rgb):
    return f"rgb({rgb[0]}, {rgb[1]}, {rgb[2]})"


def map_legend(fleet, colors):
    """HTML for the legend in the sidebar, using the same colors as the map."""
    dot = ("display:inline-block; width:14px; height:14px; border-radius:50%; "
           "border:2px solid white; margin-right:8px; vertical-align:middle; background:")
    line = "display:inline-block; width:22px; height:4px; margin-right:8px; vertical-align:middle; background:"
    row = "margin:6px 0; font-size:0.95rem;"
    stop = ("display:inline-block; width:16px; height:16px; line-height:12px; border-radius:50%; "
            "background:white; border:2px solid #888; color:#888; "
            "font-size:10px; font-weight:700; text-align:center; margin-right:8px; vertical-align:middle;")
    drivers = "".join(
        f'<div style="{row}"><span style="{dot}{css(colors.get(v.VEHICLE_ID, [128, 128, 128]))}"></span>'
        f'{v.VEHICLE_ID} - {or_default(v.DRIVER_NAME)}</div>'
        for v in fleet.itertuples()
    ) if not fleet.empty else f'<div style="{row}">No vehicles yet</div>'

    # Hotspot squares go from pale yellow (less severe) to dark red (most severe),
    # as they look over the light map (see hotspot_layer).
    scale = "linear-gradient(to right, rgb(255,242,194), rgb(255,176,125), rgb(255,56,56))"
    return f"""
<div style="{row}"><b>Drivers</b> (dot, route and stops in the same color)</div>
{drivers}
<div style="{row}"><span style="{dot}#888; opacity:0.35;"></span>Faded dot = offline (not seen for 30+ min)</div>
<div style="{row}"><span style="{line}#888;"></span>Route covering the stops shown</div>
<div style="{row}"><span style="{stop}">1</span>Stop, numbered (1 = most recent)</div>
<div style="{row} margin-top:14px;"><b>Collision hotspots</b> (~500 m squares)</div>
<div style="height:14px; border-radius:3px; border:1px solid rgb(150,0,0); background:{scale};"></div>
<div style="display:flex; justify-content:space-between; font-size:0.85rem; opacity:0.8;">
  <span>less severe</span><span>most severe</span>
</div>
"""


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


def path_layer(paths, fleet, colors):
    routes = (paths.groupby("VEHICLE_ID")
                   .apply(lambda p: p[["LONGITUDE", "LATITUDE"]].values.tolist())
                   .reset_index(name="path"))
    routes = routes.merge(fleet[["VEHICLE_ID", "DRIVER_NAME"]], on="VEHICLE_ID", how="left")
    routes["color"] = routes["VEHICLE_ID"].map(colors)
    routes["tooltip"] = routes.apply(
        lambda r: f"<b>{r['VEHICLE_ID']}</b> - route<br/>"
                  f"{or_default(r['DRIVER_NAME'])}", axis=1)
    return pdk.Layer(
        "PathLayer",
        data=routes[["path", "color", "tooltip"]],
        get_path="path",
        get_color="color",
        width_min_pixels=2,
        pickable=True,
    )


def stop_layers(stops, colors):
    """White markers with a ring and number in the driver's color (1 = most recent stop)."""
    markers = stops.copy()
    markers["color"] = markers["VEHICLE_ID"].map(colors)
    markers["number"] = markers["STOPS_AGO"].astype(int).astype(str)
    markers["tooltip"] = markers.apply(lambda s: (
        f"<b>{s['VEHICLE_ID']}</b> - {or_default(s['DRIVER_NAME'])}<br/>"
        f"Stop {int(s['STOPS_AGO'])} (1 = most recent)<br/>"
        f"Arrived {as_text(s['ARRIVED_LOCAL'], '%a %H:%M')}, "
        + ("still parked" if s["IS_PARKED_NOW"] else
           f"left {as_text(s['DEPARTED_LOCAL'], '%H:%M')} ({s['MINUTES_PARKED']:.0f} min)")
        + (f"<br/>In hotspot #{int(s['HOTSPOT_RANK'])}" if pd.notna(s["HOTSPOT_RANK"]) else "")
    ), axis=1)
    data = markers[["LONGITUDE", "LATITUDE", "number", "color", "tooltip"]]
    rings = pdk.Layer(
        "ScatterplotLayer",
        data=data,
        get_position=["LONGITUDE", "LATITUDE"],
        get_fill_color=[255, 255, 255],
        get_line_color="color",
        line_width_min_pixels=2,
        stroked=True,
        get_radius=40,
        radius_min_pixels=7,
        pickable=True,
    )
    numbers = pdk.Layer(
        "TextLayer",
        data=data,
        get_position=["LONGITUDE", "LATITUDE"],
        get_text="number",
        get_size=11,
        get_color="color",
        get_alignment_baseline="'center'",
    )
    return [rings, numbers]


def vehicle_layers(fleet, colors):
    vehicles = fleet.copy()
    vehicles["color"] = vehicles.apply(
        lambda v: colors[v["VEHICLE_ID"]] + ([255] if v["IS_ONLINE"] else [OFFLINE_OPACITY]), axis=1)
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


def stops_to_show(stops, which):
    """The stops picked in the sidebar: each vehicle's last 5, or all of today's."""
    if stops.empty:
        return stops
    if which == TODAY_OPTION:
        today = pd.Timestamp.now(tz=NEW_YORK).tz_localize(None).normalize()
        return stops[pd.to_datetime(stops["ARRIVED_LOCAL"]).dt.normalize() == today]
    return stops[stops["STOPS_AGO"] <= LAST_STOPS]


def route_to_show(paths, shown_stops, which):
    """
    Cut each vehicle's route (its whole latest driving day) down to the part that
    covers the stops shown, so every stop marker sits on its route:
      Last 5   from the 5th-last stop (or 2 hours back, if that is longer)
      Today's  from midnight (or 2 hours back, if the vehicle hasn't driven today)
    """
    if paths.empty:
        return paths
    times = pd.to_datetime(paths["EVENT_TIMESTAMP_LOCAL"])
    start = times.groupby(paths["VEHICLE_ID"]).transform("max") - pd.Timedelta(hours=MIN_ROUTE_HOURS)
    if which == TODAY_OPTION:
        midnight = pd.Timestamp.now(tz=NEW_YORK).tz_localize(None).normalize()
        start = start.where(start < midnight, midnight)
    elif not shown_stops.empty:
        first_stop = pd.to_datetime(shown_stops.groupby("VEHICLE_ID")["ARRIVED_LOCAL"].min())
        first_stop = paths["VEHICLE_ID"].map(first_stop)
        start = start.where(first_stop.isna() | (start < first_stop), first_stop)
    return paths[times >= start]


def fleet_map(fleet, hotspots, paths, stops, colors, hotspots_on_map, show_hotspots, show_paths, show_stops):
    layers = []
    if show_hotspots and not hotspots.empty:
        layers.append(hotspot_layer(hotspots[hotspots["HOTSPOT_RANK"] <= hotspots_on_map]))
    if show_paths and not paths.empty:
        layers.append(path_layer(paths, fleet, colors))
    if show_stops and not stops.empty:
        layers.extend(stop_layers(stops, colors))          # under the vehicles, above the routes
    if not fleet.empty:
        layers.extend(vehicle_layers(fleet, colors))

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
    show_paths = st.checkbox("Routes", value=True,
                             help="Each vehicle's driving that covers the stops shown (at least the last 2 hours).")
    show_stops = st.checkbox("Stops", value=True)
    which_stops = st.radio("Which stops", [LAST_STOPS_OPTION, TODAY_OPTION], horizontal=True,
                           help=f"Each vehicle's last {LAST_STOPS} stops, or every stop it made today. "
                                "Also used by the Recent stops tab.")
    st.divider()
    if st.button("Refresh now"):
        reload_page()

with st.spinner("Loading data from Snowflake..."):
    data = load_data()

fleet, hotspots, paths, daily, exposure, stops = (
    data["fleet"], data["hotspots"], data["paths"], data["daily"], data["exposure"], data["stops"])
colors = driver_colors(fleet, paths, stops)
stops = stops_to_show(stops, which_stops)          # map markers and the Recent stops tab
paths = route_to_show(paths, stops, which_stops)   # so each route covers its stops
if not fleet.empty:
    # "Hotspot" = one of the cells selected with the slider, so the KPI and the
    # Fleet table always match the squares drawn on the map.
    # (The view's own IN_HOTSPOT flag is fixed to the top 100 cells.)
    fleet["IN_HOTSPOT"] = fleet["HOTSPOT_RANK"].notna() & (fleet["HOTSPOT_RANK"] <= hotspots_on_map)

with st.sidebar:
    auto_refresh()
    st.divider()
    st.header("Legend")
    st.markdown(map_legend(fleet, colors), unsafe_allow_html=True)

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
st.pydeck_chart(fleet_map(fleet, hotspots, paths, stops, colors, hotspots_on_map,
                          show_hotspots, show_paths, show_stops),
                width="stretch")

# Tabs -----------------------------------------------------------------------
fleet_tab, stops_tab, hotspots_tab, vehicle_tab = st.tabs(
    ["Fleet", "Recent stops", "Hotspots", "Vehicle details"])

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

with stops_tab:
    if stops.empty:
        st.info("No stops to show. A stop is recorded when a vehicle parks with its engine off "
                "(2-8 minutes at each destination). With \"Today's\" picked, there are none "
                "before the fleet starts driving (Mon-Fri 8:00). If this stays empty, run "
                "snowflake/11_vehicle_recent_stops_view.sql in Snowflake.")
    else:
        choices = ["All vehicles"] + sorted(stops["VEHICLE_ID"].unique().tolist())
        chosen = st.selectbox("Show", choices, key="stops_vehicle")
        shown = stops if chosen == "All vehicles" else stops[stops["VEHICLE_ID"] == chosen]
        st.dataframe(pd.DataFrame({
            "Vehicle": shown["VEHICLE_ID"],
            "Driver": shown["DRIVER_NAME"],
            "Stop": shown["STOPS_AGO"].astype(int),
            "Arrived": shown["ARRIVED_LOCAL"].apply(as_text),
            "Left": shown.apply(lambda r: "Still parked" if r["IS_PARKED_NOW"]
                                else as_text(r["DEPARTED_LOCAL"], "%H:%M"), axis=1),
            "Minutes parked": shown["MINUTES_PARKED"].round(0),
            "Hotspot": shown["HOTSPOT_RANK"].apply(lambda r: f"#{int(r)}" if pd.notna(r) else ""),
            "Latitude": shown["LATITUDE"].round(5),
            "Longitude": shown["LONGITUDE"].round(5),
        }), hide_index=True, width="stretch")
        shown_what = (f"Each vehicle's last {LAST_STOPS} stops (fewer if it has made fewer)"
                      if which_stops == LAST_STOPS_OPTION else "Every stop made today")
        st.caption(f"{shown_what}; change this with \"Which stops\" in the sidebar. "
                   "Stop 1 = the most recent, also the numbers on the map. A stop = parked with "
                   "the engine off; red lights don't count. Hotspot = the collision rank of the "
                   "~500 m cell it parked in.")

with hotspots_tab:
    if hotspots.empty:
        st.info("No collision data yet (autopulse-collisions-gold has not produced data).")
    else:
        top = hotspots[hotspots["HOTSPOT_RANK"] <= hotspots_on_map]
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
                       f"Hotspots = the {hotspots_on_map} highest-scoring cells "
                       "(the 'Hotspot cells to show' slider).")

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

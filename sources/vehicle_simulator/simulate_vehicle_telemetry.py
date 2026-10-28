"""
AutoPulse - vehicle telemetry simulator.

Simulates a fleet of 10 vehicles driving on New York City's real streets.
Every 5 seconds each vehicle sends one JSON event to the Kafka topic
`vehicle.telemetry.raw`:

    {"event_id": "VEH-001-20261021T125501123456Z", "vehicle_id": "VEH-001",
     "driver_name": "Maria Lopez", "make": "Ford", "model": "Transit", "model_year": 2022,
     "timestamp": "2026-10-21T12:55:01.123456+00:00",
     "latitude": 40.7581, "longitude": -73.9853, "speed_mph": 23.4, "speed_limit_mph": 25,
     "odometer_miles": 45210.37, "fuel_level_pct": 61.2,
     "engine_status": "running", "is_simulated": true}

How the vehicles drive (street map: road_network/nyc_roads.bin.gz, built once
with road_network/build_road_network.py from OpenStreetMap):
  - Each vehicle makes TRIPS: it picks a destination 2-8 km away and at least
    1 km from its last 5 stops (so it keeps exploring new places), and follows
    the fastest legal route there (one-way streets respected), point by point
    along the street lines: never through buildings, parks or water.
  - Speed follows the road's speed limit and the driver's pace for the trip
    (some trips run a little over the limit), with gradual acceleration.
  - At some intersections the light is red: the vehicle waits 10-40 seconds
    (engine "idling").
  - At the destination it parks for 2-8 minutes (engine "off", like a delivery
    stop), then plans its next trip.
  - Odometer and fuel follow the distance actually driven; a vehicle refuels to
    a full tank when it drops below 10%.

Each vehicle has a driver and a make, model and year, assigned when the fleet
is created and sent in every event. The fleet's state (details, position, the
current trip) is saved to data/raw/fleet_state.json after every round, so a
restart continues where it left off.

Working hours: with WORKING_HOURS_ONLY=true (set on the EC2 host), the fleet
only drives Monday to Friday, 8:00 to 17:00 New York time; outside those hours
it sends nothing and the vehicles stay where they parked.

Run from the project root (Kafka must be up, see infrastructure/local_kafka/):
    python sources/vehicle_simulator/simulate_vehicle_telemetry.py
Stop with Ctrl+C.

Settings (environment variables, all optional):
    KAFKA_BOOTSTRAP_SERVERS   default localhost:29092
    WORKING_HOURS_ONLY        default false; true = Mon-Fri, WORKDAY_START_HOUR to WORKDAY_END_HOUR
    WORKDAY_START_HOUR        default 8
    WORKDAY_END_HOUR          default 17   (stops at 17:00)
    SCHEDULE_TIMEZONE         default America/New_York
"""

import json
import logging
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from kafka import KafkaProducer

from road_network.road_network import MPH_TO_METERS_PER_SECOND, RoadNetwork, distance_m

# --------------------------------------------------
# Settings
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC = "vehicle.telemetry.raw"

# Working hours (see the top of this file).
WORKING_HOURS_ONLY = os.getenv("WORKING_HOURS_ONLY", "false").lower() == "true"
WORKDAY_START_HOUR = int(os.getenv("WORKDAY_START_HOUR", "8"))
WORKDAY_END_HOUR = int(os.getenv("WORKDAY_END_HOUR", "17"))
SCHEDULE_TIMEZONE = ZoneInfo(os.getenv("SCHEDULE_TIMEZONE", "America/New_York"))
PAUSED_CHECK_SECONDS = 60      # outside working hours, check the clock once a minute

NUMBER_OF_VEHICLES = 10
PUBLISH_INTERVAL_SECONDS = 5

# Trips.
TRIP_DISTANCE_M = (2_000, 8_000)          # straight-line distance to the next destination
MIN_DISTANCE_FROM_RECENT_STOPS_M = 1_000  # don't go back near the last few stops
RECENT_STOPS_REMEMBERED = 5
DELIVERY_STOP_SECONDS = (120, 480)        # parked at the destination
RED_LIGHT_CHANCE = 0.15                   # at each intersection passed
RED_LIGHT_SECONDS = (10, 40)

# Driving style.
ACCELERATION_MPH_PER_TICK = 8             # how fast speed can rise in one 5-second step
BRAKING_MPH_PER_TICK = 15
SPEEDING_TRIP_CHANCE = 0.15               # trips driven faster than the limit
NORMAL_PACE = (0.75, 1.0)                 # speed as a share of the limit
SPEEDING_PACE = (1.1, 1.3)

# Fuel model for the fictional vehicles.
FUEL_TANK_GALLONS = 18.0
AVERAGE_MPG = 25.0
REFUEL_BELOW_PCT = 10.0
METERS_PER_MILE = 1609.344

# Fleet details, assigned once when the fleet is created.
VEHICLE_MODELS = [            # (make, model): typical city fleet vehicles
    ("Ford", "Transit"), ("Ford", "Explorer"), ("Toyota", "Camry"), ("Toyota", "Prius"),
    ("Honda", "Accord"), ("Chevrolet", "Express"), ("Ram", "ProMaster"),
    ("Mercedes-Benz", "Sprinter"), ("Nissan", "NV200"), ("Hyundai", "Sonata"),
]
MODEL_YEARS = range(2018, 2026)
DRIVER_NAMES = [              # fictional drivers
    "Maria Lopez", "James Carter", "Aisha Khan", "Daniel Kim", "Sofia Rossi",
    "Marcus Johnson", "Elena Petrova", "Kevin O'Brien", "Priya Patel", "Luis Romero",
    "Grace Chen", "Omar Haddad",
]

# Files.
SIMULATOR_DIR = Path(__file__).resolve().parent
ROAD_NETWORK_FILE = SIMULATOR_DIR / "road_network" / "nyc_roads.bin.gz"
PROJECT_ROOT = SIMULATOR_DIR.parents[1]
STATE_FILE = PROJECT_ROOT / "data" / "raw" / "fleet_state.json"     # data/ is git-ignored
STATE_VERSION = "road-network-1"          # saved fleets from older simulator versions are replaced

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("vehicle_simulator")


# --------------------------------------------------
# Fleet state
# --------------------------------------------------

def load_or_create_fleet(network, rng):
    """Load the saved fleet, or create 10 new vehicles (parked at random intersections)."""
    if STATE_FILE.exists():
        saved = json.loads(STATE_FILE.read_text())
        if isinstance(saved, dict) and saved.get("version") == STATE_VERSION:
            logger.info("Loaded %d vehicles from %s", len(saved["vehicles"]), STATE_FILE)
            return saved["vehicles"]
        logger.info("Saved fleet is from an older simulator version: creating a new fleet")

    drivers = rng.sample(DRIVER_NAMES, NUMBER_OF_VEHICLES)       # every vehicle its own driver
    fleet = []
    for number in range(1, NUMBER_OF_VEHICLES + 1):
        make, model = rng.choice(VEHICLE_MODELS)                  # models may repeat, like a real fleet
        start = network.random_vertex(rng)
        latitude, longitude = network.position(start)
        vehicle = {
            "vehicle_id": f"VEH-{number:03d}",
            "driver_name": drivers[number - 1],
            "make": make,
            "model": model,
            "model_year": rng.choice(MODEL_YEARS),
            "latitude": latitude,
            "longitude": longitude,
            "speed_mph": 0.0,
            "odometer_miles": round(rng.uniform(10_000, 80_000), 2),
            "fuel_level_pct": round(rng.uniform(30, 100), 2),
            "engine_status": "off",
            "vertex": start,                  # intersection where the current trip started
            "recent_stops": [start],
            "trip": None,
            "stop_seconds_left": 0,
        }
        plan_trip(vehicle, network, rng)
        fleet.append(vehicle)
    save_fleet(fleet)
    logger.info("Created a new fleet of %d vehicles", len(fleet))
    return fleet


def save_fleet(fleet):
    """Write to a temporary file first, then swap it in, so a crash never leaves a half-written file."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_file = STATE_FILE.with_suffix(".tmp")
    temp_file.write_text(json.dumps({"version": STATE_VERSION, "vehicles": fleet}, separators=(",", ":")))
    os.replace(temp_file, STATE_FILE)


# --------------------------------------------------
# Working hours
# --------------------------------------------------

def is_working_hours(now=None):
    """True when the fleet should be driving (always, unless WORKING_HOURS_ONLY is on)."""
    if not WORKING_HOURS_ONLY:
        return True
    local_time = now or datetime.now(SCHEDULE_TIMEZONE)
    is_weekday = local_time.weekday() < 5          # Monday=0 ... Friday=4
    return is_weekday and WORKDAY_START_HOUR <= local_time.hour < WORKDAY_END_HOUR


# --------------------------------------------------
# Trips
# --------------------------------------------------

def pick_destination(vehicle, network, rng):
    """A destination 2-8 km away and at least 1 km from the vehicle's recent stops."""
    start = vehicle["vertex"]
    for _ in range(200):
        goal = network.random_vertex(rng)
        if not TRIP_DISTANCE_M[0] <= network.distance_between(start, goal) <= TRIP_DISTANCE_M[1]:
            continue
        if any(network.distance_between(goal, stop) < MIN_DISTANCE_FROM_RECENT_STOPS_M
               for stop in vehicle["recent_stops"]):
            continue
        return goal
    return network.random_vertex(rng)     # very unusual (e.g. a corner of the map): anywhere


def plan_trip(vehicle, network, rng):
    """Choose the next destination and the fastest route there; set the driver's pace."""
    for _ in range(5):
        goal = pick_destination(vehicle, network, rng)
        route = network.fastest_route(vehicle["vertex"], goal)
        if route:
            break
    else:
        raise RuntimeError(f"{vehicle['vehicle_id']}: no route found from vertex {vehicle['vertex']}")

    points, speed_limits, intersections = network.route_geometry(route, vehicle["vertex"])
    speeding = rng.random() < SPEEDING_TRIP_CHANCE
    vehicle["trip"] = {
        "destination": goal,
        "points": [[round(lat, 6), round(lon, 6)] for lat, lon in points],
        "speed_limits": list(speed_limits),
        "intersections": intersections,
        "segment": 0,                      # driving from points[segment] to points[segment + 1]
        "meters_into_segment": 0.0,
        "pace": rng.uniform(*(SPEEDING_PACE if speeding else NORMAL_PACE)),
    }
    vehicle["recent_stops"] = (vehicle["recent_stops"] + [goal])[-RECENT_STOPS_REMEMBERED:]


# --------------------------------------------------
# Driving one 5-second step
# --------------------------------------------------

def drive(vehicle, network, rng, seconds):
    """Advance one vehicle by `seconds`; returns the meters driven."""
    # Waiting at a red light or parked at a destination.
    if vehicle["stop_seconds_left"] > 0:
        vehicle["stop_seconds_left"] -= seconds
        vehicle["speed_mph"] = 0.0
        if vehicle["stop_seconds_left"] <= 0 and vehicle["trip"] is None:
            plan_trip(vehicle, network, rng)        # delivery done: off to the next destination
            vehicle["engine_status"] = "idling"
        return 0.0

    trip = vehicle["trip"]
    points = trip["points"]

    # Speed: towards the road's limit x the driver's pace, changing gradually.
    target = trip["speed_limits"][trip["segment"]] * trip["pace"] + rng.uniform(-2, 2)
    change = max(-BRAKING_MPH_PER_TICK, min(ACCELERATION_MPH_PER_TICK, target - vehicle["speed_mph"]))
    vehicle["speed_mph"] = max(3.0, vehicle["speed_mph"] + change)
    vehicle["engine_status"] = "running"

    # Move along the route by the distance covered at that speed.
    to_drive = vehicle["speed_mph"] * MPH_TO_METERS_PER_SECOND * seconds
    driven = 0.0
    while to_drive > 0:
        start, end = points[trip["segment"]], points[trip["segment"] + 1]
        segment_length = distance_m(*start, *end)
        remaining = segment_length - trip["meters_into_segment"]
        if to_drive < remaining:
            trip["meters_into_segment"] += to_drive
            driven += to_drive
            break
        driven += remaining
        to_drive -= remaining
        trip["segment"] += 1
        trip["meters_into_segment"] = 0.0

        if trip["segment"] >= len(points) - 1:             # arrived at the destination
            vehicle["vertex"] = trip["destination"]
            vehicle["trip"] = None
            vehicle["stop_seconds_left"] = rng.randint(*DELIVERY_STOP_SECONDS)
            vehicle["latitude"], vehicle["longitude"] = points[-1]
            vehicle["speed_mph"], vehicle["engine_status"] = 0.0, "off"
            return driven
        if trip["segment"] in trip["intersections"] and rng.random() < RED_LIGHT_CHANCE:
            vehicle["stop_seconds_left"] = rng.randint(*RED_LIGHT_SECONDS)
            vehicle["speed_mph"], vehicle["engine_status"] = 0.0, "idling"
            break

    # Position: the point that far along the current stretch of road.
    start, end = points[trip["segment"]], points[trip["segment"] + 1]
    segment_length = distance_m(*start, *end) or 1.0
    share = min(1.0, trip["meters_into_segment"] / segment_length)
    vehicle["latitude"] = start[0] + (end[0] - start[0]) * share
    vehicle["longitude"] = start[1] + (end[1] - start[1]) * share
    return driven


def update_fuel_and_odometer(vehicle, meters):
    miles = meters / METERS_PER_MILE
    vehicle["odometer_miles"] += miles
    fuel_used_pct = (miles / AVERAGE_MPG) / FUEL_TANK_GALLONS * 100
    vehicle["fuel_level_pct"] = max(0.0, vehicle["fuel_level_pct"] - fuel_used_pct)
    if vehicle["fuel_level_pct"] < REFUEL_BELOW_PCT:
        vehicle["fuel_level_pct"] = 100.0          # stopped at a gas station


def telemetry_event(vehicle):
    trip = vehicle["trip"]
    speed_limit = trip["speed_limits"][trip["segment"]] if trip else None
    now = datetime.now(timezone.utc)
    return {
        "event_id": f"{vehicle['vehicle_id']}-{now.strftime('%Y%m%dT%H%M%S%fZ')}",
        "vehicle_id": vehicle["vehicle_id"],
        "driver_name": vehicle["driver_name"],
        "make": vehicle["make"],
        "model": vehicle["model"],
        "model_year": vehicle["model_year"],
        "timestamp": now.isoformat(),
        "latitude": round(vehicle["latitude"], 6),
        "longitude": round(vehicle["longitude"], 6),
        "speed_mph": round(vehicle["speed_mph"], 2),
        "speed_limit_mph": speed_limit,
        "odometer_miles": round(vehicle["odometer_miles"], 2),
        "fuel_level_pct": round(vehicle["fuel_level_pct"], 2),
        "engine_status": vehicle["engine_status"],
        "is_simulated": True,
    }


# --------------------------------------------------
# Main loop
# --------------------------------------------------

def main():
    network = RoadNetwork.load(ROAD_NETWORK_FILE)
    logger.info("Street map: %s, %d intersections, %d road edges",
                network.area, network.vertex_count, network.edge_count)
    rng = random.Random()
    fleet = load_or_create_fleet(network, rng)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda event: json.dumps(event).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all",       # wait until Kafka has stored the event
        retries=3,
    )
    logger.info("Sending to %s, topic %s. Press Ctrl+C to stop.", KAFKA_BOOTSTRAP_SERVERS, TOPIC)
    if WORKING_HOURS_ONLY:
        logger.info("Working hours only: Mon-Fri %02d:00-%02d:00 %s",
                    WORKDAY_START_HOUR, WORKDAY_END_HOUR, SCHEDULE_TIMEZONE.key)

    paused = False
    try:
        while True:
            if not is_working_hours():
                if not paused:
                    logger.info("Outside working hours: paused (vehicles parked)")
                    paused = True
                time.sleep(PAUSED_CHECK_SECONDS)
                continue
            if paused:
                logger.info("Working hours: resuming")
                paused = False

            for vehicle in fleet:
                meters = drive(vehicle, network, rng, PUBLISH_INTERVAL_SECONDS)
                update_fuel_and_odometer(vehicle, meters)
                # Keying by vehicle_id keeps each vehicle's events in order (same partition).
                producer.send(TOPIC, key=vehicle["vehicle_id"], value=telemetry_event(vehicle))
            producer.flush()
            save_fleet(fleet)
            logger.info("Sent %d events", len(fleet))
            time.sleep(PUBLISH_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Stopped by user")
    finally:
        producer.close()


if __name__ == "__main__":
    main()

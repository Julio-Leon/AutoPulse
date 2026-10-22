"""
AutoPulse - vehicle telemetry simulator.

Simulates a fleet of 10 vehicles driving around Midtown Manhattan. Every
5 seconds each vehicle sends one JSON event to the Kafka topic
`vehicle.telemetry.raw`:

    {"event_id": "VEH-001-20261021T125501123456Z", "vehicle_id": "VEH-001",
     "driver_name": "Maria Lopez", "make": "Ford", "model": "Transit", "model_year": 2022,
     "timestamp": "2026-10-21T12:55:01.123456+00:00",
     "latitude": 40.7581, "longitude": -73.9853, "speed_mph": 23.4,
     "odometer_miles": 45210.37, "fuel_level_pct": 61.2,
     "engine_status": "running", "is_simulated": true}

When the fleet is created, each vehicle is given a driver and a make, model
and year (picked from the lists below). They stay with the vehicle for good and
are sent in every event, so the pipeline always knows who drives what.

The fleet's state (details, position, odometer, fuel) is saved to
data/raw/fleet_state.json after every event, so a restart continues where it
left off instead of creating new vehicles. A vehicle refuels to a full tank
when it drops below 10%.

Working hours: with WORKING_HOURS_ONLY=true (set on the EC2 host), the fleet
only drives Monday to Friday, 8:00 to 17:00 New York time. Outside those hours
the simulator stays running but sends nothing, and the vehicles stay where they
parked; it resumes on its own at 8:00. Without it (e.g. on a laptop), it sends
around the clock.

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

# Vehicles start within ~4 km of Times Square.
BASE_LATITUDE = 40.7580
BASE_LONGITUDE = -73.9855

# Fuel model for the fictional vehicles.
FUEL_TANK_GALLONS = 18.0
AVERAGE_MPG = 25.0
REFUEL_BELOW_PCT = 10.0

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
VEHICLE_DETAIL_FIELDS = ("driver_name", "make", "model", "model_year")

# <project root>/data/raw/fleet_state.json  (data/ is git-ignored)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
STATE_FILE = PROJECT_ROOT / "data" / "raw" / "fleet_state.json"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("vehicle_simulator")


# --------------------------------------------------
# Fleet state
# --------------------------------------------------

def load_or_create_fleet():
    """Load the saved fleet, or create 10 new vehicles the first time."""
    if STATE_FILE.exists():
        fleet = json.loads(STATE_FILE.read_text())
        if all(field in vehicle for vehicle in fleet for field in VEHICLE_DETAIL_FIELDS):
            logger.info("Loaded %d vehicles from %s", len(fleet), STATE_FILE)
            return fleet
        logger.info("Saved fleet has no driver/make/model/year (older version): creating a new fleet")

    # Every vehicle gets its own driver; models may repeat, like in a real fleet.
    drivers = random.sample(DRIVER_NAMES, NUMBER_OF_VEHICLES)
    models = [random.choice(VEHICLE_MODELS) for _ in range(NUMBER_OF_VEHICLES)]
    fleet = [
        {
            "vehicle_id": f"VEH-{number:03d}",
            "driver_name": drivers[number - 1],
            "make": models[number - 1][0],
            "model": models[number - 1][1],
            "model_year": random.choice(MODEL_YEARS),
            "latitude": BASE_LATITUDE + random.uniform(-0.04, 0.04),
            "longitude": BASE_LONGITUDE + random.uniform(-0.04, 0.04),
            "speed_mph": random.uniform(0, 45),
            "odometer_miles": round(random.uniform(10000, 80000), 2),
            "fuel_level_pct": round(random.uniform(20, 100), 2),
            "engine_status": "running",
        }
        for number in range(1, NUMBER_OF_VEHICLES + 1)
    ]
    save_fleet(fleet)
    logger.info("Created a new fleet of %d vehicles", len(fleet))
    return fleet


def save_fleet(fleet):
    """Write to a temporary file first, then swap it in, so a crash never leaves a half-written file."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_file = STATE_FILE.with_suffix(".tmp")
    temp_file.write_text(json.dumps(fleet, indent=2))
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
# Simulation
# --------------------------------------------------

def move_vehicle(vehicle):
    """Advance one vehicle by one interval and return its telemetry event."""
    # Speed changes a little each interval, between 0 and 65 mph.
    vehicle["speed_mph"] = max(0.0, min(65.0, vehicle["speed_mph"] + random.uniform(-8, 8)))

    # Distance covered in this interval, and the fuel it used.
    miles = vehicle["speed_mph"] * PUBLISH_INTERVAL_SECONDS / 3600
    vehicle["odometer_miles"] += miles
    fuel_used_pct = (miles / AVERAGE_MPG) / FUEL_TANK_GALLONS * 100
    vehicle["fuel_level_pct"] = max(0.0, vehicle["fuel_level_pct"] - fuel_used_pct)
    if vehicle["fuel_level_pct"] < REFUEL_BELOW_PCT:
        vehicle["fuel_level_pct"] = 100.0          # stopped at a gas station

    # A moving vehicle drifts slightly in position.
    if vehicle["speed_mph"] > 0:
        vehicle["latitude"] += random.uniform(-0.0002, 0.0002)
        vehicle["longitude"] += random.uniform(-0.0002, 0.0002)
        vehicle["engine_status"] = "running"
    else:
        vehicle["engine_status"] = "idling"

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
        "odometer_miles": round(vehicle["odometer_miles"], 2),
        "fuel_level_pct": round(vehicle["fuel_level_pct"], 2),
        "engine_status": vehicle["engine_status"],
        "is_simulated": True,
    }


def main():
    fleet = load_or_create_fleet()

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
                event = move_vehicle(vehicle)
                # Keying by vehicle_id keeps each vehicle's events in order (same partition).
                producer.send(TOPIC, key=vehicle["vehicle_id"], value=event)
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

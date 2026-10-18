
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "vehicle_telemetry_producer.log"

# Runtime state persists between simulator restarts.
STATE_DIR = PROJECT_ROOT / "data" / "raw"
STATE_FILE = STATE_DIR / "fleet_state.json"
TEMP_STATE_FILE = STATE_DIR / "fleet_state.tmp"

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "vehicle.telemetry.raw"

NUMBER_OF_VEHICLES = 10
PUBLISH_INTERVAL_SECONDS = 5

BASE_LATITUDE = 40.7580
BASE_LONGITUDE = -73.9855

# Assumptions for our fictional vehicles.
FUEL_TANK_GALLONS = 18.0
AVERAGE_MPG = 25.0


# --------------------------------------------------
# Logging
# --------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="w", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
    force=True,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------
# Persistent fleet state
# --------------------------------------------------

def save_fleet_state(fleet):
    """Atomically save the latest state of all vehicles."""

    STATE_DIR.mkdir(parents=True, exist_ok=True)

    with TEMP_STATE_FILE.open("w", encoding="utf-8") as file:
        json.dump(fleet, file, indent=2)

    # Replace the previous state only after the new file is written.
    os.replace(TEMP_STATE_FILE, STATE_FILE)


def create_fleet():
    """Load existing vehicle state or initialize the fleet once."""

    if STATE_FILE.exists():
        with STATE_FILE.open("r", encoding="utf-8") as file:
            fleet = json.load(file)

        expected_ids = {
            f"VEH-{number:03d}"
            for number in range(1, NUMBER_OF_VEHICLES + 1)
        }
        actual_ids = {vehicle["vehicle_id"] for vehicle in fleet}

        if len(fleet) != NUMBER_OF_VEHICLES or actual_ids != expected_ids:
            raise ValueError(
                f"Unexpected fleet state in {STATE_FILE}. "
                "Check the file before continuing."
            )

        logger.info("Loaded existing fleet state from %s", STATE_FILE)
        return fleet

    fleet = []

    for number in range(1, NUMBER_OF_VEHICLES + 1):
        fleet.append({
            "vehicle_id": f"VEH-{number:03d}",
            "latitude": BASE_LATITUDE + random.uniform(-0.04, 0.04),
            "longitude": BASE_LONGITUDE + random.uniform(-0.04, 0.04),
            "speed_mph": random.uniform(0, 45),
            "odometer_miles": round(random.uniform(10000, 80000), 2),
            "fuel_level_pct": round(random.uniform(20, 100), 2),
            "engine_status": "running",
        })

    save_fleet_state(fleet)
    logger.info("Initialized and saved a new fleet of %s vehicles", len(fleet))

    return fleet


# --------------------------------------------------
# Telemetry generation
# --------------------------------------------------

def generate_telemetry(vehicle):
    """Update a vehicle by one interval and create a telemetry event."""

    previous_speed = vehicle["speed_mph"]

    new_speed = max(
        0.0,
        min(65.0, previous_speed + random.uniform(-8.0, 8.0)),
    )

    # Distance traveled during this interval.
    distance_miles = new_speed * PUBLISH_INTERVAL_SECONDS / 3600.0

    vehicle["odometer_miles"] += distance_miles

    # Approximate fuel consumption based on distance traveled.
    if new_speed > 0:
        fuel_used_gallons = distance_miles / AVERAGE_MPG
        fuel_used_pct = fuel_used_gallons / FUEL_TANK_GALLONS * 100.0

        vehicle["fuel_level_pct"] = max(
            0.0,
            vehicle["fuel_level_pct"] - fuel_used_pct,
        )

    # Small movement to simulate changing GPS coordinates.
    if new_speed > 0:
        vehicle["latitude"] += random.uniform(-0.0002, 0.0002)
        vehicle["longitude"] += random.uniform(-0.0002, 0.0002)

    vehicle["speed_mph"] = new_speed
    vehicle["engine_status"] = "running" if new_speed > 0 else "idling"

    now = datetime.now(timezone.utc)

    event = {
        "event_id": (
            f"{vehicle['vehicle_id']}-"
            f"{now.strftime('%Y%m%dT%H%M%S%fZ')}"
        ),
        "vehicle_id": vehicle["vehicle_id"],
        "timestamp": now.isoformat(),
        "latitude": round(vehicle["latitude"], 6),
        "longitude": round(vehicle["longitude"], 6),
        "speed_mph": round(vehicle["speed_mph"], 2),
        "odometer_miles": round(vehicle["odometer_miles"], 2),
        "fuel_level_pct": round(vehicle["fuel_level_pct"], 2),
        "engine_status": vehicle["engine_status"],
        "is_simulated": True,
    }

    return event


# --------------------------------------------------
# Kafka producer
# --------------------------------------------------

def main():
    producer = None

    try:
        fleet = create_fleet()

        logger.info("Connecting to Kafka at %s", BOOTSTRAP_SERVERS)

        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=3,
        )

        logger.info("Publishing telemetry to %s", TOPIC)
        logger.info("Fleet state file: %s", STATE_FILE)
        logger.info("Press Ctrl+C to stop")

        while True:
            for vehicle in fleet:
                event = generate_telemetry(vehicle)

                try:
                    future = producer.send(
                        TOPIC,
                        value=event,
                        key=vehicle["vehicle_id"].encode("utf-8"),
                    )
                    metadata = future.get(timeout=10)

                    # Persist each vehicle's updated state.
                    save_fleet_state(fleet)

                    logger.info(
                        "Published %s | odometer=%.2f miles "
                        "| fuel=%.2f%% | partition=%s | offset=%s",
                        event["vehicle_id"],
                        event["odometer_miles"],
                        event["fuel_level_pct"],
                        metadata.partition,
                        metadata.offset,
                    )

                except KafkaError:
                    logger.exception(
                        "Failed to publish telemetry for %s",
                        vehicle["vehicle_id"],
                    )
                    raise

            time.sleep(PUBLISH_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Simulator stopped by user")

    except Exception:
        logger.exception("Fleet telemetry simulator failed")
        raise

    finally:
        if producer is not None:
            producer.close()
            logger.info("Kafka producer closed")


if __name__ == "__main__":
    main()

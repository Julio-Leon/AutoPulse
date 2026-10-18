import json
import logging
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


# --------------------------------------------------
# Project configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "vehicle_telemetry_producer.log"

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "vehicle.telemetry.raw"

NUMBER_OF_VEHICLES = 10
PUBLISH_INTERVAL_SECONDS = 5

# NYC-area starting point for our simulated fleet.
BASE_LATITUDE = 40.7580
BASE_LONGITUDE = -73.9855


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
# Simulated fleet
# --------------------------------------------------

def create_fleet():
    """Create 10 fictional vehicles with initial telemetry state."""

    fleet = []

    for number in range(1, NUMBER_OF_VEHICLES + 1):
        vehicle = {
            "vehicle_id": f"VEH-{number:03d}",
            "latitude": BASE_LATITUDE + random.uniform(-0.04, 0.04),
            "longitude": BASE_LONGITUDE + random.uniform(-0.04, 0.04),
            "speed_mph": random.uniform(0, 45),
            "odometer_miles": random.uniform(10000, 80000),
            "fuel_level_pct": random.uniform(20, 100),
            "engine_status": "running",
        }

        fleet.append(vehicle)

    return fleet


def generate_telemetry(vehicle):
    """Generate the next simulated reading and update vehicle state."""

    previous_speed = vehicle["speed_mph"]

    # Simulate changing vehicle speed.
    new_speed = max(
        0.0,
        min(65.0, previous_speed + random.uniform(-8.0, 8.0)),
    )

    # Approximate distance traveled during this five-second interval.
    distance_miles = new_speed * PUBLISH_INTERVAL_SECONDS / 3600

    # Update odometer and fuel level.
    vehicle["odometer_miles"] += distance_miles

    if new_speed > 0:
        vehicle["fuel_level_pct"] = max(
            0.0,
            vehicle["fuel_level_pct"] - random.uniform(0.001, 0.01),
        )

    # Simulate a small GPS movement.
    if new_speed > 0:
        vehicle["latitude"] += random.uniform(-0.0002, 0.0002)
        vehicle["longitude"] += random.uniform(-0.0002, 0.0002)

    vehicle["speed_mph"] = new_speed
    vehicle["engine_status"] = "running" if new_speed > 0 else "idling"

    return {
        "event_id": (
            f"{vehicle['vehicle_id']}-"
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}"
        ),
        "vehicle_id": vehicle["vehicle_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "latitude": round(vehicle["latitude"], 6),
        "longitude": round(vehicle["longitude"], 6),
        "speed_mph": round(vehicle["speed_mph"], 2),
        "odometer_miles": round(vehicle["odometer_miles"], 2),
        "fuel_level_pct": round(vehicle["fuel_level_pct"], 2),
        "engine_status": vehicle["engine_status"],
        "is_simulated": True,
    }


# --------------------------------------------------
# Kafka producer
# --------------------------------------------------

def main():
    producer = None

    try:
        logger.info("Connecting to Kafka at %s", BOOTSTRAP_SERVERS)

        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=3,
        )

        fleet = create_fleet()
        logger.info("Created %s simulated vehicles", len(fleet))
        logger.info("Publishing telemetry to topic: %s", TOPIC)
        logger.info("Press Ctrl+C to stop the simulator")

        while True:
            for vehicle in fleet:
                event = generate_telemetry(vehicle)

                try:
                    future = producer.send(TOPIC, value=event)
                    metadata = future.get(timeout=10)

                    logger.info(
                        "Published %s | speed=%.2f mph | fuel=%.2f%% "
                        "| partition=%s | offset=%s",
                        event["vehicle_id"],
                        event["speed_mph"],
                        event["fuel_level_pct"],
                        metadata.partition,
                        metadata.offset,
                    )

                except KafkaError:
                    logger.exception(
                        "Failed to publish telemetry for %s",
                        vehicle["vehicle_id"],
                    )

            producer.flush()
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

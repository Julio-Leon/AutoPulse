
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "traffic_producer.log"

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

BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "traffic.incidents.raw"


def main():
    producer = None

    event = {
        "event_id": "traffic-test-001",
        "event_type": "traffic_incident",
        "incident_type": "road_closure",
        "description": "Test road closure near Manhattan",
        "borough": "MANHATTAN",
        "latitude": 40.7580,
        "longitude": -73.9855,
        "severity": "moderate",
        "event_time": datetime.now(timezone.utc).isoformat(),
        "is_simulated": True,
    }

    try:
        logger.info("Connecting to Kafka at %s", BOOTSTRAP_SERVERS)

        producer = KafkaProducer(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            acks="all",
            retries=3,
        )

        future = producer.send(TOPIC, value=event)
        metadata = future.get(timeout=10)

        logger.info(
            "Event published successfully: topic=%s partition=%s offset=%s",
            metadata.topic,
            metadata.partition,
            metadata.offset,
        )
        logger.info("Published event: %s", json.dumps(event))

    except (KafkaError, Exception):
        logger.exception("Failed to publish traffic event")
        raise

    finally:
        if producer is not None:
            producer.close()
            logger.info("Kafka producer closed")


if __name__ == "__main__":
    main()

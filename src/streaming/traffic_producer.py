
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in (None, ""):
    # Allow `python src/streaming/traffic_producer.py` as well as `python -m ...`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from kafka import KafkaProducer
from kafka.errors import KafkaError

from src.common import config

logger = config.setup_logging("traffic_producer.log", "autopulse.traffic_producer")

BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS
TOPIC = config.TRAFFIC_INCIDENTS_TOPIC


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

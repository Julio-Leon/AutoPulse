
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# --------------------------------------------------
# Project configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "vehicle_telemetry_stream.log"

KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "vehicle.telemetry.raw"

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "vehicle_telemetry"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints" / "vehicle_telemetry_bronze"


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
# Streaming job
# --------------------------------------------------

def main():
    spark = None
    query = None

    try:
        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

        logger.info("Starting AutoPulse Bronze telemetry ingestion")
        logger.info("Kafka topic: %s", KAFKA_TOPIC)
        logger.info("Bronze output: %s", BRONZE_DIR)
        logger.info("Checkpoint: %s", CHECKPOINT_DIR)

        spark = (
            SparkSession.builder
            .appName("AutoPulse-Vehicle-Telemetry-Bronze")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Read the Kafka stream.
        kafka_stream = (
            spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", KAFKA_TOPIC)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
        )

        # Preserve the original message and Kafka metadata.
        # Kafka's value column is binary, so cast it to a string.
        bronze_stream = kafka_stream.select(
            F.col("value").cast("string").alias("raw_payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.current_timestamp().alias("ingested_at"),
        )

        # Append each micro-batch to Bronze Parquet.
        query = (
            bronze_stream.writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", str(BRONZE_DIR))
            .option("checkpointLocation", str(CHECKPOINT_DIR))
            .trigger(processingTime="5 seconds")
            .start()
        )

        logger.info("Bronze streaming query started")
        logger.info("Waiting for telemetry messages. Press Ctrl+C to stop.")

        query.awaitTermination()

    except KeyboardInterrupt:
        logger.info("Bronze streaming job stopped by user")

    except Exception:
        logger.exception("Bronze streaming job failed")
        raise

    finally:
        if query is not None and query.isActive:
            query.stop()

        if spark is not None:
            spark.stop()

        logger.info("Bronze streaming job stopped")


if __name__ == "__main__":
    main()

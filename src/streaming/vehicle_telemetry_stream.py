"""
Kafka (vehicle.telemetry.raw) -> Bronze vehicle telemetry, via Spark
Structured Streaming.

Each micro-batch is appended to Bronze as Parquet. The trigger interval is
configurable (STREAM_TRIGGER_INTERVAL, default "1 minute"): a longer interval
produces fewer, larger files, which is cheaper on S3 and better for Snowpipe
than one tiny file every few seconds.
"""

import sys
from pathlib import Path

if __package__ in (None, ""):
    # Allow `python src/streaming/vehicle_telemetry_stream.py` as well as `python -m ...`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.common import config


# --------------------------------------------------
# Project configuration
# --------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC = config.VEHICLE_TELEMETRY_TOPIC

BRONZE_DIR = config.BRONZE_VEHICLE_TELEMETRY
CHECKPOINT_DIR = config.checkpoint_path("vehicle_telemetry_bronze")
TRIGGER_INTERVAL = config.STREAM_TRIGGER_INTERVAL

logger = config.setup_logging("vehicle_telemetry_stream.log", "autopulse.stream")


# --------------------------------------------------
# Streaming job
# --------------------------------------------------

def main():
    spark = None
    query = None

    try:
        logger.info("Starting AutoPulse Bronze telemetry ingestion | %s", config.describe())
        logger.info("Kafka topic: %s", KAFKA_TOPIC)
        logger.info("Bronze output: %s", BRONZE_DIR)
        logger.info("Checkpoint: %s", CHECKPOINT_DIR)
        logger.info("Trigger interval: %s", TRIGGER_INTERVAL)

        # kafka=True adds the spark-sql-kafka connector matching your PySpark version.
        spark = config.build_spark(
            "AutoPulse-Vehicle-Telemetry-Bronze",
            shuffle_partitions=4,
            kafka=True,
        )

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
        # coalesce(1): one file per micro-batch instead of one per Kafka partition.
        query = (
            bronze_stream.coalesce(1).writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", BRONZE_DIR)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(processingTime=TRIGGER_INTERVAL)
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

"""
Vehicle telemetry: Bronze -> Silver (+ quarantine), incremental.

Each run processes only the Bronze files that arrived since the previous run
and APPENDS the results to Silver and quarantine. It uses Spark Structured
Streaming with `trigger(availableNow=True)`: the job reads everything new,
then stops. Progress is remembered in a checkpoint, so it's safe to run on a
schedule (Airflow -> Glue every 15 minutes) and Snowpipe only ever sees new
Silver files.

Checkpoint: <CHECKPOINT_ROOT>/vehicle_telemetry_silver
  To rebuild Silver from scratch, delete Silver, quarantine and this checkpoint.

Validation rules are unchanged from the previous batch version.
"""

import sys
from pathlib import Path

if __package__ in (None, ""):
    # Allow `python src/batch/transform_vehicle_telemetry.py` as well as `python -m ...`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common import config


# --------------------------------------------------
# Project paths
# --------------------------------------------------

BRONZE_DIR = config.BRONZE_VEHICLE_TELEMETRY
SILVER_DIR = config.SILVER_VEHICLE_TELEMETRY
QUARANTINE_DIR = config.QUARANTINE_VEHICLE_TELEMETRY
CHECKPOINT_DIR = config.checkpoint_path("vehicle_telemetry_silver")

# Upper bound per micro-batch, so a large backlog is processed in steps.
MAX_FILES_PER_BATCH = 1000

logger = config.setup_logging(
    "transform_vehicle_telemetry.log", "autopulse.vehicle_silver"
)


# --------------------------------------------------
# Schemas
# --------------------------------------------------

# Bronze, as written by src/streaming/vehicle_telemetry_stream.py.
BRONZE_SCHEMA = StructType([
    StructField("raw_payload", StringType(), True),
    StructField("kafka_topic", StringType(), True),
    StructField("kafka_partition", IntegerType(), True),
    StructField("kafka_offset", LongType(), True),
    StructField("kafka_timestamp", TimestampType(), True),
    StructField("ingested_at", TimestampType(), True),
])

# JSON payload produced by the simulator.
TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_mph", DoubleType(), True),
    StructField("odometer_miles", DoubleType(), True),
    StructField("fuel_level_pct", DoubleType(), True),
    StructField("engine_status", StringType(), True),
    StructField("is_simulated", BooleanType(), True),
])


# --------------------------------------------------
# Transformation (one batch of Bronze rows)
# --------------------------------------------------

def transform(bronze_df):
    """Return (clean_df, quarantine_df) for a DataFrame of Bronze rows."""

    # Parse each raw JSON payload.
    parsed_df = (
        bronze_df
        .withColumn(
            "parsed",
            F.from_json(
                F.col("raw_payload"),
                TELEMETRY_SCHEMA,
            ),
        )
        .select(
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "ingested_at",
            "raw_payload",
            # Keep an explicit flag: "parsed" itself is not kept after
            # "parsed.*" expands it, so later filters cannot refer to it.
            F.col("parsed").isNull().alias("parse_failed"),
            "parsed.*",
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp")),
        )
    )

    # Normalize text fields.
    normalized_df = (
        parsed_df
        .withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("vehicle_id", F.upper(F.trim(F.col("vehicle_id"))))
        .withColumn("engine_status", F.lower(F.trim(F.col("engine_status"))))
    )

    # Assign a reason when a record fails validation.
    invalid_conditions = [
        (
            F.col("event_id").isNull() | (F.col("event_id") == ""),
            "missing_event_id",
        ),
        (
            F.col("vehicle_id").isNull() | (F.col("vehicle_id") == ""),
            "missing_vehicle_id",
        ),
        (
            F.col("event_timestamp").isNull(),
            "invalid_timestamp",
        ),
        (
            F.col("latitude").isNull()
            | (F.col("latitude") < -90)
            | (F.col("latitude") > 90),
            "invalid_latitude",
        ),
        (
            F.col("longitude").isNull()
            | (F.col("longitude") < -180)
            | (F.col("longitude") > 180),
            "invalid_longitude",
        ),
        (
            F.col("speed_mph").isNull() | (F.col("speed_mph") < 0),
            "invalid_speed",
        ),
        (
            F.col("odometer_miles").isNull()
            | (F.col("odometer_miles") < 0),
            "invalid_odometer",
        ),
        (
            F.col("fuel_level_pct").isNull()
            | (F.col("fuel_level_pct") < 0)
            | (F.col("fuel_level_pct") > 100),
            "invalid_fuel_level",
        ),
        (
            F.col("engine_status").isNull()
            | ~F.col("engine_status").isin("running", "idling", "off"),
            "invalid_engine_status",
        ),
        (
            F.col("is_simulated").isNull(),
            "missing_simulation_flag",
        ),
    ]

    reason_expression = F.lit(None).cast("string")

    for condition, reason in invalid_conditions:
        reason_expression = F.when(
            condition,
            F.lit(reason),
        ).otherwise(reason_expression)

    validated_df = normalized_df.withColumn(
        "validation_error",
        reason_expression,
    )

    # Parse failures produce null fields. Catch them separately.
    invalid_df = validated_df.filter(
        F.col("parse_failed")
        | F.col("validation_error").isNotNull()
    ).withColumn(
        "validation_error",
        F.coalesce(F.col("validation_error"), F.lit("unparseable_payload")),
    )

    valid_df = validated_df.filter(
        ~F.col("parse_failed")
        & F.col("validation_error").isNull()
    )

    # If duplicate event IDs exist in this batch, retain one record.
    # Kafka topic/partition/offset provide deterministic ordering.
    # (Duplicates across batches are also removed by Snowflake's MERGE.)
    dedup_window = Window.partitionBy("event_id").orderBy(
        F.col("kafka_timestamp").asc(),
        F.col("kafka_partition").asc(),
        F.col("kafka_offset").asc(),
    )

    ranked_df = valid_df.withColumn(
        "duplicate_rank",
        F.row_number().over(dedup_window),
    )

    duplicate_df = ranked_df.filter(
        F.col("duplicate_rank") > 1
    ).withColumn(
        "validation_error",
        F.lit("duplicate_event_id"),
    )

    clean_df = (
        ranked_df
        .filter(F.col("duplicate_rank") == 1)
        .drop("duplicate_rank", "parse_failed", "timestamp", "raw_payload")
    )

    # Store invalid records and duplicates in quarantine.
    quarantine_df = (
        invalid_df.drop("parse_failed")
        .unionByName(
            duplicate_df.drop(
                "duplicate_rank",
                "parse_failed",
                "raw_payload",
                "timestamp",
            ),
            allowMissingColumns=True,
        )
    )

    return clean_df, quarantine_df


def write_batch(batch_df, batch_id: int) -> None:
    """foreachBatch handler: transform one micro-batch and append it."""
    batch_df = batch_df.persist()
    try:
        bronze_count = batch_df.count()
        if bronze_count == 0:
            logger.info("Batch %s: no new Bronze rows", batch_id)
            return

        clean_df, quarantine_df = transform(batch_df)
        clean_df = clean_df.persist()
        quarantine_df = quarantine_df.persist()

        valid_count = clean_df.count()
        quarantine_count = quarantine_df.count()

        # One file per batch keeps Silver (and Snowpipe) efficient at this volume.
        if valid_count > 0:
            clean_df.coalesce(1).write.mode("append").parquet(SILVER_DIR)
        if quarantine_count > 0:
            quarantine_df.coalesce(1).write.mode("append").parquet(QUARANTINE_DIR)

        logger.info(
            "Batch %s: bronze=%s -> silver=%s, quarantine=%s",
            batch_id, bronze_count, valid_count, quarantine_count,
        )

        clean_df.unpersist()
        quarantine_df.unpersist()
    finally:
        batch_df.unpersist()


# --------------------------------------------------
# Job
# --------------------------------------------------

def main():
    spark = None

    try:
        logger.info("Starting vehicle telemetry Bronze-to-Silver job | %s", config.describe())
        logger.info("Bronze: %s | Silver: %s | Checkpoint: %s", BRONZE_DIR, SILVER_DIR, CHECKPOINT_DIR)

        spark = config.build_spark(
            "AutoPulse-Transform-Vehicle-Telemetry", shuffle_partitions=4
        )

        if not config.path_exists(spark, BRONZE_DIR):
            # Nothing has been streamed yet; not an error for a scheduled job.
            logger.warning("Bronze directory not found yet: %s. Nothing to do.", BRONZE_DIR)
            return

        query = (
            spark.readStream
            .schema(BRONZE_SCHEMA)
            .option("maxFilesPerTrigger", MAX_FILES_PER_BATCH)
            .parquet(BRONZE_DIR)
            .writeStream
            .foreachBatch(write_batch)
            .option("checkpointLocation", CHECKPOINT_DIR)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

        try:
            progress = list(query.recentProgress or [])
            total_rows = sum(int(p["numInputRows"]) for p in progress)
        except Exception:  # progress format differs slightly between Spark versions
            progress, total_rows = [], "unknown"
        logger.info(
            "Vehicle telemetry transformation completed: %s new Bronze rows in %s batch(es)",
            total_rows, len(progress),
        )

    except Exception:
        logger.exception("Vehicle telemetry transformation failed")
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

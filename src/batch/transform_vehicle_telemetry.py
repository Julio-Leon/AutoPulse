
import json
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructType,
    StructField,
)


# --------------------------------------------------
# Project paths
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "vehicle_telemetry"
SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "vehicle_telemetry"
QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine" / "vehicle_telemetry"

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "transform_vehicle_telemetry.log"


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
# Expected JSON schema
# --------------------------------------------------

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
# Transformation
# --------------------------------------------------

def main():
    spark = None

    try:
        if not BRONZE_DIR.exists():
            raise FileNotFoundError(
                f"Bronze directory not found: {BRONZE_DIR}"
            )

        logger.info("Starting vehicle telemetry Bronze-to-Silver job")

        spark = (
            SparkSession.builder
            .appName("AutoPulse-Transform-Vehicle-Telemetry")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        # Read the raw Bronze records.
        bronze_df = spark.read.parquet(str(BRONZE_DIR))
        bronze_count = bronze_df.count()

        logger.info("Bronze record count: %s", bronze_count)

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
            F.col("parsed").isNull()
            | F.col("validation_error").isNotNull()
        )

        valid_df = validated_df.filter(
            F.col("parsed").isNotNull()
            & F.col("validation_error").isNull()
        )

        # If duplicate event IDs exist, retain one record.
        # Kafka topic/partition/offset provide deterministic ordering.
        from pyspark.sql.window import Window

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
            .drop("duplicate_rank", "parsed", "timestamp", "raw_payload")
        )

        # Store invalid records and duplicates in quarantine.
        quarantine_df = (
            invalid_df.drop("parsed")
            .unionByName(
                duplicate_df.drop(
                    "duplicate_rank",
                    "parsed",
                    "raw_payload",
                    "timestamp",
                ),
                allowMissingColumns=True,
            )
        )

        valid_count = clean_df.count()
        invalid_count = invalid_df.count()
        duplicate_count = duplicate_df.count()
        quarantine_count = quarantine_df.count()

        logger.info("Valid unique records: %s", valid_count)
        logger.info("Invalid records: %s", invalid_count)
        logger.info("Duplicate records: %s", duplicate_count)
        logger.info("Total quarantine records: %s", quarantine_count)

        # Write each output as Parquet.
        clean_df.repartition(15).write.mode("overwrite").parquet(str(SILVER_DIR))
        logger.info("Silver output written to %s", SILVER_DIR)

        if quarantine_count > 0:
            quarantine_df.write.mode("overwrite").parquet(
                str(QUARANTINE_DIR)
            )
            logger.info("Quarantine output written to %s", QUARANTINE_DIR)
        else:
            logger.info("No records require quarantine")

        logger.info("Silver sample:")
        clean_df.select(
            "event_id",
            "vehicle_id",
            "event_timestamp",
            "speed_mph",
            "odometer_miles",
            "fuel_level_pct",
        ).show(10, truncate=False)

        logger.info("Vehicle telemetry transformation completed")

    except Exception:
        logger.exception("Vehicle telemetry transformation failed")
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

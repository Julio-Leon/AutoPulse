
import logging
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = PROJECT_ROOT / "data" / "silver" / "vehicle_telemetry"
GOLD_DIR = PROJECT_ROOT / "data" / "gold" / "vehicle_fleet_summary"

LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "transform_vehicle_gold.log"

# Illustrative maintenance interval for this project.
# Later, make this configurable per vehicle.
MAINTENANCE_INTERVAL_MILES = 5000


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


def main():
    spark = None

    try:
        if not SILVER_DIR.exists():
            raise FileNotFoundError(
                f"Vehicle Silver directory not found: {SILVER_DIR}"
            )

        spark = (
            SparkSession.builder
            .appName("AutoPulse-Vehicle-Gold-Analytics")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        silver_df = spark.read.parquet(str(SILVER_DIR))

        # Use the event timestamp to identify the first and latest
        # observations for each vehicle.
        window_ascending = Window.partitionBy("vehicle_id").orderBy(
            F.col("event_timestamp").asc(),
            F.col("kafka_partition").asc(),
            F.col("kafka_offset").asc(),
        )

        window_descending = Window.partitionBy("vehicle_id").orderBy(
            F.col("event_timestamp").desc(),
            F.col("kafka_partition").desc(),
            F.col("kafka_offset").desc(),
        )

        first_readings = (
            silver_df
            .withColumn("row_num", F.row_number().over(window_ascending))
            .filter(F.col("row_num") == 1)
            .select(
                "vehicle_id",
                F.col("odometer_miles").alias("first_odometer_miles"),
                F.col("event_timestamp").alias("first_event_timestamp"),
            )
        )

        latest_readings = (
            silver_df
            .withColumn("row_num", F.row_number().over(window_descending))
            .filter(F.col("row_num") == 1)
            .select(
                "vehicle_id",
                F.col("odometer_miles").alias("latest_odometer_miles"),
                F.col("fuel_level_pct").alias("latest_fuel_level_pct"),
                F.col("speed_mph").alias("latest_speed_mph"),
                F.col("engine_status").alias("latest_engine_status"),
                F.col("event_timestamp").alias("latest_event_timestamp"),
            )
        )

        # Summarize observed behavior per vehicle.
        vehicle_metrics = (
            silver_df
            .groupBy("vehicle_id")
            .agg(
                F.count("*").alias("telemetry_readings"),
                F.avg("speed_mph").alias("average_speed_mph"),
                F.max("speed_mph").alias("maximum_speed_mph"),
                F.min("event_timestamp").alias("first_seen_timestamp"),
                F.max("event_timestamp").alias("last_seen_timestamp"),
            )
        )

        gold_df = (
            vehicle_metrics
            .join(first_readings, on="vehicle_id", how="inner")
            .join(latest_readings, on="vehicle_id", how="inner")
            .withColumn(
                "observed_distance_miles",
                F.greatest(
                    F.col("latest_odometer_miles")
                    - F.col("first_odometer_miles"),
                    F.lit(0.0),
                ),
            )
            .withColumn(
                "maintenance_status",
                F.when(
                    F.pmod(
                        F.floor(F.col("latest_odometer_miles")).cast("long"),
                        F.lit(MAINTENANCE_INTERVAL_MILES),
                    ) == 0,
                    F.lit("check_service_interval"),
                ).otherwise(F.lit("no_interval_alert")),
            )
            .withColumn(
                "observation_window_minutes",
                (
                    F.col("last_seen_timestamp").cast("long")
                    - F.col("first_seen_timestamp").cast("long")
                ) / 60.0,
            )
            .select(
                "vehicle_id",
                "telemetry_readings",
                "first_seen_timestamp",
                "last_seen_timestamp",
                "observation_window_minutes",
                "observed_distance_miles",
                F.round("average_speed_mph", 2).alias("average_speed_mph"),
                F.round("maximum_speed_mph", 2).alias("maximum_speed_mph"),
                "latest_speed_mph",
                "latest_fuel_level_pct",
                "latest_engine_status",
                "latest_odometer_miles",
                "maintenance_status",
            )
            .orderBy("vehicle_id")
        )

        row_count = gold_df.count()

        gold_df.write.mode("overwrite").parquet(str(GOLD_DIR))

        logger.info("Silver input records: %s", silver_df.count())
        logger.info("Gold vehicle summaries: %s", row_count)
        logger.info("Gold output written to %s", GOLD_DIR)
        gold_df.show(20, truncate=False)

    except Exception:
        logger.exception("Vehicle Gold transformation failed")
        raise

    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()

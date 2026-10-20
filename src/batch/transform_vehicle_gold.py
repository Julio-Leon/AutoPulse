
import sys
from pathlib import Path

if __package__ in (None, ""):
    # Allow `python src/batch/transform_vehicle_gold.py` as well as `python -m ...`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.common import config


SILVER_DIR = config.SILVER_VEHICLE_TELEMETRY
GOLD_DIR = config.GOLD_VEHICLE_FLEET_SUMMARY

# Illustrative service schedule: every MAINTENANCE_INTERVAL_MILES miles, with a
# "service_due" flag once a vehicle is within MAINTENANCE_DUE_WINDOW_MILES of
# its next service. Both are configurable in .env.
MAINTENANCE_INTERVAL_MILES = config.MAINTENANCE_INTERVAL_MILES
MAINTENANCE_DUE_WINDOW_MILES = config.MAINTENANCE_DUE_WINDOW_MILES

logger = config.setup_logging("transform_vehicle_gold.log", "autopulse.vehicle_gold")


def main():
    spark = None

    try:
        logger.info("Starting vehicle Gold job | %s", config.describe())

        spark = config.build_spark(
            "AutoPulse-Vehicle-Gold-Analytics", shuffle_partitions=4
        )

        if not config.path_exists(spark, SILVER_DIR):
            raise FileNotFoundError(
                f"Vehicle Silver directory not found: {SILVER_DIR}"
            )

        silver_df = spark.read.parquet(SILVER_DIR)

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
            # Previously: flagged only when the odometer was an exact multiple
            # of the interval (e.g. exactly 15000 miles), which almost never
            # happens. Now: miles remaining until the next scheduled service.
            .withColumn(
                "miles_until_service",
                F.round(
                    F.lit(float(MAINTENANCE_INTERVAL_MILES))
                    - F.pmod(
                        F.col("latest_odometer_miles"),
                        F.lit(float(MAINTENANCE_INTERVAL_MILES)),
                    ),
                    2,
                ),
            )
            .withColumn(
                "maintenance_status",
                F.when(
                    F.col("miles_until_service") <= MAINTENANCE_DUE_WINDOW_MILES,
                    F.lit("service_due"),
                ).otherwise(F.lit("ok")),
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
                "miles_until_service",
                "maintenance_status",
            )
            .orderBy("vehicle_id")
        )

        row_count = gold_df.count()

        gold_df.coalesce(1).write.mode("overwrite").parquet(GOLD_DIR)

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

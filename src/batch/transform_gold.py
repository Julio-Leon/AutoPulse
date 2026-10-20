
import sys
from pathlib import Path

if __package__ in (None, ""):
    # Allow `python src/batch/transform_gold.py` as well as `python -m ...`.
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import functions as F

from src.common import config


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

SILVER_DIR = config.SILVER_COLLISIONS
GOLD_DIR = config.GOLD_ROOT
LOG_FILE = config.LOG_DIR / "transform_gold.log"

logger = config.setup_logging(LOG_FILE.name, "autopulse.gold")


# ---------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------

def main():
    spark = None

    try:
        logger.info("Starting Silver-to-Gold pipeline. %s", config.describe())
        logger.info("Silver input: %s", SILVER_DIR)
        logger.info("Gold output: %s", GOLD_DIR)
        logger.info("Log file: %s", LOG_FILE)

        # Create Spark session.
        spark = config.build_spark("AutoPulse-Silver-to-Gold", shuffle_partitions=12)

        if not config.path_exists(spark, SILVER_DIR):
            raise FileNotFoundError(
                f"Silver directory does not exist: {SILVER_DIR}"
            )

        logger.info("Spark session created successfully.")

        # -------------------------------------------------
        # Read Silver data
        # -------------------------------------------------

        logger.info("Reading Silver Parquet data.")

        df = spark.read.parquet(SILVER_DIR)

        silver_count = df.count()

        logger.info("Silver record count: %d", silver_count)
        logger.info("Silver column count: %d", len(df.columns))

        # -------------------------------------------------
        # Prepare columns for aggregation
        # -------------------------------------------------

        logger.info("Preparing columns for Gold aggregations.")

        # Convert crash_time, such as "18:35", into hour 18.
        df = df.withColumn(
            "crash_hour",
            F.hour(
                F.to_timestamp(
                    F.concat(
                        F.lit("2000-01-01 "),
                        F.col("crash_time"),
                    ),
                    "yyyy-MM-dd H:mm",
                )
            ),
        )

        # Treat missing injury and fatality counts as zero
        # for aggregation purposes.
        count_columns = [
            "number_of_persons_injured",
            "number_of_persons_killed",
        ]

        for column in count_columns:
            df = df.withColumn(
                column,
                F.coalesce(
                    F.col(column).cast("long"),
                    F.lit(0),
                ),
            )

        # Normalize borough names.
        df = df.withColumn(
            "borough",
            F.upper(F.trim(F.col("borough"))),
        )

        # -------------------------------------------------
        # Gold 1: Hourly collision trends
        # -------------------------------------------------

        logger.info("Building hourly collision trends.")

        hourly = (
            df.filter(F.col("crash_hour").isNotNull())
            .groupBy("crash_hour")
            .agg(
                F.countDistinct("collision_id").alias(
                    "collision_count"
                ),
                F.sum("number_of_persons_injured").alias(
                    "persons_injured"
                ),
                F.sum("number_of_persons_killed").alias(
                    "persons_killed"
                ),
            )
            .orderBy("crash_hour")
        )

        hourly_path = f"{GOLD_DIR}/hourly_collisions"

        hourly.write.mode("overwrite").parquet(
            hourly_path
        )

        hourly_count = hourly.count()

        logger.info(
            "Hourly trends written: %d rows to %s",
            hourly_count,
            hourly_path,
        )

        # -------------------------------------------------
        # Gold 2: Borough summary
        # -------------------------------------------------

        logger.info("Building borough summary.")

        borough = (
            # Silver marks missing boroughs as "UNKNOWN"; exclude them here.
            df.filter(
                F.col("borough").isNotNull()
                & (F.col("borough") != "")
                & (F.col("borough") != "UNKNOWN")
            )
            .groupBy("borough")
            .agg(
                F.countDistinct("collision_id").alias(
                    "collision_count"
                ),
                F.sum("number_of_persons_injured").alias(
                    "persons_injured"
                ),
                F.sum("number_of_persons_killed").alias(
                    "persons_killed"
                ),
            )
            .orderBy(F.desc("collision_count"))
        )

        borough_path = f"{GOLD_DIR}/borough_summary"

        borough.write.mode("overwrite").parquet(
            borough_path
        )

        borough_count = borough.count()

        logger.info(
            "Borough summary written: %d rows to %s",
            borough_count,
            borough_path,
        )

        # -------------------------------------------------
        # Gold 3: Geographic hotspots
        # -------------------------------------------------

        logger.info("Building geographic hotspot summary.")

        hotspots = (
            df.filter(
                F.col("coordinates_valid") == True
            )
            .withColumn(
                "latitude_cell",
                F.round(F.col("latitude"), 3),
            )
            .withColumn(
                "longitude_cell",
                F.round(F.col("longitude"), 3),
            )
            .groupBy(
                "latitude_cell",
                "longitude_cell",
            )
            .agg(
                F.countDistinct("collision_id").alias(
                    "collision_count"
                ),
                F.sum("number_of_persons_injured").alias(
                    "persons_injured"
                ),
                F.sum("number_of_persons_killed").alias(
                    "persons_killed"
                ),
            )
            .orderBy(F.desc("collision_count"))
        )

        hotspots_path = f"{GOLD_DIR}/geographic_hotspots"

        hotspots.write.mode("overwrite").parquet(
            hotspots_path
        )

        hotspots_count = hotspots.count()

        logger.info(
            "Geographic hotspots written: %d rows to %s",
            hotspots_count,
            hotspots_path,
        )

        # -------------------------------------------------
        # Validate and preview outputs
        # -------------------------------------------------

        logger.info("Displaying hourly collision trends.")
        hourly.show(24, truncate=False)

        logger.info("Displaying borough summary.")
        borough.show(truncate=False)

        logger.info("Displaying top 10 geographic hotspots.")
        hotspots.show(200, truncate=False)

        logger.info("Silver input records: %d", silver_count)
        logger.info("Hourly aggregate rows: %d", hourly_count)
        logger.info("Borough aggregate rows: %d", borough_count)
        logger.info("Geographic hotspot rows: %d", hotspots_count)

        logger.info("Silver-to-Gold pipeline completed successfully.")

        print(f"Gold output: {GOLD_DIR}")
        print(f"Log file: {LOG_FILE}")

    except Exception:
        logger.exception("Silver-to-Gold pipeline failed.")
        raise

    finally:
        if spark is not None:
            logger.info("Stopping Spark session.")
            spark.stop()
            logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
